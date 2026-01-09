use std::{cmp::max, sync::Arc, time::Duration, vec};

use anyhow::Ok;
use config::APP_CONFIG;
use elasticsearch::{
    http::transport::Transport, params::Refresh, BulkOperation, BulkParts, Elasticsearch,
    ScrollParts, SearchParts,
};
use flume::{Receiver, Sender};
use serde_json::{json, Value};
use tokio::{
    fs,
    sync::RwLock,
    time::{self, Instant},
};

use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressStyle};

use crate::config::APP;

mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let transport = Transport::single_node(&APP_CONFIG.src_url)?;
    let src_client = Elasticsearch::new(transport);

    let query = match APP.query_json.clone() {
        Some(json_string) => {
            println!("query json: {}", json_string);
            serde_json::from_str(&json_string)?
        }
        None => json!(
            {
                "match_all": {}// ,"range":{"datetime":{"gte":45180,"lte":45180.299}}
            }
        ),
    };

    // Create a MultiProgress object
    let multi_progress = MultiProgress::new();

    let sleeptime = Arc::new(RwLock::new(-1.0));

    // Spawn a task that writes to the ratelimit
    let progress_bar = multi_progress.add(ProgressBar::no_length()); // Add a new progress bar
    let sleeptime_w = sleeptime.clone();
    tokio::spawn(async move {
        // Define a common progress bar style
        let progress_style = ProgressStyle::with_template("{msg}").unwrap();
        progress_bar.set_style(progress_style);
        progress_bar.set_message(format!("running with rate limit: {} ms", 0.0));

        loop {
            let cur_sleeptime_lk = sleeptime_w.read().await;
            let cur_sleeptime = *cur_sleeptime_lk;
            drop(cur_sleeptime_lk);

            let tmp_sleep_time = match fs::read_to_string(".ratelimit").await {
                std::result::Result::Ok(content) => content.trim().parse().unwrap(),
                Err(_) => -1.0,
            };
            if tmp_sleep_time != cur_sleeptime {
                // Acquire a write lock
                let mut writable_data = sleeptime_w.write().await;
                *writable_data = tmp_sleep_time;
            }
            if progress_bar.is_hidden() {
                println!("running with rate limit: {} ms", tmp_sleep_time)
            } else {
                progress_bar.set_message(format!("running with rate limit: {} ms", tmp_sleep_time));
            }
            if tmp_sleep_time < 0.0 {
                return;
            }
            // Lock is automatically released when `writable_data` goes out of scope
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }
    });

    // Define a common progress bar style
    let progress_style = ProgressStyle::with_template(
        "{prefix} {spinner} {bar:40.cyan/blue} {percent_precise} {pos:>7}/{len:7} ETA:{eta} Elapsed:{elapsed} {per_sec}",
    )
    .unwrap()
    .progress_chars("#>-");

    let total_count = count_hits(src_client.clone(), query.clone()).await?;

    let (tx, rx) = flume::bounded(max(
        APP_CONFIG.bulk_size as usize * (APP_CONFIG.dest_urls.len() + 1),
        APP_CONFIG.size_per_page as usize * (APP_CONFIG.worker_count + 1) as usize,
    ));

    let mut producers = vec![];
    let mut consumers = vec![];

    for dest_url in &APP_CONFIG.dest_urls {
        let rx = rx.clone();
        let consumer = tokio::spawn(consume_hits(rx, dest_url));
        consumers.push(consumer);
    }

    for id in 0..APP_CONFIG.worker_count {
        let tmp_total_count = total_count / APP_CONFIG.worker_count as u64;
        let progress_bar = multi_progress.add(ProgressBar::new(tmp_total_count)); // Add a new progress bar
        progress_bar.set_style(progress_style.clone());
        progress_bar.set_message(format!("producer #{} running", id));
        progress_bar.set_prefix(format!("producer #{}", id));

        // The sender endpoint can be copied
        let thread_tx: Sender<BulkOperation<Value>> = tx.clone();
        let src_client = src_client.clone();
        let p = tokio::spawn(produce_hits(
            id,
            src_client,
            query.clone(),
            thread_tx,
            progress_bar,
            tmp_total_count,
            sleeptime.clone(),
        ));
        producers.push(p);
    }

    for p in producers {
        if let Err(e) = p.await? {
            eprintln!("error: {:?}", e);
        }
    }

    drop(tx);

    for consumer in consumers {
        if let Err(e) = consumer.await? {
            eprintln!("error: {:?}", e);
        }
    }

    Ok(())
}

async fn count_hits(client: Elasticsearch, query: Value) -> anyhow::Result<u64> {
    let response = client
        .count(elasticsearch::CountParts::Index(&[&APP_CONFIG.src_index]))
        .body(json!({
            "query": query
        }))
        .send()
        .await?;

    let response_body = response.json::<Value>().await?;
    response_body["count"]
        .as_u64()
        .ok_or(anyhow::anyhow!("no count"))
}

async fn consume_hits(rx: Receiver<BulkOperation<Value>>, dest_url: &str) -> anyhow::Result<()> {
    let transport = Transport::single_node(dest_url)?;
    let dest_client = Elasticsearch::new(transport);

    let capacity = APP_CONFIG.bulk_size as usize;
    let mut ops: Vec<BulkOperation<Value>> = Vec::with_capacity(capacity);

    while let core::result::Result::Ok(op) = rx.recv_async().await {
        ops.push(op);
        if ops.len() >= capacity {
            let bulk_response = dest_client
                .bulk(BulkParts::Index(&APP_CONFIG.dest_index))
                .body(ops)
                .send()
                .await?;
            if bulk_response.status_code() != 200 {
                anyhow::bail!("bulk error {:?}", bulk_response);
            }
            ops = Vec::with_capacity(capacity);
        }
    }

    if !ops.is_empty() {
        let bulk_response = dest_client
            .bulk(BulkParts::Index(&APP_CONFIG.dest_index))
            .body(ops)
            .refresh(Refresh::True)
            .error_trace(true)
            .send()
            .await?;
        if bulk_response.status_code() != 200 {
            anyhow::bail!("bulk error {:?}", bulk_response);
        }
    }
    Ok(())
}

async fn produce_hits(
    id: u32,
    src_client: Elasticsearch,
    query: Value,
    tx: Sender<BulkOperation<Value>>,
    progress_bar: ProgressBar,
    total_count: u64,
    sleeptime: Arc<RwLock<f64>>,
) -> anyhow::Result<()> {
    let scroll = "1m";

    // make a search API call
    let mut response = src_client
        .search(SearchParts::Index(&[&APP_CONFIG.src_index]))
        .scroll(scroll)
        .body(json!({
            "slice": {
                    "field": APP_CONFIG.slice_field,
                    "id": id,
                    "max": APP_CONFIG.worker_count,
                },
            "size": APP_CONFIG.size_per_page,
            "query": query,
        }))
        .send()
        .await?;

    // read the response body. Consumes search_response
    let mut response_body = response.json::<Value>().await?;

    // println!("{}", response_body.to_string());
    let mut hits = response_body["hits"]["hits"]
        .as_array()
        .ok_or(anyhow::anyhow!("no hits"))?;
    let mut scroll_id = response_body["_scroll_id"]
        .as_str()
        .ok_or(anyhow::anyhow!("no _scroll_id"))?;

    let mut start = Instant::now();
    let mut count = 0;
    let mut inc = 0;
    let mut enable_sleep = true;
    let is_progress_bar_hidden = progress_bar.is_hidden();

    // while hits are returned, keep asking for the next batch
    while !hits.is_empty() {
        // println!("send len: {}", hits.len());

        for hit in hits {
            let id = hit["_id"].as_str().unwrap();
            let source = hit["_source"].as_object().unwrap().clone();
            let op = BulkOperation::index(json!(source)).id(id).into();
            tx.send_async(op).await?;
        }

        // sleep if rate limit file is provided and elapsed time
        if enable_sleep {
            let sleep_time = *sleeptime.read().await;
            if sleep_time < 0.0 {
                enable_sleep = false;
            } else {
                time::sleep(Duration::from_millis(sleep_time as u64)).await;
            }
        }

        if is_progress_bar_hidden {
            inc += hits.len();
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 15.0 {
                count = count + inc;
                // estimate time to complete
                let etc_sec = elapsed / (inc as f64) * (total_count as f64 - count as f64);
                let etc = HumanDuration(Duration::from_secs(etc_sec as u64));
                println!(
                    "producer #{} {}/{} {:.2}% ETC:{}",
                    id,
                    count,
                    total_count,
                    count as f64 / total_count as f64 * 100.0,
                    etc
                );
                start = Instant::now();
                inc = 0;
            }
        } else {
            progress_bar.inc(hits.len() as u64);
        }

        response = src_client
            .scroll(ScrollParts::ScrollId(scroll_id))
            .body(json!({
                "scroll": scroll,
            }))
            .send()
            .await?;

        response_body = response.json::<Value>().await?;
        // get the scroll_id from this response
        scroll_id = response_body["_scroll_id"]
            .as_str()
            .ok_or(anyhow::anyhow!("no _scroll_id"))?;
        hits = response_body["hits"]["hits"]
            .as_array()
            .ok_or(anyhow::anyhow!("no hits"))?;
    }

    if is_progress_bar_hidden {
        println!("producer #{} done total_count {}", id, total_count)
    }
    progress_bar.finish_with_message(format!("producer #{} done total_count {}", id, total_count));

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn count_total() {
        let start = Instant::now();
        let transport = Transport::single_node(&APP_CONFIG.src_url).unwrap();
        let src_client = Elasticsearch::new(transport);
        let query = match APP.query_json.clone() {
            Some(json_string) => {
                println!("query json: {}", json_string);
                serde_json::from_str(&json_string).unwrap()
            }
            None => json!(
                {
                    "match_all": {}
                    // "range": {
                    //     "datetime": {
                    //     "gte": 45180,
                    //     "lte": 45180.299
                    //     }
                    // }
                }
            ),
        };
        println!("{:?}", APP_CONFIG.src_url);
        let count = count_hits(src_client, query).await.unwrap();
        println!("total hits: {}", count);
        println!("time: {:?}", start.elapsed());
    }

    // #[tokio::test]
    // #[ignore]
    // async fn test_produce_hits() {
    //     let transport = Transport::single_node(&APP_CONFIG.src_url).unwrap();
    //     let src_client = Elasticsearch::new(transport);
    //     let (tx, rx) = flume::bounded(APP_CONFIG.bulk_size as usize * APP_CONFIG.dest_urls.len());

    //     tokio::spawn(async move {
    //         while let core::result::Result::Ok(_op) = rx.recv_async().await {
    //             println!("recv op");
    //         }
    //     });

    //     let mut producers = vec![];
    //     for id in 0..APP_CONFIG.worker_count {
    //         // The sender endpoint can be copied
    //         let thread_tx: Sender<BulkOperation<Value>> = tx.clone();
    //         let src_client = src_client.clone();
    //         let p = tokio::spawn(produce_hits(id, src_client, thread_tx));
    //         producers.push(p);
    //     }

    //     for p in producers {
    //         if let Err(e) = p.await.unwrap() {
    //             eprintln!("error: {:?}", e);
    //         }
    //     }

    //     drop(tx);
    // }

    #[tokio::test]
    #[ignore]
    async fn test_read_file() {
        let mut sleep_time = match fs::read_to_string(".ratelimit").await {
            std::result::Result::Ok(content) => content.trim().parse().unwrap(),
            Err(_) => -1.0,
        };
        time::sleep(Duration::from_millis(sleep_time as u64)).await;
        assert_eq!(sleep_time, 100.0);

        let mut start = Instant::now();
        let mut i = 100;
        while i > 0 {
            i -= i;
            if sleep_time > 0.0 {
                let elapsed = start.elapsed().as_secs_f64();
                if elapsed > sleep_time {
                    sleep_time = match fs::read_to_string(".ratelimit").await {
                        std::result::Result::Ok(content) => content.trim().parse().unwrap(),
                        Err(_) => -1.0,
                    };
                    time::sleep(Duration::from_millis(sleep_time as u64)).await;
                    start = Instant::now();
                }
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_progress_bar() {
        let sleep_time = match fs::read_to_string(".ratelimit").await {
            std::result::Result::Ok(content) => content.trim().parse().unwrap(),
            Err(_) => 1000_usize,
        };
        // Create a MultiProgress object
        let multi_progress = MultiProgress::new();
        // Define a common progress bar style
        let progress_style = ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
        )
        .unwrap()
        .progress_chars("#>-");

        let mut consumers = vec![];

        for i in 0..10 {
            let tmp_total_count = 888;
            let progress_bar = multi_progress.add(ProgressBar::new(tmp_total_count)); // Add a new progress bar
            progress_bar.set_style(progress_style.clone());
            progress_bar.set_message(format!("Processing #{}", i));

            let consumer = tokio::spawn(consume_hits(i, sleep_time, tmp_total_count, progress_bar));
            consumers.push(consumer);
        }

        let progress_bar = multi_progress.add(ProgressBar::new(0)); // Add a new progress bar
        progress_bar.set_style(ProgressStyle::with_template("{msg}").unwrap());
        progress_bar.set_message(format!("Sleep Time {}", sleep_time));

        for consumer in consumers {
            if let Err(e) = consumer.await {
                eprintln!("error: {:?}", e);
            }
        }
    }

    async fn consume_hits(id: u32, sleep_time: usize, total_count: u64, progress_bar: ProgressBar) {
        let mut count = 0;
        let capacity = 10;
        let mut start = Instant::now();
        while count < total_count {
            // println!("recv op");
            time::sleep(Duration::from_millis(sleep_time as u64)).await;

            count += capacity;
            let elapsed = start.elapsed().as_secs_f64();
            // estimate time to complete
            let etc_sec = elapsed / (capacity as f64) * (total_count as f64 - count as f64);
            let etc_hour = etc_sec / 3600.0;
            let etc_day = etc_hour / 24.0;
            // println!(
            //     "consumer progress: {:.2}% ETC: {:.2} day | {:.2} hour | {:.2} sec",
            //     count as f64 / total_count as f64 * 100.0,
            //     etc_day,
            //     etc_hour,
            //     etc_sec
            // );
            progress_bar
                .clone()
                .with_message(format!(
                    "{:.2}% ETC: {:.2} day | {:.2} hour | {:.2} sec",
                    count as f64 / total_count as f64 * 100.0,
                    etc_day,
                    etc_hour,
                    etc_sec
                ))
                .inc(capacity);
            start = Instant::now();
        }

        progress_bar.finish_with_message(format!("#{} finished, total_count: {}", id, total_count));
    }
}
