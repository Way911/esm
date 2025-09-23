use std::vec;

use anyhow::Ok;
use config::APP_CONFIG;
use elasticsearch::{
    http::transport::Transport, params::Refresh, BulkOperation, BulkParts, Elasticsearch,
    ScrollParts, SearchParts,
};
use flume::{Receiver, Sender};
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::config::APP;

mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let transport = Transport::single_node(&APP_CONFIG.src_url)?;
    let src_client = Elasticsearch::new(transport);

    let total_count = count_hits(src_client.clone()).await?;

    let (tx, rx) = flume::bounded(APP_CONFIG.bulk_size as usize * APP_CONFIG.dest_urls.len());

    let mut producers = vec![];
    let mut consumers = vec![];

    for dest_url in &APP_CONFIG.dest_urls {
        let transport = Transport::single_node(dest_url)?;
        let dest_client = Elasticsearch::new(transport);
        let rx = rx.clone();
        let consumer = tokio::spawn(consume_hits(
            rx,
            dest_client,
            total_count / APP_CONFIG.dest_urls.len() as u64,
        ));
        consumers.push(consumer);
    }

    for id in 0..(APP_CONFIG.worker_count - 1) {
        // The sender endpoint can be copied
        let thread_tx: Sender<BulkOperation<Value>> = tx.clone();
        let src_client = src_client.clone();
        let p = tokio::spawn(produce_hits(id, src_client, thread_tx));
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

async fn count_hits(client: Elasticsearch) -> anyhow::Result<u64> {
    let query = match APP.query_json.clone() {
        Some(json_string) => serde_json::from_str(&json_string)?,
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

async fn consume_hits(
    rx: Receiver<BulkOperation<Value>>,
    dest_client: Elasticsearch,
    total_count: u64,
) -> anyhow::Result<()> {
    let capacity = APP_CONFIG.bulk_size as usize;
    let mut ops: Vec<BulkOperation<Value>> = Vec::with_capacity(capacity);
    let mut start = Instant::now();
    let mut count = 0;

    while let core::result::Result::Ok(op) = rx.recv_async().await {
        // println!("recv op");
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
            count += capacity;
            let elapsed = start.elapsed().as_secs_f64();
            // estimate time to complete
            let etc_sec = elapsed / (capacity as f64) * (total_count - count as u64) as f64;
            let etc_hour = etc_sec / 3600.0;
            let etc_day = etc_hour / 24.0;
            println!(
                "progress: {:.2}% ETC: {:.2} sec | {:.2} hour | {:.2} day",
                count as f64 / total_count as f64 * 100.0,
                etc_sec,
                etc_hour,
                etc_day
            );
            ops = Vec::with_capacity(capacity);
            start = Instant::now();
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

    println!("consumer done {:?}", APP_CONFIG.dest_urls);
    Ok(())
}

async fn produce_hits(
    id: u32,
    src_client: Elasticsearch,
    tx: Sender<BulkOperation<Value>>,
) -> anyhow::Result<()> {
    let scroll = "1m";

    let query = match APP.query_json.clone() {
        Some(json_string) => serde_json::from_str(&json_string)?,
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

    // while hits are returned, keep asking for the next batch
    while !hits.is_empty() {
        // println!("send len: {}", hits.len());

        for hit in hits {
            let id = hit["_id"].as_str().unwrap();
            let source = hit["_source"].as_object().unwrap().clone();
            let op = BulkOperation::index(json!(source)).id(id).into();
            tx.send_async(op).await?;
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
    println!("producer {} done", id);

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
        println!("{:?}", APP_CONFIG.src_url);
        let count = count_hits(src_client).await.unwrap();
        println!("total hits: {}", count);
        println!("time: {:?}", start.elapsed());
    }

    #[tokio::test]
    #[ignore]
    async fn test_produce_hits() {
        let transport = Transport::single_node(&APP_CONFIG.src_url).unwrap();
        let src_client = Elasticsearch::new(transport);
        let (tx, rx) = flume::bounded(APP_CONFIG.bulk_size as usize * APP_CONFIG.dest_urls.len());

        tokio::spawn(async move {
            while let core::result::Result::Ok(_op) = rx.recv_async().await {
                println!("recv op");
            }
        });

        let mut producers = vec![];
        for id in 0..(APP_CONFIG.worker_count - 1) {
            // The sender endpoint can be copied
            let thread_tx: Sender<BulkOperation<Value>> = tx.clone();
            let src_client = src_client.clone();
            let p = tokio::spawn(produce_hits(id, src_client, thread_tx));
            producers.push(p);
        }

        for p in producers {
            if let Err(e) = p.await.unwrap() {
                eprintln!("error: {:?}", e);
            }
        }

        drop(tx);
    }
}
