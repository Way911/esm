use std::vec;

use anyhow::Ok;
use config::APP_CONFIG;
use elasticsearch::{
    http::transport::Transport, params::Refresh, BulkOperation, BulkParts, Elasticsearch,
    ScrollParts, SearchParts,
};
use serde_json::{json, Value};
use tokio::sync::mpsc::{self, Receiver, Sender};

mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let transport = Transport::single_node(&APP_CONFIG.src_url)?;
    let src_client = Elasticsearch::new(transport);

    let transport = Transport::single_node(&APP_CONFIG.dest_url)?;
    let dest_client = Elasticsearch::new(transport);

    let (tx, rx) = mpsc::channel(10000);

    let mut producers = vec![];

    let consumer = tokio::spawn(consume_hits(rx, dest_client));

    for id in 0..APP_CONFIG.worker_count {
        // The sender endpoint can be copied
        let thread_tx: Sender<Vec<Value>> = tx.clone();
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
    if let Err(e) = consumer.await? {
        eprintln!("error: {:?}", e);
    }

    Ok(())
}

async fn consume_hits(
    mut rx: Receiver<Vec<Value>>,
    dest_client: Elasticsearch,
) -> anyhow::Result<()> {
    let capacity = APP_CONFIG.bulk_size as usize;
    let mut ops: Vec<BulkOperation<Value>> = Vec::with_capacity(capacity);

    while let Some(hits) = rx.recv().await {
        for hit in hits {
            let id = hit["_id"].as_str().unwrap();
            let source = hit["_source"].as_object().unwrap().clone();
            // println!("{:?}", source);
            if ops.len() >= capacity {
                println!("bulk send len: {}", ops.len());
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

            ops.push(BulkOperation::index(json!(source)).id(id).into());
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
        println!("{:?}", bulk_response);
    }

    Ok(())
}

async fn produce_hits(
    id: u32,
    src_client: Elasticsearch,
    tx: Sender<Vec<Value>>,
) -> anyhow::Result<()> {
    let scroll = "30s";

    // make a search API call
    let mut response = src_client
        .search(SearchParts::Index(&[&APP_CONFIG.src_index]))
        .scroll(scroll)
        .body(json!({
            "slice": {
                    "field": "@timestamp",
                    "id": id,
                    "max": APP_CONFIG.worker_count,
                },
            "size": APP_CONFIG.size_per_page,
            "query": {
                "match_all": {},
                // "range": {
                //     "datetime": {
                //     "gte": 45180,
                //     "lte": 45180.299
                //     }
                // }
            },
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
        println!("send len: {}", hits.len());
        tx.send(hits.clone()).await?;
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
    println!("worker {} done", id);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_produce_hits() {
        let (tx, mut rx) = mpsc::channel(100);

        tokio::spawn(async move {
            for i in 0..10 {
                if let Err(_) = tx.send(i).await {
                    println!("receiver dropped");
                    return;
                }
            }
        });

        while let Some(i) = rx.recv().await {
            println!("got = {}", i);
        }
    }
}
