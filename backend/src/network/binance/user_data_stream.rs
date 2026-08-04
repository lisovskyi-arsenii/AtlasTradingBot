use crate::execution::{ClientOrderId, OrderAck, OrderSide, OrderStatus};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::interval;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const MAINNET_REST: &str = "https://api.binance.com";
const TESTNET_REST: &str = "https://testnet.binance.vision";
const MAINNET_WS: &str = "wss://stream.binance.com:9443/ws";
const TESTNET_WS: &str = "wss://stream.binance.vision/ws";

pub struct UserDataStream {
    api_key: String,
    rest_url: &'static str,
    ws_url: &'static str,
    client: Client,
}

impl UserDataStream {
    pub fn new(api_key: String, use_testnet: bool) -> Self {
        Self {
            api_key,
            rest_url: if use_testnet { TESTNET_REST } else { MAINNET_REST },
            ws_url: if use_testnet { TESTNET_WS } else { MAINNET_WS },
            client: Client::new(),
        }
    }

    async fn get_listen_key(&self) -> Result<String, String> {
        let url = format!("{}/api/v3/userDataStream", self.rest_url);
        let resp = self.client.post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await.map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            return Err(format!("Failed to get listenKey. Status: {}", resp.status()));
        }

        let body: Value = resp.json().await.map_err(|e| e.to_string())?;
        body["listenKey"].as_str().map(|s| s.to_string()).ok_or_else(|| "No listenKey in response".to_string())
    }

    async fn keep_alive_listen_key(&self, listen_key: &str) -> Result<(), String> {
        let url = format!("{}/api/v3/userDataStream?listenKey={}", self.rest_url, listen_key);
        let resp = self.client.put(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send().await.map_err(|e| e.to_string())?;

        if !resp.status().is_success() {
            return Err(format!("Failed to keep-alive listenKey. Status: {}", resp.status()));
        }
        Ok(())
    }

    pub async fn run(self, tx: broadcast::Sender<OrderAck>) {
        loop {
            let listen_key = match self.get_listen_key().await {
                Ok(k) => k,
                Err(e) => {
                    eprintln!("[USER DATA WS] Could not get listenKey: {}. Retrying in 10s...", e);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            };

            let ws_endpoint = format!("{}/{}", self.ws_url, listen_key);
            println!("[USER DATA WS] Connecting to {}", ws_endpoint);

            match connect_async(&ws_endpoint).await {
                Ok((mut ws_stream, _)) => {
                    println!("[USER DATA WS] Connected.");

                    let mut keep_alive_interval = interval(Duration::from_secs(30 * 60)); // 30 mins
                    keep_alive_interval.tick().await; // skip first immediate tick

                    loop {
                        tokio::select! {
                            _ = keep_alive_interval.tick() => {
                                if let Err(e) = self.keep_alive_listen_key(&listen_key).await {
                                    eprintln!("[USER DATA WS] Failed keep-alive: {}. Reconnecting...", e);
                                    break; // Break inner loop to get new listenKey
                                } else {
                                    println!("[USER DATA WS] Successfully kept alive listenKey.");
                                }
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        if let Ok(value) = serde_json::from_str::<Value>(&text) {
                                            if value["e"] == "executionReport" {
                                                if let Some(ack) = parse_execution_report(value) {
                                                    let _ = tx.send(ack);
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(data))) => {
                                        let _ = ws_stream.send(Message::Pong(data)).await;
                                    }
                                    Some(Err(e)) => {
                                        eprintln!("[USER DATA WS] Read error: {}. Reconnecting...", e);
                                        break;
                                    }
                                    None => {
                                        eprintln!("[USER DATA WS] Stream closed. Reconnecting...");
                                        break;
                                    }
                                    _ => {} // Ignore other message types
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[USER DATA WS] Connect error: {}. Retrying in 5s...", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}

fn parse_execution_report(val: Value) -> Option<OrderAck> {
    let client_id_str = val["c"].as_str()?;
    let exchange_id = val["i"].as_u64()?;
    let symbol = val["s"].as_str()?;
    let side_str = val["S"].as_str()?;
    let status_str = val["X"].as_str()?;
    let filled_qty_str = val["z"].as_str()?;
    let _avg_price_str = val["p"].as_str()?; // p is order price, but L is last filled price, unfortunately average is not provided directly, we can use z (filled qty) and maybe calculate it or use Z (cumulative quote asset transacted qty) / z.

    let cumulative_quote_qty_str = val["Z"].as_str()?;

    let filled_qty: f64 = filled_qty_str.parse().ok()?;
    let cumulative_quote_qty: f64 = cumulative_quote_qty_str.parse().ok()?;

    let avg_price = if filled_qty > 0.0 {
        cumulative_quote_qty / filled_qty
    } else {
        0.0
    };

    let side = if side_str == "BUY" { OrderSide::Buy } else { OrderSide::Sell };
    let status = match status_str {
        "FILLED" => OrderStatus::Filled,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "NEW" => OrderStatus::New,
        "CANCELED" => OrderStatus::Canceled,
        _ => OrderStatus::Rejected,
    };

    Some(OrderAck {
        client_id: ClientOrderId(client_id_str.to_string()),
        exchange_order_id: Some(exchange_id),
        symbol: symbol.to_string(),
        side,
        avg_price,
        filled_qty,
        status,
    })
}
