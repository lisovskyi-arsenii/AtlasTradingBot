use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Deserialize)]
pub struct BookTickerEvent {
    #[serde(rename = "u")]
    pub update_id: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub best_bid_price: String,
    #[serde(rename = "B")]
    pub best_bid_qty: String,
    #[serde(rename = "a")]
    pub best_ask_price: String,
    #[serde(rename = "A")]
    pub best_ask_qty: String,
}

pub async fn run_book_ticker_client(
    symbol: &str,
    spread_tx: UnboundedSender<(String, f64)>,
    use_testnet: bool,
) {
    let base_url = if use_testnet {
        "wss://stream.binancefuture.com/ws" // Testnet futures or stream.testnet.binance.vision/ws for spot
        // Note: For spot testnet, the WS URL is wss://testnet.binance.vision/ws
    } else {
        "wss://stream.binance.com:9443/ws"
    };

    let ws_url = if use_testnet {
        format!("wss://stream.testnet.binance.vision/ws/{}@bookTicker", symbol.to_lowercase())
    } else {
        format!("{}/{}@bookTicker", base_url, symbol.to_lowercase())
    };

    loop {
        match connect_async(ws_url.clone()).await {
            Ok((ws_stream, _)) => {
                println!("[BOOK-TICKER] Connected to {}", symbol);
                let (_, mut read) = ws_stream.split();

                while let Some(message) = read.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            if let Ok(event) = serde_json::from_str::<BookTickerEvent>(&text) {
                                let bid: f64 = event.best_bid_price.parse().unwrap_or(0.0);
                                let ask: f64 = event.best_ask_price.parse().unwrap_or(0.0);
                                if bid > 0.0 && ask > 0.0 {
                                    let spread_pct = (ask - bid) / ask * 100.0;
                                    let _ = spread_tx.send((symbol.to_string(), spread_pct));
                                }
                            }
                        }
                        Ok(Message::Ping(_ping)) => {
                            // Tungstenite handles ping/pong automatically if we don't block
                        }
                        Ok(Message::Close(_)) => {
                            eprintln!("[BOOK-TICKER] Connection closed for {}", symbol);
                            break;
                        }
                        Err(e) => {
                            eprintln!("[BOOK-TICKER] Read error for {}: {}", symbol, e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                eprintln!("[BOOK-TICKER] Connection failed for {}: {}. Retrying in 5s...", symbol, e);
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
