//! LIVE order-book depth stream for the order-book-imbalance (OBI) filter.
//!
//! Subscribes to Binance's partial book depth stream
//! (`<symbol>@depth<levels>@100ms`), collapses the top levels into a single
//! imbalance value in `[-1.0, 1.0]` and forwards it on `tx`. Used only in live
//! mode; backtests never open this stream.
//!
//! Includes the same ping/pong heartbeat as the ticker client: ping every 20s,
//! force-reconnect if pong doesn't arrive within 10s.

use crate::strategy::order_book::compute_imbalance;
use crate::utility::utility::sleep_seconds;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::connect_async;
use tungstenite::Message;

const PING_INTERVAL: Duration = Duration::from_secs(20);

/// Select the correct WebSocket host based on testnet flag.
fn ws_host(use_testnet: bool) -> &'static str {
    if use_testnet {
        "stream.testnet.binance.vision:9443"
    } else {
        "stream.binance.com:9443"
    }
}

/// Parse `(price, qty)` pairs from a partial-depth payload side (array of
/// `["price", "qty"]` string pairs).
fn parse_levels(side: Option<&Value>) -> Vec<(f64, f64)> {
    let Some(Value::Array(rows)) = side else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| {
            let price = row.get(0)?.as_str()?.parse::<f64>().ok()?;
            let qty = row.get(1)?.as_str()?.parse::<f64>().ok()?;
            Some((price, qty))
        })
        .collect()
}

/// Extract the order-book imbalance from one partial-depth message.
fn imbalance_from_payload(text: &str, levels: usize) -> Option<f64> {
    let v: Value = serde_json::from_str(text).ok()?;
    let bids = parse_levels(v.get("bids"));
    let asks = parse_levels(v.get("asks"));
    compute_imbalance(&bids, &asks, levels)
}

/// Connect to the depth stream for `symbol` and forward each computed imbalance.
/// Reconnects with exponential backoff on error. Sends ping every 20s.
pub async fn run_binance_depth_client(
    symbol: &str,
    levels: usize,
    tx: UnboundedSender<f64>,
    use_testnet: bool,
) {
    let host = ws_host(use_testnet);
    // Binance partial-depth streams are offered for a fixed set of level counts.
    let stream_levels = if levels <= 5 { 5 } else if levels <= 10 { 10 } else { 20 };
    let mut retry_delay = 2u64;

    loop {
        let url = format!(
            "wss://{}/ws/{}@depth{}@100ms",
            host,
            symbol.to_lowercase(),
            stream_levels
        );

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                retry_delay = 2;
                let (mut write, mut read) = ws_stream.split();
                let mut last_ping = Instant::now();
                let mut pending_pong = false;

                loop {
                    let next_ping_in = PING_INTERVAL
                        .checked_sub(last_ping.elapsed())
                        .unwrap_or(Duration::ZERO);

                    tokio::select! {
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Some(obi) = imbalance_from_payload(&text, levels) {
                                        let _ = tx.send(obi);
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    pending_pong = false;
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Close(_))) | None => {
                                    eprintln!("[WS-DEPTH] {} connection closed by server", symbol);
                                    break;
                                }
                                Some(Err(e)) => {
                                    eprintln!("[WS-DEPTH] {} error: {}", symbol, e);
                                    break;
                                }
                                Some(Ok(_)) => {}
                            }
                        }
                        _ = tokio::time::sleep(next_ping_in) => {
                            if pending_pong {
                                eprintln!("[WS-DEPTH] {} ping timeout, reconnecting...", symbol);
                                break;
                            }
                            let _ = write.send(Message::Ping(vec![].into())).await;
                            last_ping = Instant::now();
                            pending_pong = true;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[WS-DEPTH] {} connect error: {}. Retrying in {}s...", symbol, e, retry_delay);
            }
        }

        sleep_seconds(retry_delay).await;
        retry_delay = (retry_delay * 2).min(60);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_partial_depth_payload() {
        let payload = r#"{
            "lastUpdateId": 1,
            "bids": [["100.0", "9.0"], ["99.0", "1.0"]],
            "asks": [["101.0", "1.0"], ["102.0", "1.0"]]
        }"#;
        // bids = 10, asks = 2 over two levels -> (10-2)/12 = 0.666...
        let obi = imbalance_from_payload(payload, 5).unwrap();
        assert!((obi - (8.0 / 12.0)).abs() < 1e-9);
    }
}
