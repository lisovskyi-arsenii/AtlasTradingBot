//! LIVE order-book depth stream for the order-book-imbalance (OBI) filter.
//!
//! Subscribes to Binance's partial book depth stream
//! (`<symbol>@depth<levels>@100ms`), collapses the top levels into a single
//! imbalance value in `[-1.0, 1.0]` and forwards it on `tx`. Used only in live
//! mode; backtests never open this stream.

use crate::strategy::order_book::compute_imbalance;
use crate::utility::utility::sleep_seconds;
use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::connect_async;
use tungstenite::Message;

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
/// Reconnects on error, mirroring `run_binance_websocket_client`.
pub async fn run_binance_depth_client(symbol: &str, levels: usize, tx: UnboundedSender<f64>) {
    // Binance partial-depth streams are offered for a fixed set of level counts.
    let stream_levels = if levels <= 5 {
        5
    } else if levels <= 10 {
        10
    } else {
        20
    };
    loop {
        let url = format!(
            "wss://stream.binance.com:9443/ws/{}@depth{}@100ms",
            symbol.to_lowercase(),
            stream_levels
        );
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                let (_, mut read) = ws_stream.split();
                while let Some(message) = read.next().await {
                    if let Ok(Message::Text(text)) = message {
                        if let Some(obi) = imbalance_from_payload(&text, levels) {
                            let _ = tx.send(obi);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[WS-DEPTH] {} error: {}. Retrying in 5s...", symbol, e);
                sleep_seconds(5).await;
            }
        }
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
