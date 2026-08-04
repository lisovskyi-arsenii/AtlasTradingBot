//! Binance Kline (candlestick) WebSocket stream with ping/pong heartbeat.
//!
//! Subscribes to `<symbol>@kline_<interval>` and emits a complete [`Candle`]
//! every time the exchange closes a candle (field `"x": true`). Unlike the
//! tick-aggregated approach, this gives real OHLCV data so the volume filter
//! works correctly in live mode.
//!
//! Reconnects with exponential backoff; sends a Binance-required ping every 20s.

use crate::models::candle::Candle;
use crate::utility::utility::sleep_seconds;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::connect_async;
use tungstenite::Message;

const PING_INTERVAL: Duration = Duration::from_secs(20);

fn ws_host(use_testnet: bool) -> &'static str {
    if use_testnet {
        "stream.testnet.binance.vision:9443"
    } else {
        "stream.binance.com:9443"
    }
}

/// Map a config candle period in seconds to a Binance kline interval string.
/// Falls back to `"15m"` for unrecognised values.
pub fn seconds_to_kline_interval(seconds: u64) -> &'static str {
    match seconds {
        60 => "1m",
        180 => "3m",
        300 => "5m",
        900 => "15m",
        1800 => "30m",
        3600 => "1h",
        7200 => "2h",
        14400 => "4h",
        86400 => "1d",
        _ => "15m",
    }
}

/// Parse a closed kline message into a [`Candle`].
/// Returns `None` if the candle is still open (`"x": false`) or on parse error.
fn parse_kline(text: &str) -> Option<Candle> {
    let v: Value = serde_json::from_str(text).ok()?;
    let k = v.get("k")?;

    // Only emit when the candle is closed ("x": true)
    if k.get("x")?.as_bool()? != true {
        return None;
    }

    let open: f64 = k.get("o")?.as_str()?.parse().ok()?;
    let high: f64 = k.get("h")?.as_str()?.parse().ok()?;
    let low: f64 = k.get("l")?.as_str()?.parse().ok()?;
    let close: f64 = k.get("c")?.as_str()?.parse().ok()?;
    let volume: f64 = k.get("v")?.as_str()?.parse().ok()?;

    if open <= 0.0 || close <= 0.0 {
        return None;
    }

    Some(Candle { open, high, low, close, volume })
}

/// Connect to Binance kline stream for `symbol` at `interval` (e.g. `"15m"`).
/// Emits a [`Candle`] on `tx` each time a candle closes. Reconnects with
/// exponential backoff; sends ping every 20s.
pub async fn run_binance_kline_client(
    symbol: &str,
    interval: &str,
    tx: UnboundedSender<Candle>,
    use_testnet: bool,
) {
    let host = ws_host(use_testnet);
    let mut retry_delay = 2u64;

    loop {
        let url = format!(
            "wss://{}/ws/{}@kline_{}",
            host,
            symbol.to_lowercase(),
            interval
        );

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                retry_delay = 2; // reset on success
                if use_testnet {
                    println!("[WS-KLINE] {} connected to TESTNET kline stream ({})", symbol, interval);
                } else {
                    println!("[WS-KLINE] {} connected ({} stream)", symbol, interval);
                }

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
                                    if let Some(candle) = parse_kline(&text) {
                                        let _ = tx.send(candle);
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    pending_pong = false;
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Close(_))) | None => {
                                    eprintln!("[WS-KLINE] {} connection closed by server", symbol);
                                    break;
                                }
                                Some(Err(e)) => {
                                    eprintln!("[WS-KLINE] {} error: {}", symbol, e);
                                    break;
                                }
                                Some(Ok(_)) => {}
                            }
                        }
                        _ = tokio::time::sleep(next_ping_in) => {
                            if pending_pong {
                                eprintln!(
                                    "[WS-KLINE] {} ping timeout, reconnecting...",
                                    symbol
                                );
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
                eprintln!(
                    "[WS-KLINE] {} connect error: {}. Retrying in {}s...",
                    symbol, e, retry_delay
                );
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
    fn parses_closed_kline() {
        let payload = r#"{
            "e": "kline",
            "k": {
                "o": "50000.00", "h": "51000.00", "l": "49000.00",
                "c": "50500.00", "v": "123.456", "x": true
            }
        }"#;
        let candle = parse_kline(payload).unwrap();
        assert!((candle.open - 50000.0).abs() < 1e-9);
        assert!((candle.close - 50500.0).abs() < 1e-9);
        assert!((candle.volume - 123.456).abs() < 1e-9);
    }

    #[test]
    fn ignores_open_kline() {
        let payload = r#"{
            "e": "kline",
            "k": {
                "o": "50000.00", "h": "51000.00", "l": "49000.00",
                "c": "50500.00", "v": "123.456", "x": false
            }
        }"#;
        assert!(parse_kline(payload).is_none());
    }

    #[test]
    fn interval_mapping() {
        assert_eq!(seconds_to_kline_interval(900), "15m");
        assert_eq!(seconds_to_kline_interval(3600), "1h");
        assert_eq!(seconds_to_kline_interval(86400), "1d");
        assert_eq!(seconds_to_kline_interval(999), "15m"); // fallback
    }
}
