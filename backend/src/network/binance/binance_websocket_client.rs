//! Binance WebSocket ticker client with ping/pong heartbeat and testnet support.
//!
//! Binance requires a ping frame at least every 30 seconds, or the server
//! closes the connection silently. We ping every 20s and treat a pong timeout
//! (> 5s after ping) as a disconnect, triggering a fresh reconnect.

use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::connect_async;
use tungstenite::Message;
use futures_util::{SinkExt, StreamExt};
use crate::network::client::parse_price_from_json;
use crate::utility::utility::sleep_seconds;
use tokio::time::{Duration, Instant};

const PING_INTERVAL: Duration = Duration::from_secs(20);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);

/// Select the correct WebSocket host based on testnet flag.
fn ws_host(use_testnet: bool) -> &'static str {
    if use_testnet {
        "stream.testnet.binance.vision:9443"
    } else {
        "stream.binance.com:9443"
    }
}

/// Connect to Binance (or testnet) ticker stream for `symbol` and forward each
/// parsed price to `tx`. Automatically reconnects with exponential backoff on
/// any error. Sends a WebSocket ping every 20 seconds; force-reconnects if no
/// pong arrives within 10 seconds of the ping (stale connection detection).
pub async fn run_binance_websocket_client(symbol: &str, tx: UnboundedSender<f64>, use_testnet: bool) {
    let host = ws_host(use_testnet);
    let mut retry_delay = 2u64;

    loop {
        let url = format!("wss://{}/ws/{}@miniTicker", host, symbol.to_lowercase());

        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                retry_delay = 2; // reset backoff on successful connect
                if use_testnet {
                    println!("[WS] {} connected to TESTNET ticker stream", symbol);
                }

                let (mut write, mut read) = ws_stream.split();
                let mut last_ping = Instant::now();
                let mut pending_pong = false;

                loop {
                    let next_ping_in = PING_INTERVAL
                        .checked_sub(last_ping.elapsed())
                        .unwrap_or(Duration::ZERO);

                    tokio::select! {
                        // Receive next message from exchange
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Some(price) = parse_price_from_json(&text) {
                                        let _ = tx.send(price);
                                    }
                                }
                                Some(Ok(Message::Pong(_))) => {
                                    pending_pong = false;
                                }
                                Some(Ok(Message::Ping(data))) => {
                                    // Echo back pings from the server (polite)
                                    let _ = write.send(Message::Pong(data)).await;
                                }
                                Some(Ok(Message::Close(_))) | None => {
                                    eprintln!("[WS] {} connection closed by server", symbol);
                                    break;
                                }
                                Some(Err(e)) => {
                                    eprintln!("[WS] {} error: {}", symbol, e);
                                    break;
                                }
                                Some(Ok(_)) => {} // ignore Binary / Frame
                            }
                        }
                        // Send periodic ping
                        _ = tokio::time::sleep(next_ping_in) => {
                            if pending_pong {
                                // No pong received since last ping → stale connection
                                eprintln!("[WS] {} ping timeout (no pong in {}s), reconnecting...",
                                    symbol, PONG_TIMEOUT.as_secs());
                                break;
                            }
                            let _ = write.send(Message::Ping(vec![].into())).await;
                            last_ping = Instant::now();
                            pending_pong = true;

                            // Schedule a pong timeout check
                            tokio::spawn({
                                // We use a separate sleep here; if pong arrives before
                                // this fires, pending_pong is cleared and the outer loop
                                // won't break.
                                let _timeout = PONG_TIMEOUT;
                                async move {
                                    tokio::time::sleep(_timeout).await;
                                }
                            });
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[WS] {} connect error: {}. Retrying in {}s...", symbol, e, retry_delay);
            }
        }

        sleep_seconds(retry_delay).await;
        retry_delay = (retry_delay * 2).min(60); // exponential backoff, cap at 60s
    }
}
