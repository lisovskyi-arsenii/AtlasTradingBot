use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::connect_async;
use tungstenite::Message;
use futures_util::StreamExt;
use crate::network::client::parse_price_from_json;
use crate::utility::utility::sleep_seconds;

pub async fn run_binance_websocket_client(symbol: &str, tx: UnboundedSender<f64>) {
    loop {
        let url = format!("wss://stream.binance.com:9443/ws/{}@ticker", symbol.to_lowercase());
        match connect_async(&url).await {
            Ok((ws_stream, _)) => {
                let (_, mut read) = ws_stream.split();
                while let Some(message) = read.next().await {
                    if let Ok(Message::Text(text)) = message {
                        if let Some(price) = parse_price_from_json(&text) {
                            let _ = tx.send(price);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[WS] Error: {}. Retrying in 5s...", e);
                sleep_seconds(5).await;
            }
        }
    }
}
