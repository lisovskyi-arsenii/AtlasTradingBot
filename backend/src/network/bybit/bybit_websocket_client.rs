use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::connect_async;
use tungstenite::Message;
use futures_util::{StreamExt, SinkExt};
use crate::network::client::parse_price_from_json;

pub async fn run_bybit_websocket_client(symbol: &str, tx: UnboundedSender<f64>) {
    // Updated to v5 WebSocket API for spot trading
    let url = "wss://stream.bybit.com/v5/public/spot";
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect to Bybit WebSocket");

    // Subscribe to spot ticker for the symbol using v5 format
    let subscribe = serde_json::json!({"op": "subscribe", "args": [format!("tickers.{}", symbol)]});
    let _ = ws_stream.send(Message::Text(subscribe.to_string().into())).await;

    while let Some(message) = ws_stream.next().await {
        if let Ok(Message::Text(text)) = message {
            if let Some(price) = parse_price_from_json(&text) {
                let _ = tx.send(price);
            }
        }
    }
}



