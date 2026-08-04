use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::connect_async;
use tungstenite::Message;
use futures_util::{StreamExt, SinkExt};
use crate::network::client::parse_price_from_json;

fn normalize_market_symbol(symbol: &str) -> String {
    if symbol.contains('_') {
        return symbol.to_string();
    }

    const QUOTE_ASSETS: [&str; 11] = [
        "USDT", "USDC", "USD", "BTC", "ETH", "BNB", "TRY", "EUR", "UAH", "JPY", "KZT",
    ];

    for quote in QUOTE_ASSETS {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return format!("{}_{}", base, quote);
            }
        }
    }

    symbol.to_string()
}

pub async fn run_whitebit_websocket_client(symbol: &str, tx: UnboundedSender<f64>) {
    // Updated to v4 WebSocket API
    let url = "wss://api.whitebit.com/api/v4/ws/spot/ticker";
    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect to Whitebit WebSocket");

    let market = normalize_market_symbol(symbol);
    let subscribe = serde_json::json!({"event": "subscribe", "channel": "ticker", "symbol": market});
    let _ = ws_stream.send(Message::Text(subscribe.to_string().into())).await;

    while let Some(message) = ws_stream.next().await {
        if let Ok(Message::Text(text)) = message {
            if let Some(price) = parse_price_from_json(&text) {
                let _ = tx.send(price);
            }
        }
    }
}
