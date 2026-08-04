// Re-exports of request DTOs so exchange-specific clients can import them via
// `crate::network::client::...` (keeps existing import sites working).
pub(crate) use crate::models::cryptocurrency::binance::BinancePriceRequest;
pub(crate) use crate::models::cryptocurrency::bybit::BybitPriceRequest;
pub(crate) use crate::models::cryptocurrency::whitebit::WhitebitPriceRequest;

// Small helper used by websocket clients: attempts to extract a price from a
// JSON payload returned by various exchanges. It checks several common keys
// and nested structures and returns Option<f64>.
use serde_json::Value;

pub fn parse_price_from_json(text: &str) -> Option<f64> {
    if let Ok(v) = serde_json::from_str::<Value>(text) {
        // Try common numeric fields first
        if let Some(p) = v.get("c").and_then(|x| x.as_f64()) {
            return Some(p);
        }
        if let Some(s) = v.get("c").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()) {
            return Some(s);
        }

        if let Some(p) = v.get("price").and_then(|x| x.as_f64()) {
            return Some(p);
        }
        if let Some(s) = v.get("price").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()) {
            return Some(s);
        }

        // Whitebit/Bybit style keys
        if let Some(s) = v.get("last_price").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()) {
            return Some(s);
        }
        if let Some(s) = v.get("lastPrice").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()) {
            return Some(s);
        }

        // Nested arrays / objects (e.g., Bybit result.list[0].last_price)
        if let Some(arr) = v.get("result").and_then(|r| r.get("list")) {
            if let Some(first) = arr.get(0) {
                if let Some(s) = first.get("lastPrice").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()) {
                    return Some(s);
                }
                if let Some(s) = first.get("last_price").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()) {
                    return Some(s);
                }
            }
        }

        if let Some(arr) = v.get("data").and_then(|d| d.get(0)) {
            if let Some(s) = arr.get("lastPrice").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()) {
                return Some(s);
            }
            if let Some(s) = arr.get("last_price").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()) {
                return Some(s);
            }
        }
    }

    None
}

