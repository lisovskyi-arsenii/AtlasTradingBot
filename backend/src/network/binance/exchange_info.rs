//! Binance `GET /exchangeInfo` parser — loads real tick size, step size, and
//! minimum notional per symbol. Replaces the hardcoded `SymbolFilters::for_exchange()`
//! constants that are catastrophically wrong for low-priced tokens (PEPE, SHIB, etc.).

use reqwest::Client;
use serde_json::Value;
use crate::spot::spot_wallet::SymbolFilters;

const MAINNET_BASE: &str = "https://api.binance.com";
const TESTNET_BASE: &str = "https://testnet.binance.vision";

/// Fetch real symbol filters for a single symbol from Binance `exchangeInfo`.
/// Returns `None` on any parse error so callers can fall back to defaults.
pub async fn fetch_symbol_filters(
    symbol: &str,
    client: &Client,
    use_testnet: bool,
) -> Option<SymbolFilters> {
    let base = if use_testnet { TESTNET_BASE } else { MAINNET_BASE };
    let url = format!("{}/api/v3/exchangeInfo?symbol={}", base, symbol.to_uppercase());

    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[EXCHANGE-INFO] Failed to fetch filters for {}: {}", symbol, e);
            return None;
        }
    };

    let body: Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[EXCHANGE-INFO] Failed to parse response for {}: {}", symbol, e);
            return None;
        }
    };

    parse_symbol_filters(&body, symbol)
}

/// Extract `SymbolFilters` from an `exchangeInfo` JSON response.
/// Handles missing or malformed fields gracefully.
fn parse_symbol_filters(body: &Value, symbol: &str) -> Option<SymbolFilters> {
    let symbols = body.get("symbols")?.as_array()?;

    let sym_obj = symbols.iter().find(|s| {
        s.get("symbol")
            .and_then(|v| v.as_str())
            .map(|n| n.eq_ignore_ascii_case(symbol))
            .unwrap_or(false)
    })?;

    let filters = sym_obj.get("filters")?.as_array()?;

    let mut tick_size: Option<f64> = None;
    let mut step_size: Option<f64> = None;
    let mut min_notional: Option<f64> = None;

    for filter in filters {
        let filter_type = filter.get("filterType").and_then(|v| v.as_str()).unwrap_or("");

        match filter_type {
            "PRICE_FILTER" => {
                tick_size = filter
                    .get("tickSize")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|&v| v > 0.0);
            }
            "LOT_SIZE" => {
                step_size = filter
                    .get("stepSize")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|&v| v > 0.0);
            }
            "NOTIONAL" | "MIN_NOTIONAL" => {
                // New format uses "NOTIONAL" with "minNotional"; old format uses "MIN_NOTIONAL"
                let val = filter
                    .get("minNotional")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|&v| v > 0.0);
                if val.is_some() {
                    min_notional = val;
                }
            }
            _ => {}
        }
    }

    let filters = SymbolFilters {
        tick_size: tick_size.unwrap_or(0.01),
        step_size: step_size.unwrap_or(0.00001),
        min_notional: min_notional.unwrap_or(10.0),
    };

    println!(
        "[EXCHANGE-INFO] {} filters: tick={}, step={}, min_notional={}",
        symbol.to_uppercase(),
        filters.tick_size,
        filters.step_size,
        filters.min_notional
    );

    Some(filters)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exchange_info_json() {
        let json = serde_json::json!({
            "symbols": [{
                "symbol": "BTCUSDT",
                "filters": [
                    { "filterType": "PRICE_FILTER", "tickSize": "0.01000000" },
                    { "filterType": "LOT_SIZE", "stepSize": "0.00001000" },
                    { "filterType": "NOTIONAL", "minNotional": "5.00000000" }
                ]
            }]
        });

        let filters = parse_symbol_filters(&json, "BTCUSDT").unwrap();
        assert!((filters.tick_size - 0.01).abs() < 1e-9);
        assert!((filters.step_size - 0.00001).abs() < 1e-9);
        assert!((filters.min_notional - 5.0).abs() < 1e-9);
    }

    #[test]
    fn falls_back_to_defaults_on_missing_filters() {
        let json = serde_json::json!({
            "symbols": [{ "symbol": "BTCUSDT", "filters": [] }]
        });
        let filters = parse_symbol_filters(&json, "BTCUSDT").unwrap();
        // Should use hardcoded fallback values
        assert!(filters.tick_size > 0.0);
        assert!(filters.step_size > 0.0);
        assert!(filters.min_notional > 0.0);
    }
}
