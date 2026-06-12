use crate::models::crypto_dto::CryptoPriceResult;
use serde::Deserialize;

// Bybit
#[derive(Debug, Deserialize)]
pub struct BybitPriceRequest {
    pub result: BybitResult
}

impl From<BybitPriceRequest> for CryptoPriceResult {
    fn from(value: BybitPriceRequest) -> Self {
        if let Some(ticker) = value.result.list.first() {
            let current_price = ticker.last_price.parse::<f64>().unwrap_or(0.0);
            CryptoPriceResult {
                symbol: ticker.symbol.clone(),
                price: current_price
            }
        } else {
            CryptoPriceResult {
                symbol: String::from("UNKNOWN"),
                price: 0.0
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct BybitResult {
    pub list: Vec<BybitTicker>
}

#[derive(Debug, Deserialize)]
pub struct BybitTicker {
    pub symbol: String,
    #[serde(rename = "lastPrice")]
    pub last_price: String
}
