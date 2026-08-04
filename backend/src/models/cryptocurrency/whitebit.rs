use crate::models::crypto_dto::CryptoPriceResult;
use serde::Deserialize;
use std::collections::HashMap;

// Whitebit
#[derive(Debug, Deserialize)]
pub struct WhitebitPriceRequest (pub HashMap<String, WhitebitTicker>);

impl From<WhitebitPriceRequest> for CryptoPriceResult {
    fn from(value: WhitebitPriceRequest) -> Self {
        let map = value.0;

        if let Some((market_name, ticker)) = map.into_iter().next() {
            let current_price = ticker.last_price.parse::<f64>().unwrap_or(0.0);

            CryptoPriceResult {
                symbol: market_name,
                price: current_price
            }
        } else {
            CryptoPriceResult {
                symbol: String::from("UNKNOWN"),
                price: 0.0,
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct WhitebitTicker {
    #[serde(rename = "last_price")]
    pub last_price: String
}
