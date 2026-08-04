use crate::models::crypto_dto::CryptoPriceResult;
use serde::Deserialize;

// Binance
#[derive(Debug, Deserialize)]
pub struct BinancePriceRequest {
    pub symbol: String,
    pub price: String
}

impl From<BinancePriceRequest> for CryptoPriceResult {
    fn from(value: BinancePriceRequest) -> Self {
        let current_price: f64 = value.price.parse::<f64>().unwrap();

        CryptoPriceResult {
            symbol: value.symbol,
            price: current_price
        }
    }
}
