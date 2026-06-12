pub mod client;
pub mod binance;
pub mod bybit;
pub mod whitebit;
pub mod scanner;

use crate::models::candle::Candle;
use crate::models::crypto_dto::CryptoPriceResult;
use reqwest::Client;
use async_trait::async_trait;

#[async_trait]
pub trait ExchangeClient: Send + Sync {
    async fn fetch_price(&self, symbol: &str, client: &Client) -> Result<CryptoPriceResult, reqwest::Error>;
    async fn fetch_history(&self, symbol: &str, limit: usize, client: &Client) -> Result<Vec<Candle>, reqwest::Error>;
}
