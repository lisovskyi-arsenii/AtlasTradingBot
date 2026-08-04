use async_trait::async_trait;
use crate::models::candle::Candle;
use crate::models::crypto_dto::CryptoPriceResult;
use crate::network::client::BybitPriceRequest;
use reqwest::Client;
use crate::network::ExchangeClient;

pub struct BybitClient;

#[async_trait]
impl ExchangeClient for BybitClient {
    async fn fetch_price(&self, symbol: &str, client: &Client) -> Result<CryptoPriceResult, reqwest::Error> {
        let url = format!("https://api.bybit.com/v5/market/tickers?category=spot&symbol={}", symbol);
        let resp = client.get(url).send().await?.json::<BybitPriceRequest>().await?;
        Ok(resp.into())
    }

    async fn fetch_history(&self, _symbol: &str, _limit: usize, _client: &Client) -> Result<Vec<Candle>, reqwest::Error> {
        // Bybit klines implementation can be added later. For now return empty vec.
        Ok(Vec::new())
    }
}
