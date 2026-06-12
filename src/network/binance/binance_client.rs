use async_trait::async_trait;
use futures_util::StreamExt;
use crate::models::candle::Candle;
use crate::models::crypto_dto::CryptoPriceResult;
use crate::network::client::{parse_price_from_json, BinancePriceRequest};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::connect_async;
use tungstenite::Message;
use crate::network::ExchangeClient;
use crate::utility::utility::sleep_seconds;

pub struct BinanceClient;

#[async_trait]
impl ExchangeClient for BinanceClient {
    async fn fetch_price(&self, symbol: &str, client: &Client) -> Result<CryptoPriceResult, reqwest::Error> {
        let url = format!("https://api.binance.com/api/v3/ticker/price?symbol={}", symbol);
        let resp = client.get(url).send().await?.json::<BinancePriceRequest>().await?;
        Ok(resp.into())
    }

    async fn fetch_history(&self, symbol: &str, limit: usize, client: &Client) -> Result<Vec<Candle>, reqwest::Error> {
        let url = format!("https://api.binance.com/api/v3/klines?symbol={}&interval=1h&limit={}", symbol, limit);
        let data: Vec<Vec<Value>> = client.get(url).send().await?.json().await?;

        let mut candles: Vec<Candle> = Vec::new();

        for kline in data {
            if kline.len() >= 6 {
                let open = kline[1].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                let high = kline[2].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                let low = kline[3].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                let close = kline[4].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                let volume = kline[5].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);

                candles.push(Candle { open, high, low, close, volume });
            }
        }

        Ok(candles)
    }
}

// Convenience wrappers for legacy code that expect functions instead of a
// typed client. They call the ExchangeClient implementation above.

pub async fn fetch_historical_candles(symbol: &str, limit: usize, client: &Client) -> Result<Vec<Candle>, reqwest::Error> {
    BinanceClient.fetch_history(symbol, limit, client).await
}
