use crate::models::candle::Candle;
use crate::models::crypto_dto::CryptoPriceResult;
use crate::models::data::CryptoExchange;
use crate::network::client::fetch_raw_price;
use reqwest::Client;
use serde_json::Value;

pub async fn fetch_binance(symbol: &str, client: &Client) -> Result<CryptoPriceResult, reqwest::Error> {
    fetch_raw_price(CryptoExchange::Binance, symbol, client).await
}

// TODO: переробити так щоб було як у верхньому методі логіка винесена в іншу структуру
pub async fn fetch_historical_candles(symbol: &str, limit: usize, client: &Client) -> Result<Vec<Candle>, reqwest::Error> {
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval=1m&limit={}",
        symbol, limit
    );

    let response = client.get(url).send().await?;

    let data: Vec<Vec<Value>> = response.json().await?;

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
