use async_trait::async_trait;
use crate::models::candle::Candle;
use crate::models::crypto_dto::CryptoPriceResult;
use crate::network::client::BinancePriceRequest;
use reqwest::Client;
use serde_json::Value;
use crate::network::ExchangeClient;

/// Mainnet and testnet base URLs.
const MAINNET_BASE: &str = "https://api.binance.com";
const TESTNET_BASE: &str = "https://testnet.binance.vision";

pub struct BinanceClient {
    pub use_testnet: bool,
}

impl BinanceClient {
    pub fn new(use_testnet: bool) -> Self {
        Self { use_testnet }
    }

    fn base_url(&self) -> &'static str {
        if self.use_testnet { TESTNET_BASE } else { MAINNET_BASE }
    }
}

impl Default for BinanceClient {
    fn default() -> Self {
        Self { use_testnet: false }
    }
}

/// Exponential-backoff helper: retries `f` on HTTP 429 / 5xx.
/// Waits `delay_ms` before the first retry, doubling each time, capped at 30s.
async fn with_backoff<T, F, Fut>(mut f: F) -> Result<T, reqwest::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, reqwest::Error>>,
{
    let mut delay_ms = 500u64;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                // Retry on connection/timeout errors and explicit 429/5xx
                let is_retryable = e.is_timeout()
                    || e.is_connect()
                    || e.status().map(|s| s.as_u16() == 429 || s.is_server_error()).unwrap_or(false);

                if !is_retryable || delay_ms > 30_000 {
                    return Err(e);
                }
                eprintln!(
                    "[REST] Retryable error ({}). Backing off {}ms...",
                    e.status().map(|s| s.as_u16()).unwrap_or(0),
                    delay_ms
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                delay_ms = (delay_ms * 2).min(30_000);
            }
        }
    }
}

#[async_trait]
impl ExchangeClient for BinanceClient {
    async fn fetch_price(&self, symbol: &str, client: &Client) -> Result<CryptoPriceResult, reqwest::Error> {
        let base = self.base_url();
        let url = format!("{}/api/v3/ticker/price?symbol={}", base, symbol);
        let resp = with_backoff(|| client.get(&url).send()).await?;
        let result = resp.json::<BinancePriceRequest>().await?;
        Ok(result.into())
    }

    async fn fetch_history(&self, symbol: &str, limit: usize, client: &Client) -> Result<Vec<Candle>, reqwest::Error> {
        fetch_candles_page(self.base_url(), symbol, "1h", limit, None, client).await
    }
}

/// Fetch one page of klines (up to `limit`, max 1000 per Binance API rules).
/// `start_time_ms` is the open-time of the oldest candle to fetch (exclusive cursor).
async fn fetch_candles_page(
    base_url: &str,
    symbol: &str,
    interval: &str,
    limit: usize,
    start_time_ms: Option<u64>,
    client: &Client,
) -> Result<Vec<Candle>, reqwest::Error> {
    let page_limit = limit.min(1000);
    let mut url = format!(
        "{}/api/v3/klines?symbol={}&interval={}&limit={}",
        base_url, symbol, interval, page_limit
    );
    if let Some(ts) = start_time_ms {
        url.push_str(&format!("&startTime={}", ts));
    }

    let data: Vec<Vec<Value>> = with_backoff(|| client.get(&url).send())
        .await?
        .json()
        .await?;

    let candles = data
        .into_iter()
        .filter_map(|kline| {
            if kline.len() < 6 {
                return None;
            }
            let open = kline[1].as_str()?.parse::<f64>().ok()?;
            let high = kline[2].as_str()?.parse::<f64>().ok()?;
            let low = kline[3].as_str()?.parse::<f64>().ok()?;
            let close = kline[4].as_str()?.parse::<f64>().ok()?;
            let volume = kline[5].as_str()?.parse::<f64>().ok()?;
            Some(Candle { open, high, low, close, volume })
        })
        .collect();

    Ok(candles)
}

/// Paginated fetch: downloads `total` candles in 1000-bar pages using a
/// startTime cursor. Replaces the old single-call that was silently capped at
/// 1000 bars regardless of the requested limit.
pub async fn fetch_historical_candles(
    symbol: &str,
    interval: &str,
    total: usize,
    client: &Client,
) -> Result<Vec<Candle>, reqwest::Error> {
    fetch_historical_candles_with_testnet(symbol, interval, total, client, false).await
}

pub async fn fetch_historical_candles_with_testnet(
    symbol: &str,
    interval: &str,
    total: usize,
    client: &Client,
    use_testnet: bool,
) -> Result<Vec<Candle>, reqwest::Error> {
    let base = if use_testnet { TESTNET_BASE } else { MAINNET_BASE };
    let mut all: Vec<Candle> = Vec::with_capacity(total);
    // Binance kline open_time is in field [0] as milliseconds.
    // We walk backwards in time: start from now, page back using the open_time
    // of the oldest candle in each batch as the next endTime (exclusive).
    // Strategy: fetch from now backwards until we have `total` candles.
    // Use endTime instead of startTime so we always get the most recent data first.
    let mut end_time_ms: Option<u64> = None;

    while all.len() < total {
        let remaining = total - all.len();
        let page_size = remaining.min(1000);

        let mut url = format!(
            "{}/api/v3/klines?symbol={}&interval={}&limit={}",
            base, symbol, interval, page_size
        );
        if let Some(end) = end_time_ms {
            url.push_str(&format!("&endTime={}", end));
        }

        let data: Vec<Vec<Value>> = with_backoff(|| client.get(&url).send())
            .await?
            .json()
            .await?;

        if data.is_empty() {
            break;
        }

        // The oldest candle's open_time is data[0][0] — use it as the next endTime
        // (subtract 1ms so we don't re-fetch the same candle).
        let oldest_open_time = data[0][0].as_u64().unwrap_or(0);
        if oldest_open_time > 0 {
            end_time_ms = Some(oldest_open_time.saturating_sub(1));
        }

        let mut batch: Vec<Candle> = data
            .into_iter()
            .filter_map(|kline| {
                if kline.len() < 6 {
                    return None;
                }
                let open = kline[1].as_str()?.parse::<f64>().ok()?;
                let high = kline[2].as_str()?.parse::<f64>().ok()?;
                let low = kline[3].as_str()?.parse::<f64>().ok()?;
                let close = kline[4].as_str()?.parse::<f64>().ok()?;
                let volume = kline[5].as_str()?.parse::<f64>().ok()?;
                Some(Candle { open, high, low, close, volume })
            })
            .collect();

        // Prepend (we fetched oldest-first per page, newest overall at the end)
        batch.append(&mut all);
        all = batch;
    }

    // Truncate to exactly `total` most-recent candles
    if all.len() > total {
        all.drain(..all.len() - total);
    }

    Ok(all)
}
