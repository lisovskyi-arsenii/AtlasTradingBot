use async_trait::async_trait;
use crate::models::candle::Candle;
use crate::models::crypto_dto::CryptoPriceResult;
use crate::network::client::WhitebitPriceRequest;
use reqwest::Client;
use crate::network::ExchangeClient;

pub struct WhitebitClient;

const WHITEBIT_API_BASE: &str = "https://api.whitebit.com/api/v4";

fn normalize_market_symbol(symbol: &str) -> String {
    if symbol.contains('_') {
        return symbol.to_string();
    }

    // Whitebit market names typically use an underscore between base and quote,
    // e.g. BTC_USDT. Support the most common quote assets used by the bot.
    const QUOTE_ASSETS: [&str; 11] = [
        "USDT", "USDC", "USD", "BTC", "ETH", "BNB", "TRY", "EUR", "UAH", "JPY", "KZT",
    ];

    for quote in QUOTE_ASSETS {
        if let Some(base) = symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return format!("{}_{}", base, quote);
            }
        }
    }

    symbol.to_string()
}

#[async_trait]
impl ExchangeClient for WhitebitClient {
    async fn fetch_price(&self, symbol: &str, client: &Client) -> Result<CryptoPriceResult, reqwest::Error> {
        let market = normalize_market_symbol(symbol);
        let url = format!("{WHITEBIT_API_BASE}/public/ticker?market={}", market);
        let resp = client.get(url).send().await?.json::<WhitebitPriceRequest>().await?;
        Ok(resp.into())
    }

    async fn fetch_history(&self, _symbol: &str, _limit: usize, _client: &Client) -> Result<Vec<Candle>, reqwest::Error> {
        // Whitebit klines/history not implemented yet
        Ok(Vec::new())
    }
}

