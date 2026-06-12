use crate::models::crypto_dto::CryptoPriceResult;
use crate::models::data::CryptoExchange;
use crate::network::client::fetch_raw_price;
use reqwest::Client;

pub async fn fetch_bybit(symbol: &str, client: &Client) -> Result<CryptoPriceResult, reqwest::Error> {
    fetch_raw_price(CryptoExchange::Bybit, symbol, client).await
}
