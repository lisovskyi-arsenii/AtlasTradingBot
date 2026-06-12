use crate::models::crypto_dto::CryptoPriceResult;
pub(crate) use crate::models::cryptocurrency::binance::BinancePriceRequest;
pub(crate) use crate::models::cryptocurrency::bybit::BybitPriceRequest;
pub(crate) use crate::models::cryptocurrency::whitebit::WhitebitPriceRequest;
use crate::models::data::CryptoExchange;
use reqwest::Client;

const BINANCE_URL: &str = "https://api.binance.com/api/v3/ticker/price?symbol=";
const BYBIT_URL: &str = "https://api.bybit.com/v5/market/tickers?category=spot&symbol=";
const WHITEBIT_URL: &str = "https://api.whitebit.com/v1/public/ticker?market=";


pub(crate) async fn fetch_raw_price(url: CryptoExchange, symbol: &str, client: &Client) -> Result<CryptoPriceResult, reqwest::Error> {
    let base_url: &str = match url {
        CryptoExchange::Binance => BINANCE_URL,
        CryptoExchange::Bybit => BYBIT_URL,
        CryptoExchange::Whitebit => WHITEBIT_URL
    };

    let mut custom_url: String = base_url.to_owned();
    custom_url.push_str(symbol);

    let response = client.get(custom_url).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        eprintln!("Server responded with error: {}", status);

        let error = response.error_for_status().unwrap_err();
        return Err(error)
    }

    let result: CryptoPriceResult = match url {
        CryptoExchange::Binance => {
            let request: BinancePriceRequest = response.json::<BinancePriceRequest>().await?;
            request.into()
        },
        CryptoExchange::Bybit => {
            let request: BybitPriceRequest = response.json::<BybitPriceRequest>().await?;
            request.into()
        },
        CryptoExchange::Whitebit => {
            let request: WhitebitPriceRequest = response.json::<WhitebitPriceRequest>().await?;
            request.into()
        }
    };

    Ok(result)
}
