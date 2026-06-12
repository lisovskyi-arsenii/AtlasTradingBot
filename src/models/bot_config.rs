use crate::models::data::{CryptoExchange, Mode};

pub struct BotConfig {
    pub mode: Mode,
    pub crypto_exchange: CryptoExchange,
    pub symbol: String,
    pub leverage: f64,
    pub margin: f64
}

impl BotConfig {
    pub fn parse() -> Self {
        let args: Vec<String> = std::env::args().collect();

        let mut mode: Mode = Mode::Spot;
        let mut exchange: CryptoExchange = CryptoExchange::Binance;
        let mut symbol: String = String::from("");
        let mut leverage: f64 = 10.0;
        let mut margin: f64 = 1000.0;

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--mode" => {
                    if i + 1 < args.len() {
                        mode = match args[i + 1].to_lowercase().as_str() {
                            "spot" => Mode::Spot,
                            "futures" => Mode::Futures,
                            _ => Mode::Spot
                        };
                        i += 1;
                    }
                }
                "--exchange" => {
                    if i + 1 < args.len() {
                        exchange = match args[i + 1].to_lowercase().as_str() {
                            "bybit" => CryptoExchange::Bybit,
                            "whitebit" => CryptoExchange::Whitebit,
                            _ => CryptoExchange::Binance,
                        };
                        i += 1;
                    }
                }
                "--symbol" => {
                    if i + 1 < args.len() {
                        symbol = args[i + 1].to_string();
                        i += 1;
                    }
                }
                "--leverage" => {
                    if i + 1 < args.len() {
                        leverage = args[i + 1].parse().unwrap_or(10.0);
                        i += 1;
                    }
                }
                "--margin" => {
                    if i + 1 < args.len() {
                        margin = args[i + 1].parse().unwrap_or(1000.0);
                        i += 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        Self { mode, crypto_exchange: exchange, symbol, leverage, margin }
    }
}
