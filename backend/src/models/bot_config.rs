use crate::models::data::{CryptoExchange, ExecutionMode, Mode};
use crate::models::strategy_config::{StrategyConfig, StrategyFileConfig};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct BotConfig {
    pub mode: Mode,
    pub crypto_exchange: CryptoExchange,
    pub symbol: String,
    pub leverage: f64,
    pub margin: f64,
    pub use_testnet: bool,
    pub execution_mode: ExecutionMode,
    pub strategy: StrategyConfig,
    pub runtime: RuntimeConfig,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub backtest_csv_path: String,
    /// Symbols traded in batch backtest (RUN_BATCH) and as live candidates when
    /// AUTO_SCAN is off. Lets you focus capital on the alpha-generating symbols
    /// and drop ballast that only churns fees.
    pub backtest_symbols: Vec<String>,
    /// Relative capital weight per symbol (e.g. `ETHUSDT = 2.0` gets twice the
    /// share of a coin left at the implicit default of 1.0). Lets you tilt the
    /// book toward higher-conviction symbols without changing total capital.
    /// Empty map = equal weighting (unchanged behaviour).
    pub symbol_weights: HashMap<String, f64>,
    pub live_log_path: String,
    pub candle_timeframe_seconds: u64,
    pub poll_interval_seconds: u64,
    /// Port for the Prometheus /metrics HTTP endpoint. 0 disables.
    pub metrics_port: u16,
    /// Port for the frontend dashboard server. 0 disables.
    pub dashboard_port: u16,
    /// When true, history downloader will always fetch from the real Binance API
    /// (ignoring bot.use_testnet) so backtests run on authentic market data.
    pub backtest_use_real_data: bool,
}

fn default_backtest_symbols() -> Vec<String> {
    [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT", "TRXUSDT",
        "PEPEUSDT", "SUIUSDT", "BABYUSDT", "XLMUSDT",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            backtest_csv_path: "BTCUSDT-1h-2026-05.csv".to_string(),
            backtest_symbols: default_backtest_symbols(),
            symbol_weights: HashMap::new(),
            live_log_path: "trading_bot.csv".to_string(),
            candle_timeframe_seconds: 15 * 60,
            poll_interval_seconds: 3,
            metrics_port: 9100,
            dashboard_port: 8080,
            backtest_use_real_data: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RuntimeFileConfig {
    pub backtest_csv_path: Option<String>,
    pub backtest_symbols: Option<Vec<String>>,
    pub symbol_weights: Option<HashMap<String, f64>>,
    pub live_log_path: Option<String>,
    pub candle_timeframe_seconds: Option<u64>,
    pub poll_interval_seconds: Option<u64>,
    pub metrics_port: Option<u16>,
    pub dashboard_port: Option<u16>,
    pub backtest_use_real_data: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct BotFileConfig {
    pub mode: Option<String>,
    pub exchange: Option<String>,
    pub symbol: Option<String>,
    pub leverage: Option<f64>,
    pub margin: Option<f64>,
    pub use_testnet: Option<bool>,
    pub execution_mode: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ConfigFile {
    #[serde(default)]
    pub bot: BotFileConfig,
    #[serde(default)]
    pub strategy: StrategyFileConfig,
    #[serde(default)]
    pub runtime: RuntimeFileConfig,
}

impl BotConfig {
    pub fn parse() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let config_path = Self::extract_config_path(&args);
        let file_config = Self::load_config_file(&config_path);

        let mut config = Self {
            mode: Mode::Spot,
            crypto_exchange: CryptoExchange::Binance,
            symbol: String::new(),
            leverage: 10.0,
            margin: 1000.0,
            use_testnet: false,
            execution_mode: ExecutionMode::Paper,
            strategy: StrategyConfig::default(),
            runtime: RuntimeConfig::default(),
        };

        if let Some(file_config) = file_config {
            config.apply_file_config(file_config);
        }

        config.apply_cli_overrides(&args);
        config
    }

    /// Split `total_pool` across `symbols` according to `runtime.symbol_weights`.
    /// A symbol with no explicit weight defaults to 1.0; if all weights are zero
    /// (or the map is empty) capital is split equally — i.e. unchanged behaviour.
    pub fn allocate_capital(&self, symbols: &[String], total_pool: f64) -> HashMap<String, f64> {
        let weight_of = |s: &String| -> f64 {
            self.runtime
                .symbol_weights
                .get(s)
                .copied()
                .unwrap_or(1.0)
                .max(0.0)
        };
        let total_weight: f64 = symbols.iter().map(weight_of).sum();
        symbols
            .iter()
            .map(|s| {
                let capital = if total_weight > 0.0 {
                    total_pool * weight_of(s) / total_weight
                } else {
                    total_pool / symbols.len().max(1) as f64
                };
                (s.clone(), capital)
            })
            .collect()
    }

    fn extract_config_path(args: &[String]) -> String {
        let mut config_path = std::env::var("BOT_CONFIG").unwrap_or_else(|_| "config.toml".to_string());

        let mut i = 1;
        while i < args.len() {
            if args[i] == "--config" && i + 1 < args.len() {
                config_path = args[i + 1].clone();
                break;
            }
            i += 1;
        }

        config_path
    }

    fn load_config_file(config_path: &str) -> Option<ConfigFile> {
        let path = Path::new(config_path);
        if !path.exists() {
            return None;
        }

        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) => {
                eprintln!("[CONFIG] Failed to read {}: {}", config_path, error);
                std::process::exit(1);
            }
        };

        match toml::from_str::<ConfigFile>(&contents) {
            Ok(config) => Some(config),
            Err(error) => {
                eprintln!("[CONFIG] Failed to parse {}: {}", config_path, error);
                std::process::exit(1);
            }
        }
    }

    fn apply_file_config(&mut self, file_config: ConfigFile) {
        if let Some(mode) = file_config.bot.mode {
            self.mode = Self::parse_mode(&mode);
        }

        if let Some(exchange) = file_config.bot.exchange {
            self.crypto_exchange = Self::parse_exchange(&exchange);
        }

        if let Some(symbol) = file_config.bot.symbol {
            self.symbol = symbol;
        }

        if let Some(leverage) = file_config.bot.leverage {
            self.leverage = leverage;
        }

        if let Some(margin) = file_config.bot.margin {
            self.margin = margin;
        }

        if let Some(use_testnet) = file_config.bot.use_testnet {
            self.use_testnet = use_testnet;
        }

        if let Some(exec_mode) = file_config.bot.execution_mode {
            self.execution_mode = match exec_mode.to_lowercase().as_str() {
                "testnet" => ExecutionMode::Testnet,
                "live" => ExecutionMode::Live,
                _ => ExecutionMode::Paper,
            };
        }

        self.strategy = StrategyConfig::from_file(file_config.strategy);

        if let Some(backtest_csv_path) = file_config.runtime.backtest_csv_path {
            self.runtime.backtest_csv_path = backtest_csv_path;
        }

        if let Some(backtest_symbols) = file_config.runtime.backtest_symbols {
            if !backtest_symbols.is_empty() {
                self.runtime.backtest_symbols = backtest_symbols;
            }
        }

        if let Some(symbol_weights) = file_config.runtime.symbol_weights {
            self.runtime.symbol_weights = symbol_weights;
        }

        if let Some(live_log_path) = file_config.runtime.live_log_path {
            self.runtime.live_log_path = live_log_path;
        }

        if let Some(candle_timeframe_seconds) = file_config.runtime.candle_timeframe_seconds {
            self.runtime.candle_timeframe_seconds = candle_timeframe_seconds.max(1);
        }

        if let Some(poll_interval_seconds) = file_config.runtime.poll_interval_seconds {
            self.runtime.poll_interval_seconds = poll_interval_seconds.max(1);
        }

        if let Some(metrics_port) = file_config.runtime.metrics_port {
            self.runtime.metrics_port = metrics_port;
        }

        if let Some(dashboard_port) = file_config.runtime.dashboard_port {
            self.runtime.dashboard_port = dashboard_port;
        }

        if let Some(backtest_use_real_data) = file_config.runtime.backtest_use_real_data {
            self.runtime.backtest_use_real_data = backtest_use_real_data;
        }
    }

    fn apply_cli_overrides(&mut self, args: &[String]) {
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--config" => {
                    if i + 1 < args.len() {
                        i += 1;
                    }
                }
                "--mode" => {
                    if i + 1 < args.len() {
                        self.mode = Self::parse_mode(&args[i + 1]);
                        i += 1;
                    }
                }
                "--exchange" => {
                    if i + 1 < args.len() {
                        self.crypto_exchange = Self::parse_exchange(&args[i + 1]);
                        i += 1;
                    }
                }
                "--symbol" => {
                    if i + 1 < args.len() {
                        self.symbol = args[i + 1].clone();
                        i += 1;
                    }
                }
                "--leverage" => {
                    if i + 1 < args.len() {
                        self.leverage = args[i + 1].parse().unwrap_or(self.leverage);
                        i += 1;
                    }
                }
                "--margin" => {
                    if i + 1 < args.len() {
                        self.margin = args[i + 1].parse().unwrap_or(self.margin);
                        i += 1;
                    }
                }
                _ => {}
            }
            i += 1;
        }
    }

    fn parse_mode(value: &str) -> Mode {
        match value.to_lowercase().as_str() {
            "spot" => Mode::Spot,
            "futures" => Mode::Futures,
            _ => Mode::Spot,
        }
    }

    fn parse_exchange(value: &str) -> CryptoExchange {
        match value.to_lowercase().as_str() {
            "bybit" => CryptoExchange::Bybit,
            "whitebit" => CryptoExchange::Whitebit,
            _ => CryptoExchange::Binance,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn syms(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn equal_split_when_no_weights() {
        let cfg = BotConfig::default_for_test();
        let alloc = cfg.allocate_capital(&syms(&["A", "B", "C", "D"]), 4000.0);
        for v in alloc.values() {
            assert!((v - 1000.0).abs() < 1e-9);
        }
    }

    #[test]
    fn weights_redistribute_but_conserve_total() {
        let mut cfg = BotConfig::default_for_test();
        cfg.runtime.symbol_weights = HashMap::from([("A".to_string(), 3.0)]);
        // A weight 3, B/C default 1 -> total weight 5, pool 5000.
        let alloc = cfg.allocate_capital(&syms(&["A", "B", "C"]), 5000.0);
        assert!((alloc["A"] - 3000.0).abs() < 1e-9);
        assert!((alloc["B"] - 1000.0).abs() < 1e-9);
        assert!((alloc["C"] - 1000.0).abs() < 1e-9);
        let total: f64 = alloc.values().sum();
        assert!((total - 5000.0).abs() < 1e-9);
    }

    #[test]
    fn zero_weight_excludes_symbol() {
        let mut cfg = BotConfig::default_for_test();
        cfg.runtime.symbol_weights = HashMap::from([("B".to_string(), 0.0)]);
        let alloc = cfg.allocate_capital(&syms(&["A", "B"]), 2000.0);
        assert!((alloc["A"] - 2000.0).abs() < 1e-9);
        assert!(alloc["B"].abs() < 1e-9);
    }
}

#[cfg(test)]
impl BotConfig {
    fn default_for_test() -> Self {
        Self {
            mode: Mode::Spot,
            crypto_exchange: CryptoExchange::Binance,
            symbol: String::new(),
            leverage: 1.0,
            margin: 1000.0,
            use_testnet: false,
            execution_mode: ExecutionMode::Paper,
            strategy: StrategyConfig::default(),
            runtime: RuntimeConfig::default(),
        }
    }
}
