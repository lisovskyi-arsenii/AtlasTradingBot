pub mod models;
pub mod network;
pub mod strategy;
pub mod utility;
pub mod spot;
pub mod futures;
pub mod logs;

use crate::logs::csv::write_to_csv_file;
use crate::models::bot_config::BotConfig;
use crate::models::candle::Candle;
use crate::models::candle_log_entry::CandleLogEntry;
use crate::models::data::{BacktestResult, CandleBuilder, CryptoExchange, Mode};
use crate::network::binance::binance_client::{fetch_binance, fetch_historical_candles};
use crate::network::bybit::bybit_client::fetch_bybit;
use crate::network::whitebit::whitebit_client::fetch_whitebit;
use crate::strategy::spot_strategy::SpotStrategy;
use crate::strategy::{TradingStrategy, WARMUP_PERIOD};
use crate::utility::utility::sleep;
use csv::ReaderBuilder;
use rand::Rng;
use reqwest::Client;
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;

const DEFAULT_BACKTEST_CSV_PATH: &str = "BTCUSDT-15m-2026-05.csv";

fn load_candles_from_csv(file_path: &str) -> Result<Vec<Candle>, Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_path(file_path)?;

    let mut candles: Vec<Candle> = Vec::new();

    for result in rdr.records() {
        let record = result?;

        let open: f64 = record[1].parse().unwrap_or(0.0);
        let high: f64 = record[2].parse().unwrap_or(0.0);
        let low: f64 = record[3].parse().unwrap_or(0.0);
        let close: f64 = record[4].parse().unwrap_or(0.0);
        let volume: f64 = record[5].parse().unwrap_or(0.0);

        if open <= 0.0 || high <= 0.0 || low <= 0.0 || close <= 0.0 {
            continue;
        }

        candles.push(Candle {
            open,
            high,
            low,
            close,
            volume,
        });
    }

    Ok(candles)
}

/// Run a single CSV backtest and return structured result
pub fn run_single_csv_backtest(
    file_path: &str,
    strategy: &mut SpotStrategy,
) -> Result<BacktestResult, Box<dyn Error>> {
    println!("╔═══ CSV Backtest: {} ═══╗", file_path);

    let candles: Vec<Candle> = load_candles_from_csv(file_path)?;
    if candles.is_empty() {
        return Err("No valid candles were loaded from CSV".into());
    }

    for candle in &candles {
        strategy.on_candle_close(candle);
    }

    let last_close = candles.last().map(|c| c.close).unwrap_or(0.0);
    strategy.finalize_backtest(last_close);

    let result = strategy.compute_backtest_result(last_close, file_path);
    strategy.print_backtest_summary(last_close);

    println!("╚══════════════════════════════════════════╝\n");

    Ok(result)
}

/// Scan directory for CSV files matching backtest pattern
fn find_csv_files() -> Vec<String> {
    let mut csv_files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(".") {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "csv" {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        csv_files.push(name.to_string());
                    }
                }
            }
        }
    }
    // Sort for consistent ordering
    csv_files.sort();
    csv_files
}

/// Parse symbol from CSV filename (e.g. "BTCUSDT-15m-2026-05.csv" -> "BTCUSDT")
fn parse_symbol_from_filename(filename: &str) -> String {
    let stem = Path::new(filename).file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(filename);
    // Take everything before the first '-' as the symbol
    if let Some(symbol) = stem.split('-').next() {
        symbol.to_string()
    } else {
        "UNKNOWN".to_string()
    }
}

/// Print a formatted summary table of all backtest results
fn print_results_table(all_results: &[BacktestResult]) {
    if all_results.is_empty() {
        println!("No backtest results to display.");
        return;
    }

    println!("\n");
    println!("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                            MASTER BACKTEST SUMMARY TABLE                                                 ║");
    println!("╠════════════════════╦══════════╦═══════╦════════════╦════════════╦════════════╦════════════╦════════════╦════════╦═══════════╣");
    println!("║ CSV File           ║ Symbol   ║ Trades║ Win Rate   ║ Profit Fact║ Total PnL% ║ Max DD%    ║ Sharpe     ║ Recvry ║ Max Loss  ║");
    println!("╠════════════════════╬══════════╬═══════╬════════════╬════════════╬════════════╬════════════╬════════════╬════════╬═══════════╣");

    for r in all_results {
        let pf_str = if r.profit_factor.is_infinite() {
            "   ∞    ".to_string()
        } else {
            format!("{:>8.2}", r.profit_factor)
        };

        let sharpe_str = format!("{:>8.2}", r.sharpe_ratio);
        let recovery_str = format!("{:>6.2}", r.recovery_factor);

        println!(
            "║ {:<18} ║ {:<8} ║ {:>5} ║ {:>8.1}% ║ {} ║ {:>8.2}% ║ {:>8.2}% ║ {} ║ {} ║ {:>7}  ║",
            truncate_str(&r.csv_file, 18),
            r.symbol,
            r.total_trades,
            r.win_rate,
            pf_str,
            r.total_pnl_pct,
            r.max_drawdown_pct,
            sharpe_str,
            recovery_str,
            r.max_consecutive_losses,
        );
    }

    // Calculate and print totals
    let total_initial: f64 = all_results.first().map(|r| r.initial_capital).unwrap_or(0.0);
    let total_final: f64 = all_results.iter().map(|r| r.final_equity - r.initial_capital).sum::<f64>() + total_initial;
    let total_pnl_usdt: f64 = all_results.iter().map(|r| r.total_pnl_usdt).sum();
    let total_pnl_pct = if total_initial > 0.0 {
        ((total_final - total_initial) / total_initial) * 100.0
    } else {
        0.0
    };
    let total_trades: usize = all_results.iter().map(|r| r.total_trades).sum();
    let avg_win_rate: f64 = all_results.iter().map(|r| r.win_rate).sum::<f64>() / all_results.len() as f64;
    let avg_sharpe: f64 = all_results.iter().map(|r| r.sharpe_ratio).sum::<f64>() / all_results.len() as f64;
    let max_dd: f64 = all_results.iter().map(|r| r.max_drawdown_pct).fold(0.0_f64, f64::min);

    println!("╠════════════════════╬══════════╬═══════╬════════════╬════════════╬════════════╬════════════╬════════════╬════════╬═══════════╣");
    println!(
        "║ TOTAL              ║          ║ {:>5} ║ {:>8.1}% ║            ║ {:>8.2}% ║ {:>8.2}% ║ {:>8.2}  ║        ║           ║",
        total_trades, avg_win_rate, total_pnl_pct, max_dd, avg_sharpe
    );
    println!("╚════════════════════╩══════════╩═══════╩════════════╩════════════╩════════════╩════════════╩════════════╩════════╩═══════════╝");
    println!();
    println!("Portfolio Summary:");
    println!("  Initial Capital:  ${:.2}", total_initial);
    println!("  Final Capital:    ${:.2}", total_final);
    println!("  Total PnL:        ${:.2} ({:+.2}%)", total_pnl_usdt, total_pnl_pct);
    println!("  Total Trades:     {}", total_trades);
    println!("  Avg Win Rate:     {:.1}%", avg_win_rate);
    println!("  Avg Sharpe Ratio: {:.2}", avg_sharpe);
    println!("  Worst Drawdown:   {:.2}%", max_dd);
}

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}…", &s[..max_len-1])
    }
}

/// Run backtests on ALL CSV files found in the current directory
pub fn run_multi_csv_backtest(
    csv_files: &[String],
    config: &BotConfig,
    symbol: &str,
) -> Vec<BacktestResult> {
    let mut all_results: Vec<BacktestResult> = Vec::new();

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         MULTI-FILE BACKTEST — All CSV Files Found           ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ Symbol: {:<50} ║", symbol);
    println!("║ Capital: ${:<46.2} ║", config.margin);
    println!("║ Exchange: {:?}{:<40} ║", config.crypto_exchange, "");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    for csv_file in csv_files {
        // Extract symbol from filename
        let file_symbol = parse_symbol_from_filename(csv_file);

        let (log_tx, log_rx) = mpsc::unbounded_channel::<CandleLogEntry>();
        drop(log_rx);

        let mut strategy = SpotStrategy::new(
            config.margin,
            &file_symbol,
            log_tx,
            config.crypto_exchange,
        );

        match run_single_csv_backtest(csv_file, &mut strategy) {
            Ok(result) => {
                all_results.push(result);
            }
            Err(e) => {
                eprintln!("[ERROR] Backtest failed for {}: {}", csv_file, e);
            }
        }
    }

    // Print master summary table
    print_results_table(&all_results);

    all_results
}

/// Run a single-file CSV backtest (legacy behavior)
pub fn run_csv_backtest(file_path: &str, strategy: &mut SpotStrategy) -> Result<(), Box<dyn Error>> {
    let result = run_single_csv_backtest(file_path, strategy)?;
    print_results_table(&[result]);
    Ok(())
}

/// Simulates realistic exchange network behavior:
/// - Random network latency (50-500ms)
/// - Occasional temporary errors (5% chance) to test resilience
/// - Price jitter based on exchange liquidity
async fn fetch_price_with_simulation(
    exchange: CryptoExchange,
    symbol: &str,
    client: &Client,
) -> Result<(f64, f64), Box<dyn Error>> {
    // Simulate network latency (realistic: 50-500ms depending on exchange)
    let base_latency_ms = match exchange {
        CryptoExchange::Binance => rand::thread_rng().gen_range(20..80),
        CryptoExchange::Bybit => rand::thread_rng().gen_range(30..120),
        CryptoExchange::Whitebit => rand::thread_rng().gen_range(50..200),
    };
    tokio::time::sleep(Duration::from_millis(base_latency_ms)).await;

    // Simulate occasional network errors (5% chance)
    if rand::thread_rng().gen_range(0..100) < 5 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!("[SIM] Simulated network error on {:?}", exchange),
        )));
    }

    let result = match exchange {
        CryptoExchange::Binance => fetch_binance(symbol, client).await,
        CryptoExchange::Bybit => fetch_bybit(symbol, client).await,
        CryptoExchange::Whitebit => fetch_whitebit(symbol, client).await,
    };

    match result {
        Ok(response) => {
            // Simulate bid-ask spread (more on lower-liquidity exchanges)
            let spread_pct = match exchange {
                CryptoExchange::Binance => rand::thread_rng().gen_range(0.0001..0.0005),  // 0.01-0.05%
                CryptoExchange::Bybit => rand::thread_rng().gen_range(0.0002..0.0008),    // 0.02-0.08%
                CryptoExchange::Whitebit => rand::thread_rng().gen_range(0.0005..0.002),  // 0.05-0.2%
            };

            // Mid price is what we got from API, simulate buy/sell prices around it
            let mid_price = response.price;
            let buy_price = mid_price * (1.0 + spread_pct / 2.0);
            let sell_price = mid_price * (1.0 - spread_pct / 2.0);

            Ok((buy_price, sell_price))
        }
        Err(e) => Err(Box::new(e)),
    }
}

/// Fetch historical candles with realistic delay between requests
async fn fetch_historical_candles_with_simulation(
    symbol: &str,
    limit: usize,
    client: &Client,
) -> Result<Vec<Candle>, Box<dyn Error>> {
    // Simulate delay for historical data
    tokio::time::sleep(Duration::from_millis(100)).await;

    match fetch_historical_candles(symbol, limit, client).await {
        Ok(candles) => Ok(candles),
        Err(e) => Err(Box::new(e)),
    }
}

/// Multi-symbol live trading mode
async fn run_multi_symbol_live(
    symbols: &[String],
    config: &BotConfig,
    client: &Client,
    log_tx: UnboundedSender<CandleLogEntry>,
) {
    let capital_per_symbol = config.margin / symbols.len() as f64;
    let mut strategies: HashMap<String, SpotStrategy> = HashMap::new();

    for symbol in symbols {
        let strategy = SpotStrategy::new(
            capital_per_symbol,
            symbol,
            log_tx.clone(),
            config.crypto_exchange,
        );
        strategies.insert(symbol.clone(), strategy);
    }

    println!("[BOOT] Multi-symbol mode with {} symbols. Capital per symbol: ${:.2}", symbols.len(), capital_per_symbol);

    // Warm up each strategy
    for symbol in symbols {
        println!("[BOOT] Fetching {} historical candles for {} warm-up...", WARMUP_PERIOD, symbol);
        match fetch_historical_candles_with_simulation(symbol, WARMUP_PERIOD + 1, client).await {
            Ok(history) => {
                println!("[BOOT] {}: Loaded {} historical candles. Warming up...", symbol, history.len());
                if let Some(strategy) = strategies.get_mut(symbol) {
                    for candle in &history {
                        strategy.on_candle_close(candle);
                    }
                }
            }
            Err(e) => {
                eprintln!("[BOOT] {}: Failed to fetch history: {}. Will warm up in real-time.", symbol, e);
            }
        }
    }

    println!("[BOOT] Warm-up complete. Bot is now live with {} symbols.\n", symbols.len());

    let mut current_candles: HashMap<String, Option<CandleBuilder>> = HashMap::new();
    for symbol in symbols {
        current_candles.insert(symbol.clone(), None);
    }

    let candle_timeframe = Duration::from_secs(15 * 60); // 15 min

    // Track consecutive errors for backoff
    let mut consecutive_errors: u32 = 0;

    loop {
        // Exponential backoff on errors
        if consecutive_errors > 0 {
            let backoff_secs: u64 = std::cmp::min(consecutive_errors as u64 * 5, 60);
            println!(
                "[NET] Backing off for {}s after {} consecutive errors...",
                backoff_secs, consecutive_errors
            );
            sleep(backoff_secs).await;
        }

        let mut all_ok = true;

        for symbol in symbols {
            let result = fetch_price_with_simulation(config.crypto_exchange, symbol, client).await;

            match result {
                Ok((buy_price, sell_price)) => {
                    consecutive_errors = 0;

                    // Use mid-price for candle building
                    let mid_price = (buy_price + sell_price) / 2.0;

                    let builder = current_candles.get_mut(symbol).unwrap();
                    if builder.is_none() {
                        *builder = Some(CandleBuilder::new(mid_price));
                    }

                    if let Some(ref mut b) = builder {
                        b.update(mid_price);
                    }

                    // For strategy tick
                    if let Some(strategy) = strategies.get_mut(symbol) {
                        strategy.on_tick(mid_price);
                    }

                    // Check if candle timeframe elapsed
                    if let Some(ref builder) = *current_candles.get(symbol).unwrap() {
                        if builder.start_time.elapsed() >= candle_timeframe {
                            let finished_candle = Candle {
                                open: builder.open,
                                high: builder.high,
                                low: builder.low,
                                close: builder.close,
                                volume: 0.0,
                            };

                            if let Some(strategy) = strategies.get_mut(symbol) {
                                strategy.on_candle_close(&finished_candle);

                                let equity = strategy.wallet.total_value(mid_price);
                                let pnl = ((equity - capital_per_symbol) / capital_per_symbol) * 100.0;
                                println!(
                                    "[STATUS] {}: Equity: ${:.2} ({:+.2}%) | Trades: {}",
                                    symbol, equity, pnl, strategy.total_trades()
                                );
                            }

                            *current_candles.get_mut(symbol).unwrap() = None;
                        }
                    }
                }
                Err(error) => {
                    consecutive_errors += 1;
                    all_ok = false;
                    eprintln!(
                        "[NET] {}: Error #{:?}: {}. Retrying...",
                        symbol, consecutive_errors, error
                    );
                }
            }
        }

        // Print total portfolio status
        if all_ok {
            let mut total_equity = 0.0;
            for symbol in symbols {
                if let Some(strategy) = strategies.get(symbol) {
                    // Use last known price approximation
                    total_equity += strategy.wallet.total_value(
                        strategy.wallet.usdt_balance + 1.0 // placeholder, real value from fetch
                    ) - 1.0;
                }
            }

            // Only print if we have a rough equity estimate
            if total_equity > 0.0 {
                let pnl = ((total_equity - config.margin) / config.margin) * 100.0;
                println!(
                    "[PORTFOLIO] Total Equity: ${:.2} ({:+.2}%)",
                    total_equity, pnl
                );
            }
        }

        sleep(3).await;
    }
}

#[tokio::main]
async fn main() {
    let client: Client = Client::new();
    let config: BotConfig = BotConfig::parse();

    let symbol: String = if config.symbol.is_empty() {
        "BTCUSDT".to_string()
    } else {
        config.symbol.clone()
    };

    println!("╔══════════════════════════════════════════╗");
    println!("║        RustBot Trading System v0.2       ║");
    println!("║   Multi-File Backtester + Multi-Symbol   ║");
    println!("╠══════════════════════════════════════════╣");
    println!("║ Mode: {:?}                               ", config.mode);
    println!("║ Exchange: {:?}                           ", config.crypto_exchange);
    println!("║ Symbol: {}                             ", symbol);
    println!("║ Margin: ${:.2}                          ", config.margin);
    if let Mode::Futures = config.mode {
        println!("║ Leverage: {}x                             ", config.leverage);
    }
    println!("╚══════════════════════════════════════════╝\n");

    // ---- BACKTEST MODE: Check if we should run backtest ----
    let backtest_csv_path = std::env::var("BACKTEST_CSV_PATH")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_BACKTEST_CSV_PATH.to_string());

    let run_all_csv = std::env::var("RUN_ALL_CSV").ok()
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    if run_all_csv || backtest_csv_path == "__ALL__" {
        let csv_files = find_csv_files();
        if csv_files.is_empty() {
            eprintln!("[ERROR] No CSV files found in current directory!");
            return;
        }
        println!("[BACKTEST] Found {} CSV files. Running multi-file backtest...", csv_files.len());
        let _results = run_multi_csv_backtest(&csv_files, &config, &symbol);
        return;
    }

    if !backtest_csv_path.is_empty() && backtest_csv_path != "__ALL__" {
        // Check if it's a single file or "ALL" mode
        let (log_tx, log_rx) = mpsc::unbounded_channel::<CandleLogEntry>();
        drop(log_rx);

        let mut strategy = SpotStrategy::new(
            config.margin,
            &symbol,
            log_tx,
            config.crypto_exchange,
        );

        if let Err(e) = run_csv_backtest(&backtest_csv_path, &mut strategy) {
            eprintln!("CSV backtest failed: {}", e);
        }

        return;
    }

    // ---- LIVE TRADING MODE ----
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<CandleLogEntry>();

    let _log_handle = tokio::spawn(async move {
        while let Some(entry) = log_rx.recv().await {
            if let Err(err) = write_to_csv_file(&entry).await {
                eprintln!("Failed to write candle log: {}", err);
            }
        }
    });

    // Check if multi-symbol mode is requested via env var
    let multi_symbol_env = std::env::var("SYMBOLS").ok()
        .unwrap_or_default();

    if !multi_symbol_env.is_empty() {
        let symbols: Vec<String> = multi_symbol_env
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if symbols.len() > 1 {
            println!("[BOOT] Multi-symbol mode enabled: {:?}", symbols);
            run_multi_symbol_live(&symbols, &config, &client, log_tx.clone()).await;
            return;
        }
    }

    // ---- SINGLE SYMBOL LIVE MODE (legacy) ----
    let mut strategy = SpotStrategy::new(
        config.margin,
        &symbol,
        log_tx.clone(),
        config.crypto_exchange,
    );

    println!("[BOOT] Fetching {} historical candles for warm-up...", WARMUP_PERIOD);

    match fetch_historical_candles_with_simulation(&symbol, WARMUP_PERIOD + 1, &client).await {
        Ok(history) => {
            println!("[BOOT] Loaded {} historical candles. Warming up indicators...", history.len());
            for candle in &history {
                strategy.on_candle_close(candle);
            }
            println!("[BOOT] Warm-up complete. Bot is now live.\n");
        }
        Err(e) => {
            eprintln!("[BOOT] Failed to fetch history: {}. Bot will warm up in real-time.", e);
        }
    }

    let mut current_candle: Option<CandleBuilder> = None;
    let candle_timeframe = Duration::from_secs(15 * 60); // 15 min

    // Track consecutive errors for backoff
    let mut consecutive_errors: u32 = 0;

    loop {
        // Exponential backoff on errors
        if consecutive_errors > 0 {
            let backoff_secs: u64 = std::cmp::min(consecutive_errors as u64 * 5, 60);
            println!(
                "[NET] Backing off for {}s after {} consecutive errors...",
                backoff_secs, consecutive_errors
            );
            sleep(backoff_secs).await;
        }

        let result = fetch_price_with_simulation(config.crypto_exchange, &symbol, &client).await;

        match result {
            Ok((buy_price, sell_price)) => {
                consecutive_errors = 0;

                // Use mid-price for candle building (most accurate representation)
                let mid_price = (buy_price + sell_price) / 2.0;

                if current_candle.is_none() {
                    current_candle = Some(CandleBuilder::new(mid_price));
                    println!(
                        "[LIVE] New candle started at ${:.4} ({:?})",
                        mid_price, config.crypto_exchange
                    );
                }

                let builder = current_candle.as_mut().unwrap();
                builder.update(mid_price);

                // For strategy tick, use a realistic fill price (considering we'd buy at ask or sell at bid)
                // Use buy_price for exit checks (stop-loss/take-profit are triggered by market moves)
                strategy.on_tick(mid_price);

                if builder.start_time.elapsed() >= candle_timeframe {
                    let finished_candle = Candle {
                        open: builder.open,
                        high: builder.high,
                        low: builder.low,
                        close: builder.close,
                        volume: 0.0,
                    };

                    println!(
                        "[LIVE] Candle closed: O={:.2} H={:.2} L={:.2} C={:.2}",
                        finished_candle.open, finished_candle.high,
                        finished_candle.low, finished_candle.close
                    );

                    strategy.on_candle_close(&finished_candle);

                    // Print current status
                    let equity = strategy.wallet.total_value(mid_price);
                    let pnl = ((equity - config.margin) / config.margin) * 100.0;
                    println!(
                        "[STATUS] Equity: ${:.2} ({:+.2}%) | Trades: {}",
                        equity, pnl, strategy.total_trades()
                    );

                    current_candle = None;
                }
            }
            Err(error) => {
                consecutive_errors += 1;
                eprintln!(
                    "[NET] Error #{:?}: {}. Retrying in {}s...",
                    consecutive_errors, error, 3
                );
            }
        }

        // Realistic polling interval: 3 seconds for 15-min candles
        sleep(3).await;
    }
}