pub mod models;
pub mod network;
pub mod strategy;
pub mod utility;
pub mod spot;
pub mod futures;
pub mod logs;
pub mod metrics;

use std::collections::HashMap;
use std::sync::Arc;
use crate::logs::csv::write_to_csv_file;
use crate::models::bot_config::BotConfig;
use crate::models::candle::Candle;
use crate::models::candle_log_entry::CandleLogEntry;
use crate::models::data::{BacktestResult, CandleBuilder, Mode};
use crate::network::binance::binance_client::fetch_historical_candles;
use crate::network::binance::binance_websocket_client::run_binance_websocket_client;
use crate::network::binance::binance_depth_client::run_binance_depth_client;
use crate::strategy::futures_strategy::FuturesTradingStrategy;
use crate::strategy::spot_strategy::SpotStrategy;
use crate::strategy::TradingStrategy;
use crate::utility::utility::sleep_milliseconds;
use crate::metrics::{BotMetrics, SymbolSnapshot, update_metrics, run_metrics_server};

use csv::ReaderBuilder;
use reqwest::Client;
use std::error::Error;
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use crate::models::log_level::LogLevel;
use crate::network::scanner::get_top_volume_pairs;
// const DEFAULT_BACKTEST_CSV_PATH: &str = "BTCUSDT-1h-2026-05.csv";

/// Автоматично скачує історичні свічки та зберігає в CSV
async fn download_history_to_csv(symbol: &str, limit: usize, client: &Client) -> Result<String, Box<dyn Error>> {
    let candles = fetch_historical_candles(symbol, limit, client).await?;
    let file_name = format!("{}-1h-auto.csv", symbol);

    let mut wtr = csv::Writer::from_path(&file_name)?;
    for c in candles {
        wtr.write_record(&[
            "0", // timestamp (заглушка)
            &c.open.to_string(),
            &c.high.to_string(),
            &c.low.to_string(),
            &c.close.to_string(),
            &c.volume.to_string(),
        ])?;
    }
    wtr.flush()?;
    Ok(file_name)
}

fn load_candles_from_csv(file_path: &str) -> Result<Vec<Candle>, Box<dyn Error>> {
    let mut rdr = ReaderBuilder::new().has_headers(false).from_path(file_path)?;
    let mut candles: Vec<Candle> = Vec::new();

    for result in rdr.records() {
        let record = result?;
        if record.len() < 6 { continue; }

        let open: f64 = record[1].parse().unwrap_or(0.0);
        let high: f64 = record[2].parse().unwrap_or(0.0);
        let low: f64 = record[3].parse().unwrap_or(0.0);
        let close: f64 = record[4].parse().unwrap_or(0.0);
        let volume: f64 = record[5].parse().unwrap_or(0.0);

        if open > 0.0 && high > 0.0 && low > 0.0 && close > 0.0 {
            candles.push(Candle { open, high, low, close, volume });
        }
    }

    if candles.len() < 250 {
        return Err(format!("Недостатньо даних у {}: всього {} свічок (потрібно мінімум 250)", file_path, candles.len()).into());
    }
    Ok(candles)
}

pub fn run_single_csv_backtest(
    file_path: &str,
    strategy: &mut SpotStrategy,
) -> Result<BacktestResult, Box<dyn Error>> {
    println!("\n╔═══ CSV Backtest: {} ═══╗", file_path);

    let candles = load_candles_from_csv(file_path)?;
    println!("[VALIDATOR] Завантажено свічок: {}", candles.len());

    // Load BTC data if circuit-breaker filter is enabled
    let btc_candles = if strategy.config.use_btc_circuit_breaker {
        let btc_file = "BTCUSDT-1h-auto.csv";
        if Path::new(btc_file).exists() {
            match load_candles_from_csv(btc_file) {
                Ok(btc_data) => {
                    println!("[BTC-CIRCUIT-BREAKER] Завантажено BTC свічок: {}", btc_data.len());
                    Some(btc_data)
                }
                Err(e) => {
                    eprintln!("[WARNING] Не вдалося завантажити BTC дані: {}. Фільтр вимкнено.", e);
                    None
                }
            }
        } else {
            eprintln!("[WARNING] Файл {} не знайдено. Фільтр вимкнено.", btc_file);
            None
        }
    } else {
        None
    };

    for (i, candle) in candles.iter().enumerate() {
        // Update BTC price if filter is enabled and data is available
        if let Some(ref btc_data) = btc_candles {
            if i < btc_data.len() {
                strategy.update_btc_price(btc_data[i].close);
            }
        }
        strategy.on_candle_close(candle);
    }

    let last_close = candles.last().map(|c| c.close).unwrap_or(0.0);
    strategy.finalize_backtest(last_close);

    let result = strategy.compute_backtest_result(last_close, file_path);
    strategy.print_backtest_summary(last_close);

    println!("╚══════════════════════════════════════════╝");
    Ok(result)
}

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len { s.to_string() } else { format!("{}…", &s[..max_len - 1]) }
}

fn print_results_table(all_results: &[BacktestResult], config: &BotConfig) {
    if all_results.is_empty() { return; }

    println!("\n╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║ MASTER BACKTEST SUMMARY TABLE                                                                                                ║");
    println!("╠════════════════════╦══════════╦═══════╦════════════╦════════════╦════════════╦════════════╦════════════╦════════╦═══════════╣");
    println!("║ CSV File           ║ Symbol   ║ Trades║ Win Rate   ║ Profit Fact║ Total PnL% ║ Max DD%    ║ Sharpe     ║ Recvry ║ Max Loss  ║");
    println!("╠════════════════════╬══════════╬═══════╬════════════╬════════════╬════════════╬════════════╬════════════╬════════╬═══════════╣");

    for r in all_results {
        let pf_str = if r.profit_factor.is_infinite() { "       ∞".to_string() } else { format!("{:>8.2}", r.profit_factor) };
        println!(
            "║ {:<18} ║ {:<8} ║ {:>5} ║ {:>8.1}% ║ {} ║ {:>8.2}% ║ {:>8.2}% ║ {:>8.2} ║ {:>6.2} ║ {:>9} ║",
            truncate_str(&r.csv_file, 18), r.symbol, r.total_trades, r.win_rate, pf_str,
            r.total_pnl_pct, r.max_drawdown_pct, r.sharpe_ratio, r.recovery_factor, r.max_consecutive_losses,
        );
    }

    // Розрахунок підсумкових показників
    let total_initial = config.margin * all_results.len() as f64;
    let total_pnl_usdt: f64 = all_results.iter().map(|r| r.total_pnl_usdt).sum();
    let total_final = total_initial + total_pnl_usdt;
    let total_pnl_pct = if total_initial > 0.0 { (total_pnl_usdt / total_initial) * 100.0 } else { 0.0 };

    let total_trades: usize = all_results.iter().map(|r| r.total_trades).sum();
    let avg_win_rate = all_results.iter().map(|r| r.win_rate).sum::<f64>() / all_results.len() as f64;
    let avg_sharpe = all_results.iter().map(|r| r.sharpe_ratio).sum::<f64>() / all_results.len() as f64;
    let max_dd = all_results.iter().map(|r| r.max_drawdown_pct).fold(0.0_f64, f64::min);

    println!("╠════════════════════╬══════════╬═══════╬════════════╬════════════╬════════════╬════════════╬════════════╬════════╦═══════════╣");
    println!("║ TOTAL              ║          ║ {:>5} ║ {:>8.1}% ║            ║ {:>8.2}% ║ {:>8.2}% ║ {:>8.2} ║        ║           ║",
             total_trades, avg_win_rate, total_pnl_pct, max_dd, avg_sharpe);
    println!("╚════════════════════╩══════════╩═══════╩════════════╩════════════╩════════════╩════════════╩════════════╩════════╩═══════════╝\n");

    println!("Portfolio Summary:");
    println!("  Initial Capital: ${:.2}", total_initial);
    println!("  Final Capital:   ${:.2}", total_final);
    println!("  Total PnL:       ${:.2} ({:+.2}%)", total_pnl_usdt, total_pnl_pct);
    println!("  Total Trades:    {}", total_trades);
    println!("  Avg Win Rate:    {:.1}%", avg_win_rate);
    println!("  Worst Drawdown:  {:.2}%", max_dd);
}

#[tokio::main]
async fn main() {
    let config = BotConfig::parse();
    let symbol = if config.symbol.is_empty() { "BTCUSDT".to_string() } else { config.symbol.clone() };
    let log_level: LogLevel = LogLevel::from_env();

    println!("╔══════════════════════════════════════════╗");
    println!("║       RustBot Trading System v0.2        ║");
    println!("╚══════════════════════════════════════════╝\n");

    // ==========================================
    // 1. РЕЖИМ БАТЧ-БЕКТЕСТУ (МАСОВИЙ АНАЛІЗ)
    // ==========================================
    if std::env::var("RUN_BATCH").is_ok() {
        let client = Client::new();
        let mut symbols_to_test: Vec<String> = Vec::new();

        // Якщо додано AUTO_SCAN, сканер сам знайде пари для бектесту
        if std::env::var("AUTO_SCAN").is_ok() {
            println!("[SCANNER] Запитуємо топ-10 пар за об'ємом з Binance для тестування...");
            if let Ok(top_pairs) = get_top_volume_pairs(&client, 10).await {
                symbols_to_test = top_pairs;
            }
        } else {
            // Інакше використовуємо список з config (runtime.backtest_symbols)
            symbols_to_test = config.runtime.backtest_symbols.clone();
        }

        let mut all_results = Vec::new();
        println!("[BATCH] Запуск пакетного тестування для {} пар...", symbols_to_test.len());

        // Total pool keeps the per-run capital the same as the equal-weight case
        // (margin per symbol); weights only redistribute it between symbols.
        let total_pool = config.margin * symbols_to_test.len() as f64;
        let capital_by_symbol = config.allocate_capital(&symbols_to_test, total_pool);

        for sym in symbols_to_test {
            let symbol_capital = capital_by_symbol.get(&sym).copied().unwrap_or(config.margin);
            if !config.runtime.symbol_weights.is_empty() {
                println!("[ALLOC] {}: ${:.2}", sym, symbol_capital);
            }
            let file_name = format!("{}-1h-auto.csv", sym);

            // Якщо даних немає, завантажуємо їх
            if !Path::new(&file_name).exists() {
                println!("[NETWORK] Скачування даних за рік (8760 год) для {}...", sym);
                if let Err(e) = download_history_to_csv(&sym, 8760, &client).await {
                    eprintln!("[ERROR] Не вдалося скачати дані для {}: {}", sym, e);
                    continue; // Пропускаємо монету при помилці
                }
            }

            let (log_tx, _log_rx) = mpsc::unbounded_channel();
            let mut strategy = SpotStrategy::new(
                symbol_capital, &sym, log_tx, config.crypto_exchange, config.strategy.clone(), log_level.clone()
            );

            // Проганяємо бектест на датасеті
            match run_single_csv_backtest(&file_name, &mut strategy) {
                Ok(res) => all_results.push(res),
                Err(e) => eprintln!("[ERROR] Бектест {} провалився: {}", sym, e),
            }
        }

        // Виводимо фінальну таблицю з результатами
        print_results_table(&all_results, &config);
        return;
    }

    // ==========================================
    // 2. РЕЖИМ ОДИНИЧНОГО БЕКТЕСТУ
    // ==========================================
    let backtest_csv = std::env::var("BACKTEST_CSV_PATH").unwrap_or_else(|_| config.runtime.backtest_csv_path.clone());
    if !backtest_csv.is_empty() {
        let (log_tx, _log_rx) = mpsc::unbounded_channel();
        let mut strategy = SpotStrategy::new(
            config.margin, &symbol, log_tx, config.crypto_exchange, config.strategy.clone(), log_level.clone()
        );

        if Path::new(&backtest_csv).exists() {
            if let Err(e) = run_single_csv_backtest(&backtest_csv, &mut strategy) {
                eprintln!("[ERROR] CSV backtest failed: {}", e);
            }
        } else {
            eprintln!("[ERROR] Файл {} не знайдено!", backtest_csv);
        }
        return;
    }

    // ==========================================
    // 3. РЕЖИМ LIVE (WebSocket + Auto Scanner)
    // ==========================================
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<CandleLogEntry>();
    let live_log_path = config.runtime.live_log_path.clone();

    // Запис логів у фоні
    tokio::spawn(async move {
        while let Some(entry) = log_rx.recv().await {
            let _ = write_to_csv_file(&entry, &live_log_path).await;
        }
    });

    let client = Client::new();
    let mut best_live_symbols = vec![symbol.clone()];

    // 3.1 Пошук найкращих пар
    if std::env::var("AUTO_SCAN").is_ok() {
        println!("[SCANNER] Пошук найліквідніших пар на Binance...");
        if let Ok(top_pairs) = get_top_volume_pairs(&client, 5).await {
            println!("[SCANNER] Знайдено топ пари за об'ємом: {:?}", top_pairs);
            for sym in top_pairs {
                if !best_live_symbols.contains(&sym) {
                    best_live_symbols.push(sym);
                }
            }
        }
    }
    // Обмежуємо до 5 пар, щоб не перевантажувати API
    best_live_symbols.truncate(5);
    println!("[BOOT] Пари для Live торгівлі: {:?}", best_live_symbols);

    // 3.2 Ініціалізація та ПРОГРІВ стратегій
    // Distribute the total live capital across symbols by configured weights
    // (equal split when no weights are set).
    let live_capital_by_symbol = config.allocate_capital(&best_live_symbols, config.margin);
    let mut strategies: HashMap<String, Box<dyn TradingStrategy>> = HashMap::new();
    let warmup_period = config.strategy.warmup_period();

    for sym in &best_live_symbols {
        let capital_per_symbol = live_capital_by_symbol
            .get(sym)
            .copied()
            .unwrap_or(config.margin / best_live_symbols.len().max(1) as f64);
        let mut strategy: Box<dyn TradingStrategy> = match config.mode {
            Mode::Spot => Box::new(SpotStrategy::new(capital_per_symbol, sym, log_tx.clone(), config.crypto_exchange, config.strategy.clone(), log_level.clone())),
            Mode::Futures => Box::new(FuturesTradingStrategy::new(capital_per_symbol, sym, log_tx.clone(), config.crypto_exchange, config.strategy.clone(), config.leverage)),
        };

        println!("[BOOT] Warm-up: fetching {} candles for {}...", warmup_period, sym);
        if let Ok(history) = fetch_historical_candles(sym, warmup_period + 1, &client).await {
            for candle in &history { strategy.on_candle_close(candle); }
            println!("[BOOT] {} warmed up successfully.", sym);
        } else {
            eprintln!("[ERROR] Не вдалося завантажити історію для {}. Пропускаємо.", sym);
            continue;
        }

        strategies.insert(sym.clone(), strategy);
        sleep_milliseconds(200).await; // Пауза, щоб не отримати бан від біржі
    }

    if strategies.is_empty() {
        eprintln!("[ERROR] Немає жодної готової стратегії. Бот зупиняється.");
        return;
    }

    // 3.3 Запуск WebSocket для всіх підготовлених пар
    // Центральний канал для f64 перетворюється на (String, f64), щоб знати від кого ціна
    let (main_price_tx, mut main_price_rx) = mpsc::unbounded_channel::<(String, f64)>();

    // Optional LIVE-only order-book-imbalance feed (one channel for all symbols).
    let (obi_tx, mut obi_rx) = mpsc::unbounded_channel::<(String, f64)>();
    let use_obi = config.strategy.use_order_book_filter;
    let obi_levels = config.strategy.obi_depth_levels;
    if use_obi {
        println!(
            "[BOOT] Order-book filter ON (levels={}, threshold={:.2}).",
            obi_levels, config.strategy.obi_threshold
        );
    }

    for sym in strategies.keys() {
        let ws_symbol = sym.clone();
        let (local_tx, mut local_rx) = mpsc::unbounded_channel::<f64>();
        let main_tx_clone = main_price_tx.clone();
        let sym_clone = sym.clone();

        // Спавнимо клієнт
        tokio::spawn(async move {
            run_binance_websocket_client(&ws_symbol, local_tx).await;
        });

        // Форвардимо ціну з прив'язкою до символу
        tokio::spawn(async move {
            while let Some(price) = local_rx.recv().await {
                let _ = main_tx_clone.send((sym_clone.clone(), price));
            }
        });

        // Окремий стрім стакану для OBI-фільтра (лише якщо ввімкнено).
        if use_obi {
            let depth_symbol = sym.clone();
            let (depth_local_tx, mut depth_local_rx) = mpsc::unbounded_channel::<f64>();
            let obi_tx_clone = obi_tx.clone();
            let obi_sym = sym.clone();
            tokio::spawn(async move {
                run_binance_depth_client(&depth_symbol, obi_levels, depth_local_tx).await;
            });
            tokio::spawn(async move {
                while let Some(obi) = depth_local_rx.recv().await {
                    let _ = obi_tx_clone.send((obi_sym.clone(), obi));
                }
            });
        }
    }

    println!("[BOOT] Live mode engaged for {} symbols!", strategies.len());

    // 3.5 Start Prometheus metrics server (if configured)
    let bot_metrics = Arc::new(BotMetrics::new());
    let metrics_port = config.runtime.metrics_port;
    if metrics_port > 0 {
        let metrics_clone = Arc::clone(&bot_metrics);
        tokio::spawn(async move {
            run_metrics_server(metrics_clone, metrics_port).await;
        });
    }

    // 3.4 Головний цикл торгівлі
    let mut current_candles: HashMap<String, CandleBuilder> = HashMap::new();
    let candle_timeframe = Duration::from_secs(config.runtime.candle_timeframe_seconds);
    let mut last_candle_time = Instant::now();

    loop {
        // Підтягуємо найсвіжіший дисбаланс стакану перед обробкою тіків.
        while let Ok((sym, obi)) = obi_rx.try_recv() {
            if let Some(strategy) = strategies.get_mut(&sym) {
                strategy.set_order_book_imbalance(obi);
            }
        }

        // Читаємо ціну та її символ
        if let Ok((sym, price)) = main_price_rx.try_recv() {
            if let Some(strategy) = strategies.get_mut(&sym) {
                // Відправляємо тік
                strategy.on_tick(price);

                // Будуємо свічку
                let builder = current_candles.entry(sym.clone()).or_insert_with(|| CandleBuilder::new(price));
                builder.update(price);

                // Якщо час вийшов — закриваємо свічки для ВСІХ пар
                if last_candle_time.elapsed() >= candle_timeframe {
                    let mut total_equity = 0.0;
                    let mut snapshots: Vec<SymbolSnapshot> = Vec::new();

                    for (s, b) in current_candles.iter_mut() {
                        let candle = Candle { open: b.open, high: b.high, low: b.low, close: b.close, volume: 0.0 };
                        if let Some(strategy) = strategies.get_mut(s) {
                            strategy.on_candle_close(&candle);
                            println!("[LIVE] {} Candle: O={:.2} H={:.2} L={:.2} C={:.2}", s, candle.open, candle.high, candle.low, candle.close);

                            // Збираємо еквайті для портфельного статусу
                            total_equity += strategy.final_equity(candle.close);

                            // Create metrics snapshot for SpotStrategy
                            if let Some(spot_strat) = strategy.as_any().downcast_ref::<SpotStrategy>() {
                                snapshots.push(spot_strat.to_metrics_snapshot(candle.close));
                            }
                        }
                        // Скидаємо свічку використовуючи останню ціну закриття
                        *b = CandleBuilder::new(candle.close);
                    }

                    // Update Prometheus metrics
                    if !snapshots.is_empty() {
                        update_metrics(&bot_metrics, &snapshots, config.margin);
                    }

                    let pnl = ((total_equity - config.margin) / config.margin) * 100.0;
                    println!("===================================================");
                    println!("[PORTFOLIO STATUS] Total Equity: ${:.2} ({:+.2}%)", total_equity, pnl);
                    println!("===================================================");

                    last_candle_time = Instant::now();
                }
            }
        }
        sleep_milliseconds(5).await;
    }
}
