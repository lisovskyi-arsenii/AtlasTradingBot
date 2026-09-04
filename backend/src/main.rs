use RustBot::execution::binance_broker::BinanceBroker;
use RustBot::execution::state::StateManager;
use RustBot::execution::reconciliation::reconcile_startup;

use std::collections::HashMap;
use std::sync::Arc;
use RustBot::alerts::telegram;
use RustBot::logs::csv::write_to_csv_file;
use RustBot::logs::json::write_to_jsonl_file;
use RustBot::models::bot_config::BotConfig;
use RustBot::models::candle::Candle;
use RustBot::models::candle_log_entry::CandleLogEntry;
use RustBot::models::data::{BacktestResult, Mode};
use RustBot::network::binance::binance_client::fetch_historical_candles_with_testnet;
use RustBot::network::binance::binance_kline_client::{run_binance_kline_client, seconds_to_kline_interval};
use RustBot::network::binance::binance_websocket_client::run_binance_websocket_client;
use RustBot::network::binance::binance_depth_client::run_binance_depth_client;
use RustBot::network::binance::book_ticker_client::run_book_ticker_client;
use RustBot::network::binance::exchange_info::fetch_symbol_filters;
use RustBot::network::binance::fear_greed::fetch_fear_greed_index;
use RustBot::risk::kill_switch::{KillSwitch, run_control_server};
use RustBot::risk::risk_manager::RiskManager;
use RustBot::strategy::spot_strategy::SpotStrategy;
use RustBot::metrics::{BotMetrics, SymbolSnapshot, update_metrics, run_metrics_server};
use RustBot::dashboard::server::{DashboardState, PositionInfo, run_dashboard_server};
use RustBot::utility::walk_forward::{run_walk_forward, print_walk_forward_report};

use csv::ReaderBuilder;
use reqwest::Client;
use std::error::Error;
use std::path::Path;
use tokio::sync::mpsc;
use RustBot::models::log_level::LogLevel;
use RustBot::network::scanner::get_top_volume_pairs;
use RustBot::strategy::TradingStrategy;

use RustBot::execution::{ExecutionBroker, OrderRequest, OrderSide, OrderType, ClientOrderId, OrderAck};
use RustBot::execution::state::PositionState;

// ── History download ──────────────────────────────────────────────────────────

fn seconds_to_interval(seconds: u64) -> &'static str {
    match seconds {
        60 => "1m",
        180 => "3m",
        300 => "5m",
        900 => "15m",
        1800 => "30m",
        3600 => "1h",
        7200 => "2h",
        14400 => "4h",
        21600 => "6h",
        28800 => "8h",
        43200 => "12h",
        86400 => "1d",
        _ => "15m", // default to 15m if weird timeframe
    }
}

/// Download `limit` hourly candles (paginated, bypasses the old 1000-bar cap).
async fn download_history_to_csv(
    symbol: &str,
    interval: &str,
    limit: usize,
    client: &Client,
    use_testnet: bool,
) -> Result<String, Box<dyn Error>> {
    println!("[DOWNLOAD] Fetching {} candles for {}...", limit, symbol);
    let candles = fetch_historical_candles_with_testnet(symbol, interval, limit, client, use_testnet).await?;
    let file_name = format!("{}-{}-auto.csv", symbol, interval);

    let mut wtr = csv::Writer::from_path(&file_name)?;
    for c in &candles {
        wtr.write_record(&[
            "0",
            &c.open.to_string(),
            &c.high.to_string(),
            &c.low.to_string(),
            &c.close.to_string(),
            &c.volume.to_string(),
        ])?;
    }
    wtr.flush()?;
    println!("[DOWNLOAD] Saved {} candles for {} → {}", candles.len(), symbol, file_name);
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
        return Err(format!(
            "Insufficient data in {}: only {} candles (need ≥250)",
            file_path, candles.len()
        ).into());
    }
    Ok(candles)
}

/// Buffer between a protective stop's trigger price and its resting limit
/// price, so the STOP_LOSS_LIMIT order stays marketable once triggered
/// instead of sitting unfilled past the stop level in a fast move.
const STOP_LIMIT_BUFFER_PCT: f64 = 0.005;

/// Minimum trailing-stop move (as a fraction of price) before we bother
/// cancelling and replacing the resting exchange-side stop order. `on_tick`
/// fires on every price update, so without this threshold a trailing stop
/// would cancel/replace on almost every tick and burn through Binance's
/// order rate limit.
const STOP_REPLACE_THRESHOLD_PCT: f64 = 0.002;

/// Round `value` to the nearest multiple of `tick`. Binance's PRICE_FILTER
/// rejects any price that isn't an exact multiple of the symbol's tick size
/// (e.g. 0.01 for BTCUSDT) — a raw `price * (1 ± buffer)` computation has
/// far more decimal digits than that and gets rejected with `-1013 Filter
/// failure: PRICE_FILTER` every time. `tick <= 0.0` (filters not loaded)
/// falls back to returning the value unrounded rather than dividing by zero.
fn round_to_tick(value: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return value;
    }
    (value / tick).round() * tick
}

/// Place (or replace) the exchange-side protective STOP_LOSS_LIMIT order for
/// `state`. Returns the new order's `client_id` on success so the caller can
/// persist it on the position row.
///
/// Long position → SELL stop below price. Short position → BUY stop above
/// price (only reachable once real margin/futures shorting is wired up;
/// spot shorts are disabled in config today). `tick_size` is the symbol's
/// price filter tick from `exchangeInfo` — pass `0.0` only if it's
/// genuinely unavailable (this skips rounding and risks a PRICE_FILTER
/// rejection, so avoid it whenever `SymbolFilters` has already been fetched).
async fn place_protective_stop(
    broker: &Arc<dyn ExecutionBroker>,
    symbol: &str,
    state: &PositionState,
    tick_size: f64,
) -> Result<String, String> {
    // `trailing_stop_price` only gets set once the strategy has tightened
    // the stop past its initial level — a fresh entry (or one recovered via
    // reconciliation before the first trail update) reports it as 0. Fall
    // back to `initial_stop_price`, the ATR-based stop fixed at entry.
    let raw_stop_price = if state.trailing_stop_price > 0.0 {
        state.trailing_stop_price
    } else {
        state.initial_stop_price
    };
    if raw_stop_price <= 0.0 || state.qty <= 0.0 {
        return Err(format!(
            "invalid stop inputs (trailing={:.8}, initial={:.8}, qty={:.8})",
            state.trailing_stop_price, state.initial_stop_price, state.qty
        ));
    }

    let (side, raw_limit_price) = if state.is_short {
        (OrderSide::Buy, raw_stop_price * (1.0 + STOP_LIMIT_BUFFER_PCT))
    } else {
        (OrderSide::Sell, raw_stop_price * (1.0 - STOP_LIMIT_BUFFER_PCT))
    };
    let stop_price = round_to_tick(raw_stop_price, tick_size);
    let limit_price = round_to_tick(raw_limit_price, tick_size);

    let client_id = ClientOrderId::new(symbol, side);
    let req = OrderRequest {
        symbol: symbol.to_string(),
        client_id: client_id.clone(),
        side,
        order_type: OrderType::StopLossLimit,
        qty: state.qty,
        price: Some(limit_price),
        stop_price: Some(stop_price),
    };

    match broker.place_order(req).await {
        Ok(ack) => {
            println!(
                "[EXECUTION] Placed protective stop for {}: {:?} {:.8} @ stop {:.8} (limit {:.8})",
                symbol, side, state.qty, stop_price, limit_price
            );
            Ok(ack.client_id.0)
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Cancel a previously-placed resting stop order. Failure is logged, not
/// propagated — the order may already be gone (filled by the exchange, or
/// cancelled in a prior run) and that must never block the market order that
/// follows a cancel-then-replace sequence.
async fn cancel_resting_stop(broker: &Arc<dyn ExecutionBroker>, symbol: &str, stop_order_id: &str) {
    let id = ClientOrderId(stop_order_id.to_string());
    if let Err(e) = broker.cancel(symbol, &id).await {
        eprintln!(
            "[EXECUTION] Cancel of resting stop {} for {} failed (may already be filled/gone): {}",
            stop_order_id, symbol, e
        );
    }
}

async fn sync_broker_state(
    symbol: &str,
    old_state: &Option<PositionState>,
    new_state: &Option<PositionState>,
    broker: &Arc<dyn ExecutionBroker>,
    state_manager: &StateManager,
    tick_size: f64,
) {
    if old_state == new_state {
        return; // No change
    }

    // PaperBroker fills every order immediately, including "stop" orders —
    // see `PaperBroker::place_order`. Placing a resting protective stop
    // there would instantly close the paper position, so exchange-side
    // stops only make sense against a real broker.
    let use_exchange_stops = broker.name() != "PaperBroker";

    // `strategy.get_position_state()` never knows about broker-side order
    // ids (see the `stop_order_id` doc comment on `PositionState`), so the
    // only place that remembers the currently-resting stop is what we
    // persisted last time. Load it before we overwrite anything.
    let persisted_old = state_manager.load_position(symbol).await.ok().flatten();
    let old_stop_order_id = persisted_old.and_then(|p| p.stop_order_id);

    let old_qty = old_state.as_ref().map(|s| {
        if s.is_holding { s.qty } else if s.is_short { -s.qty } else { 0.0 }
    }).unwrap_or(0.0);

    let new_qty = new_state.as_ref().map(|s| {
        if s.is_holding { s.qty } else if s.is_short { -s.qty } else { 0.0 }
    }).unwrap_or(0.0);

    let delta = new_qty - old_qty;

    // The state we'll actually persist — enriched with whatever stop order
    // id results from the actions below (strategy-reported `new_state` never
    // carries one).
    let mut to_persist = new_state.clone();

    if delta.abs() > 0.00000001 {
        // Position size is changing: opening, closing, or flipping side.
        // Any resting stop from the *previous* position is now wrong (wrong
        // side, wrong qty, or the position it protected no longer exists)
        // and must go before we touch the market position.
        if use_exchange_stops && old_qty.abs() > 0.00000001 {
            if let Some(id) = old_stop_order_id.as_deref() {
                cancel_resting_stop(broker, symbol, id).await;
            }
        }

        let side = if delta > 0.0 { OrderSide::Buy } else { OrderSide::Sell };
        let req = OrderRequest {
            symbol: symbol.to_string(),
            client_id: ClientOrderId::new(symbol, side),
            side,
            order_type: OrderType::Market,
            qty: delta.abs(),
            price: None,
            stop_price: None,
        };

        println!("[EXECUTION] Mirroring strategy state: {:?} {:.8} {}", side, delta.abs(), symbol);
        match broker.place_order(req).await {
            Ok(ack) => {
                println!("[EXECUTION] Successfully mirrored. Avg Price: {:.2}, Filled: {:.8}", ack.avg_price, ack.filled_qty);

                // Position is now open (or flipped) — plant a fresh
                // exchange-side stop so it's protected even if this process
                // dies before the next tick.
                if use_exchange_stops {
                    if let Some(ref mut ns) = to_persist {
                        match place_protective_stop(broker, symbol, ns, tick_size).await {
                            Ok(stop_id) => ns.stop_order_id = Some(stop_id),
                            Err(e) => {
                                eprintln!("[EXECUTION-ERROR] No exchange-side stop for {}: {}", symbol, e);
                                let _ = telegram::alert_critical(
                                    "STOP_ORDER_FAILED",
                                    &format!("{} is open with NO exchange-side protective stop: {}", symbol, e),
                                ).await;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("[EXECUTION-ERROR] Failed to mirror trade on {}: {}", symbol, e);
                let _ = telegram::alert_critical("EXECUTION_FAILED", &format!("Failed to execute {} on {}: {}", delta, symbol, e)).await;
            }
        }
    } else if use_exchange_stops {
        // No size change — check whether the trailing stop moved far enough
        // to be worth cancelling and replacing the resting order.
        if let (Some(os), Some(ref mut ns)) = (old_state.as_ref(), to_persist.as_mut()) {
            let ref_price = os.trailing_stop_price.max(ns.trailing_stop_price).max(1e-9);
            let moved_pct = (os.trailing_stop_price - ns.trailing_stop_price).abs() / ref_price;

            if moved_pct > STOP_REPLACE_THRESHOLD_PCT {
                if let Some(id) = old_stop_order_id.as_deref() {
                    cancel_resting_stop(broker, symbol, id).await;
                }
                match place_protective_stop(broker, symbol, ns, tick_size).await {
                    Ok(stop_id) => ns.stop_order_id = Some(stop_id),
                    Err(e) => {
                        eprintln!("[EXECUTION-ERROR] Failed to replace trailing stop for {}: {}", symbol, e);
                        // Keep the old id rather than silently dropping protection —
                        // it may still be resting on the exchange at the old level.
                        ns.stop_order_id = old_stop_order_id.clone();
                    }
                }
            } else {
                // Stop didn't move enough to replace — carry the existing
                // resting order id forward so we don't lose track of it.
                ns.stop_order_id = old_stop_order_id.clone();
            }
        }
    }

    // Persist the final state (including whatever stop_order_id resulted above).
    if let Some(s) = to_persist.as_ref() {
        if let Err(e) = state_manager.save_position(s).await {
            eprintln!("[ERROR] Failed to save state to SQLite: {}", e);
        }
    } else if let Err(e) = state_manager.delete_position(symbol).await {
        eprintln!("[ERROR] Failed to delete state from SQLite: {}", e);
    }
}

// ── Single CSV backtest ───────────────────────────────────────────────────────

pub fn run_single_csv_backtest(
    file_path: &str,
    strategy: &mut SpotStrategy,
) -> Result<BacktestResult, Box<dyn Error>> {
    println!("\n╔═══ CSV Backtest: {} ═══╗", file_path);

    let candles = load_candles_from_csv(file_path)?;
    println!("[VALIDATOR] Loaded candles: {}", candles.len());

    let btc_candles = if strategy.config.use_btc_circuit_breaker {
        let btc_file = "BTCUSDT-1h-auto.csv";
        if Path::new(btc_file).exists() {
            match load_candles_from_csv(btc_file) {
                Ok(btc_data) => {
                    println!("[BTC-CIRCUIT-BREAKER] Loaded {} BTC candles", btc_data.len());
                    Some(btc_data)
                }
                Err(e) => { eprintln!("[WARNING] BTC data load failed: {}. Filter disabled.", e); None }
            }
        } else {
            eprintln!("[WARNING] {} not found. Filter disabled.", btc_file);
            None
        }
    } else {
        None
    };

    for (i, candle) in candles.iter().enumerate() {
        if let Some(ref btc) = btc_candles {
            if i < btc.len() { strategy.update_btc_price(btc[i].close); }
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

// ── Walk-forward mode ─────────────────────────────────────────────────────────

fn run_walk_forward_mode(
    file_path: &str,
    config: &BotConfig,
    _log_level: LogLevel,
) -> Result<(), Box<dyn Error>> {
    let candles = load_candles_from_csv(file_path)?;
    println!("[WF] Loaded {} candles from {}", candles.len(), file_path);

    let train_bars: usize = std::env::var("WF_TRAIN_BARS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    let test_bars: usize = std::env::var("WF_TEST_BARS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    let step_bars: usize = std::env::var("WF_STEP_BARS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);

    println!(
        "[WF] Parameters: train={} test={} step={} bars",
        train_bars, test_bars, step_bars
    );

    match run_walk_forward(
        &candles,
        &config.strategy,
        config.margin,
        &config.symbol,
        train_bars,
        test_bars,
        step_bars,
    ) {
        Some((windows, summary)) => print_walk_forward_report(&windows, &summary),
        None => eprintln!("[WF] Not enough data for even one walk-forward window."),
    }

    Ok(())
}

// ── Results table ─────────────────────────────────────────────────────────────

fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len { s.to_string() } else { format!("{}…", &s[..max_len - 1]) }
}

fn print_results_table(all_results: &[BacktestResult], config: &BotConfig) {
    if all_results.is_empty() { return; }

    println!("\n╔═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║ MASTER BACKTEST SUMMARY TABLE                                                                                               ║");
    println!("╠════════════════════╦══════════╦═══════╦════════════╦════════════╦════════════╦════════════╦════════════╦════════╦══════════╣");
    println!("║ CSV File           ║ Symbol   ║ Trades║ Win Rate   ║ Profit Fact║ Total PnL% ║ Max DD%    ║ Sharpe     ║ Recvry ║ Max Loss ║");
    println!("╠════════════════════╬══════════╬═══════╬════════════╬════════════╬════════════╬════════════╬════════════╬════════╬══════════╣");

    for r in all_results {
        let pf_str = if r.profit_factor.is_infinite() { "       ∞".to_string() } else { format!("{:>8.2}", r.profit_factor) };
        println!(
            "║ {:<18} ║ {:<8} ║ {:>5} ║ {:>8.1}% ║ {} ║ {:>8.2}% ║ {:>8.2}% ║ {:>8.2} ║ {:>6.2} ║ {:>8} ║",
            truncate_str(&r.csv_file, 18), r.symbol, r.total_trades, r.win_rate, pf_str,
            r.total_pnl_pct, r.max_drawdown_pct, r.sharpe_ratio, r.recovery_factor, r.max_consecutive_losses,
        );
    }

    let total_initial = config.margin * all_results.len() as f64;
    let total_pnl_usdt: f64 = all_results.iter().map(|r| r.total_pnl_usdt).sum();
    let total_final = total_initial + total_pnl_usdt;
    let total_pnl_pct = if total_initial > 0.0 { (total_pnl_usdt / total_initial) * 100.0 } else { 0.0 };
    let total_trades: usize = all_results.iter().map(|r| r.total_trades).sum();
    let avg_win_rate = all_results.iter().map(|r| r.win_rate).sum::<f64>() / all_results.len() as f64;
    let avg_sharpe = all_results.iter().map(|r| r.sharpe_ratio).sum::<f64>() / all_results.len() as f64;
    let max_dd = all_results.iter().map(|r| r.max_drawdown_pct).fold(0.0_f64, f64::min);

    println!("╠════════════════════╬══════════╬═══════╬════════════╬════════════╬════════════╬════════════╬════════════╬════════╬══════════╣");
    println!("║ TOTAL              ║          ║ {:>5} ║ {:>8.1}% ║            ║ {:>8.2}% ║ {:>8.2}% ║ {:>8.2} ║        ║          ║",
             total_trades, avg_win_rate, total_pnl_pct, max_dd, avg_sharpe);
    println!("╚════════════════════╩══════════╩═══════╩════════════╩════════════╩════════════╩════════════╩════════════╩════════╩══════════╝\n");

    println!("Portfolio: Initial ${:.2}  →  Final ${:.2}  (PnL: ${:+.2} / {:+.2}%)",
             total_initial, total_final, total_pnl_usdt, total_pnl_pct);
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let config = BotConfig::parse();
    let symbol = if config.symbol.is_empty() { "BTCUSDT".to_string() } else { config.symbol.clone() };
    let log_level: LogLevel = LogLevel::Debug;
    let use_testnet = config.use_testnet;

    println!("╔══════════════════════════════════════════╗");
    println!("║       AtlasTradingBot v0.4               ║");
    if use_testnet {
        println!("║       *** TESTNET MODE ***               ║");
    }
    println!("╚══════════════════════════════════════════╝\n");

    // P0: Futures mode guard
    if config.mode == Mode::Futures {
        eprintln!("[FATAL] Mode::Futures is not yet implemented.");
        eprintln!("        FuturesTradingStrategy contains todo!() calls.");
        eprintln!("        Set mode = \"spot\" in config.toml.");
        std::process::exit(1);
    }

    let backtest_csv = std::env::var("BACKTEST_CSV_PATH")
        .unwrap_or_else(|_| config.runtime.backtest_csv_path.clone());

    // ==========================================
    // WALK-FORWARD MODE
    // ==========================================
    if std::env::var("WF_TEST").is_ok() {
        if backtest_csv.is_empty() || !Path::new(&backtest_csv).exists() {
            eprintln!("[WF] Set BACKTEST_CSV_PATH to a valid CSV file.");
            return;
        }
        if let Err(e) = run_walk_forward_mode(&backtest_csv, &config, log_level) {
            eprintln!("[WF] Error: {}", e);
        }
        return;
    }

    // ==========================================
    // BATCH BACKTEST MODE
    // ==========================================
    if std::env::var("RUN_BATCH").is_ok() {
        let client = Client::new();
        let mut symbols_to_test: Vec<String> = config.runtime.backtest_symbols.clone();

        if std::env::var("AUTO_SCAN").is_ok() {
            if let Ok(top_pairs) = get_top_volume_pairs(&client, 10).await {
                symbols_to_test = top_pairs;
            }
        }

        let mut all_results = Vec::new();
        let total_pool = config.margin * symbols_to_test.len() as f64;
        let capital_by_symbol = config.allocate_capital(&symbols_to_test, total_pool);

        for sym in &symbols_to_test {
            let symbol_capital = capital_by_symbol.get(sym).copied().unwrap_or(config.margin);
            let interval = seconds_to_interval(config.runtime.candle_timeframe_seconds);
            let file_name = format!("{}-{}-auto.csv", sym, interval);

            if !Path::new(&file_name).exists() {
                let download_testnet = use_testnet && !config.runtime.backtest_use_real_data;
                if let Err(e) = download_history_to_csv(sym, interval, 8760, &client, download_testnet).await {
                    eprintln!("[ERROR] Download failed for {}: {}", sym, e);
                    continue;
                }
            }

            let (log_tx, _) = mpsc::unbounded_channel();
            let mut strategy = SpotStrategy::new(
                symbol_capital, sym, log_tx, config.crypto_exchange,
                config.strategy.clone(), log_level.clone(), None
            );

            match run_single_csv_backtest(&file_name, &mut strategy) {
                Ok(res) => all_results.push(res),
                Err(e) => eprintln!("[ERROR] Backtest for {} failed: {}", sym, e),
            }
        }

        print_results_table(&all_results, &config);
        return;
    }

    // ==========================================
    // SINGLE CSV BACKTEST MODE
    // ==========================================
    if !backtest_csv.is_empty() {
        let (log_tx, _) = mpsc::unbounded_channel();
        let mut strategy = SpotStrategy::new(
            config.margin, &symbol, log_tx, config.crypto_exchange,
            config.strategy.clone(), log_level.clone(), None
        );
        let client = Client::new();
        let interval = seconds_to_interval(config.runtime.candle_timeframe_seconds);
        
        if !Path::new(&backtest_csv).exists() {
            println!("[BOOT] File {} not found. Downloading 2000 {} candles of history...", backtest_csv, interval);
            let download_testnet = use_testnet && !config.runtime.backtest_use_real_data;
            if let Err(e) = download_history_to_csv(&symbol, interval, 2000, &client, download_testnet).await {
                eprintln!("[ERROR] Failed to download history: {}", e);
                return;
            }
        }
        
        let target_csv = if Path::new(&backtest_csv).exists() {
            backtest_csv.clone()
        } else {
            format!("{}-{}-auto.csv", symbol, interval)
        };

        if Path::new(&target_csv).exists() {
            if let Err(e) = run_single_csv_backtest(&target_csv, &mut strategy) {
                eprintln!("[ERROR] CSV backtest failed: {}", e);
            }
        } else {
            eprintln!("[ERROR] File {} not found!", target_csv);
        }
        return;
    }

    // ==========================================
    // LIVE MODE
    // ==========================================

    // ── P3: Dual-format log writer (CSV + JSONL) ──────────────────────────────
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<CandleLogEntry>();
    let live_log_path = config.runtime.live_log_path.clone();

    tokio::spawn(async move {
        while let Some(entry) = log_rx.recv().await {
            // CSV log
            if let Err(e) = write_to_csv_file(&entry, &live_log_path).await {
                eprintln!("[CSV-ERROR] {}", e);
            }
            // JSONL structured log (alongside CSV)
            if let Err(e) = write_to_jsonl_file(&entry, &live_log_path).await {
                eprintln!("[JSONL-ERROR] {}", e);
            }
        }
    });

    // ── P2: Kill switch + HTTP control server ─────────────────────────────────
    let kill_switch = KillSwitch::new();
    let ks_control = kill_switch.clone();
    tokio::spawn(async move {
        run_control_server(ks_control, 9101).await;
    });

    let client = Client::new();
    let mut best_live_symbols = vec![symbol.clone()];

    if std::env::var("AUTO_SCAN").is_ok() {
        if let Ok(top_pairs) = get_top_volume_pairs(&client, 5).await {
            for sym in top_pairs {
                if !best_live_symbols.contains(&sym) { best_live_symbols.push(sym); }
            }
        }
    }
    best_live_symbols.truncate(5);
    println!("[BOOT] Live symbols: {:?}", best_live_symbols);
    if use_testnet { println!("[BOOT] Running on BINANCE TESTNET"); }

    // ── Broker & StateManager Initialization ──────────────────────────────────
    let broker: Arc<dyn ExecutionBroker> = Arc::new(
        BinanceBroker::new(use_testnet).expect("Failed to initialize BinanceBroker")
    );
    
    // Check NTP Sync
    if let Some(binance_broker) = broker.as_any().downcast_ref::<BinanceBroker>() {
        if let Err(e) = binance_broker.check_time_sync().await {
            eprintln!("[ERROR] NTP Sync failed: {}. Exiting to prevent Binance rejects.", e);
            return;
        }
    }

    let state_manager = StateManager::new("atlas_state.db")
        .await
        .expect("Failed to init SQLite DB");

    // ── Warm-up: paginated history + real exchangeInfo filters ────────────────
    let live_capital_by_symbol = config.allocate_capital(&best_live_symbols, config.margin);
    let mut strategies: HashMap<String, Box<dyn TradingStrategy>> = HashMap::new();
    let warmup_period = config.strategy.warmup_period();

    for sym in &best_live_symbols {
        let capital_per_symbol = live_capital_by_symbol
            .get(sym)
            .copied()
            .unwrap_or(config.margin / best_live_symbols.len().max(1) as f64);

        let symbol_filters = fetch_symbol_filters(sym, &client, use_testnet).await;

        let mut reconciled_state = match reconcile_startup(sym, &broker, &state_manager, &config).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[ERROR] Reconciliation failed for {}: {}", sym, e);
                continue;
            }
        };

        // A position confirmed by reconciliation but with no resting stop
        // (fresh row from before this stop-order logic existed, or the
        // process died between market entry and placing the stop, or the
        // recorded stop turned out to be gone — see `reconcile_startup`)
        // must not enter the live loop unprotected: `sync_broker_state`
        // only places/replaces a stop on a *change* of position state, and
        // a recovered position that just sits there may never see one.
        if broker.name() != "PaperBroker" {
            let tick_size = symbol_filters.as_ref().map(|f| f.tick_size).unwrap_or(0.01);
            if let Some(ref mut state) = reconciled_state {
                if (state.is_holding || state.is_short) && state.stop_order_id.is_none() {
                    match place_protective_stop(&broker, sym, state, tick_size).await {
                        Ok(stop_id) => {
                            state.stop_order_id = Some(stop_id);
                            if let Err(e) = state_manager.save_position(state).await {
                                eprintln!("[ERROR] Failed to persist recovered stop for {}: {}", sym, e);
                            }
                        }
                        Err(e) => {
                            eprintln!("[EXECUTION-ERROR] {} recovered with an open position and NO exchange-side stop: {}", sym, e);
                            let _ = telegram::alert_critical(
                                "STOP_ORDER_FAILED",
                                &format!("{} recovered at boot with NO exchange-side protective stop: {}", sym, e),
                            ).await;
                        }
                    }
                }
            }
        }

        let mut strategy: Box<dyn TradingStrategy> = Box::new(SpotStrategy::new(
            capital_per_symbol, sym, log_tx.clone(),
            config.crypto_exchange, config.strategy.clone(), log_level.clone(), reconciled_state
        ));

        if let Some(filters) = symbol_filters {
            if let Some(any_mut) = strategy.as_any_mut() {
                if let Some(spot) = any_mut.downcast_mut::<SpotStrategy>() {
                    spot.wallet.filters = filters;
                }
            }
        }

        let interval = seconds_to_interval(config.runtime.candle_timeframe_seconds);
        match fetch_historical_candles_with_testnet(sym, interval, warmup_period + 1, &client, use_testnet).await {
            Ok(history) => {
                for candle in &history { strategy.on_candle_close(candle); }
                println!("[BOOT] {} warmed up ({} candles).", sym, history.len());
            }
            Err(e) => {
                eprintln!("[ERROR] Warmup failed for {}: {}", sym, e);
                telegram::alert_critical("WARMUP_FAILED", &format!("{}: {}", sym, e)).await;
                continue;
            }
        }

        strategies.insert(sym.clone(), strategy);
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }

    if strategies.is_empty() {
        eprintln!("[ERROR] No strategies ready. Stopping.");
        return;
    }

    // ── P2: Kline WebSocket streams (real OHLCV, heartbeat, testnet) ──────────
    let kline_interval = seconds_to_kline_interval(config.runtime.candle_timeframe_seconds);
    let (kline_tx, mut kline_rx) = mpsc::unbounded_channel::<(String, Candle)>();

    // Ticker stream for on_tick() calls (price updates between candles)
    let (tick_tx, mut tick_rx) = mpsc::unbounded_channel::<(String, f64)>();

    // OBI depth stream
    let (obi_tx, mut obi_rx) = mpsc::unbounded_channel::<(String, f64)>();

    // Spread stream
    let (spread_tx, mut spread_rx) = mpsc::unbounded_channel::<(String, f64)>();

    let use_obi = config.strategy.use_order_book_filter;
    let obi_levels = config.strategy.obi_depth_levels;

    for sym in strategies.keys() {
        let sym_clone = sym.clone();
        let interval = kline_interval;

        // Kline stream → delivers complete candles with real volume
        {
            let (local_kline_tx, mut local_kline_rx) = mpsc::unbounded_channel::<Candle>();
            let kline_fwd_tx = kline_tx.clone();
            let sym2 = sym_clone.clone();
            tokio::spawn(async move {
                run_binance_kline_client(&sym2, interval, local_kline_tx, use_testnet).await;
            });
            let sym3 = sym_clone.clone();
            tokio::spawn(async move {
                while let Some(c) = local_kline_rx.recv().await {
                    let _ = kline_fwd_tx.send((sym3.clone(), c));
                }
            });
        }

        // Ticker stream → for on_tick() between candle closes
        {
            let (local_tick_tx, mut local_tick_rx) = mpsc::unbounded_channel::<f64>();
            let tick_fwd_tx = tick_tx.clone();
            let sym2 = sym_clone.clone();
            tokio::spawn(async move {
                run_binance_websocket_client(&sym2, local_tick_tx, use_testnet).await;
            });
            let sym3 = sym_clone.clone();
            tokio::spawn(async move {
                while let Some(p) = local_tick_rx.recv().await {
                    let _ = tick_fwd_tx.send((sym3.clone(), p));
                }
            });
        }

        // OBI depth stream
        if use_obi {
            let (local_obi_tx, mut local_obi_rx) = mpsc::unbounded_channel::<f64>();
            let obi_fwd_tx = obi_tx.clone();
            let sym2 = sym_clone.clone();
            tokio::spawn(async move {
                run_binance_depth_client(&sym2, obi_levels, local_obi_tx, use_testnet).await;
            });
            let sym3 = sym_clone.clone();
            tokio::spawn(async move {
                while let Some(obi) = local_obi_rx.recv().await {
                    let _ = obi_fwd_tx.send((sym3.clone(), obi));
                }
            });
        }
        
        // Book Ticker (Spread) stream
        {
            let (local_spread_tx, mut local_spread_rx) = mpsc::unbounded_channel::<(String, f64)>();
            let spread_fwd_tx = spread_tx.clone();
            let sym2 = sym_clone.clone();
            tokio::spawn(async move {
                run_book_ticker_client(&sym2, local_spread_tx, use_testnet).await;
            });
            tokio::spawn(async move {
                while let Some(spread) = local_spread_rx.recv().await {
                    let _ = spread_fwd_tx.send(spread);
                }
            });
        }
    }

    // Fear & Greed index stream (polls every hour)
    let (fg_tx, mut fg_rx) = mpsc::unbounded_channel::<(f64, String)>();
    let fg_client = client.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((val, class)) = fetch_fear_greed_index(&fg_client).await {
                let _ = fg_tx.send((val, class));
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await; // 1 hour
        }
    });

    println!(
        "[BOOT] Live mode: {} symbols, {} kline stream (heartbeat ON).",
        strategies.len(), kline_interval
    );

    // ── Prometheus metrics ────────────────────────────────────────────────────
    let bot_metrics = Arc::new(BotMetrics::new());
    let metrics_port = config.runtime.metrics_port;
    if metrics_port > 0 {
        let mc = Arc::clone(&bot_metrics);
        tokio::spawn(async move { run_metrics_server(mc, metrics_port).await; });
    }

    let mut current_prices: std::collections::HashMap<String, f64> = std::collections::HashMap::new();

    let mut risk_manager = RiskManager::new(&config);

    // ── Dashboard Server ──────────────────────────────────────────────────────
    let (dash_tx, _) = tokio::sync::broadcast::channel::<DashboardState>(100);
    let dash_tx_clone = dash_tx.clone();
    tokio::spawn(async move {
        run_dashboard_server(8080, dash_tx_clone).await;
    });

    // ── P2: Main loop — tokio::select! on kline/tick/obi + kill switch ────────
    let ks = kill_switch.clone();

    loop {
        // Check kill switch before processing
        if ks.is_halted() {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }

        tokio::select! {
            // ── Exchange kline close: complete OHLCV candle (P2 fix: real volume)
            Some((sym, candle)) = kline_rx.recv() => {
                current_prices.insert(sym.clone(), candle.close);
                if let Some(strategy) = strategies.get_mut(&sym) {
                    let old_state = strategy.get_position_state();
                    
                    strategy.on_candle_close(&candle);
                    
                    let new_state = strategy.get_position_state();

                    let tick_size = strategy.as_any().downcast_ref::<SpotStrategy>()
                        .map(|s| s.wallet.filters.tick_size)
                        .unwrap_or(0.01);
                    sync_broker_state(&sym, &old_state, &new_state, &broker, &state_manager, tick_size).await;

                    if let Some(spot) = strategy.as_any().downcast_ref::<SpotStrategy>() {
                        if spot.reporter.drawdown_stop_active && spot.position.is_holding_asset {
                            let reason = format!(
                                "{} drawdown halt triggered. Equity: ${:.2}",
                                sym, spot.reporter.current_equity
                            );
                            ks.halt(&reason).await;
                            telegram::alert_critical("DRAWDOWN_HALT", &reason).await;
                        }
                    }
                }

                let mut snapshots: Vec<SymbolSnapshot> = Vec::new();
                for (s, strategy) in &strategies {
                    if let Some(spot) = strategy.as_any().downcast_ref::<SpotStrategy>() {
                        if s == &sym {
                            snapshots.push(spot.to_metrics_snapshot(candle.close));
                        }
                    }
                }
                
                if !snapshots.is_empty() {
                    update_metrics(&bot_metrics, &snapshots, config.margin);
                }

                let mut total_equity = 0.0f64;
                for strategy in strategies.values() {
                    total_equity += strategy.final_equity(candle.close);
                }

                broadcast_dashboard(&strategies, &current_prices, config.margin, &risk_manager, &ks, &dash_tx);

                println!("[PORTFOLIO] Total: ${:.2}", total_equity);

                if let Err(risk_msg) = risk_manager.evaluate_portfolio(&strategies, config.margin, total_equity) {
                    ks.halt(&risk_msg).await;
                    telegram::alert_critical("PORTFOLIO_RISK_HALT", &risk_msg).await;
                }

                // Daily loss limit alert
                if let Some(strategy) = strategies.get(&sym) {
                    if let Some(spot) = strategy.as_any().downcast_ref::<SpotStrategy>() {
                        if spot.daily_pnl < -spot.daily_loss_limit_usdt {
                            let msg = format!(
                                "{} daily loss limit hit: ${:.2} (limit: ${:.2})",
                                sym, spot.daily_pnl.abs(), spot.daily_loss_limit_usdt
                            );
                            telegram::alert_critical("DAILY_LOSS_LIMIT", &msg).await;
                            ks.halt(&msg).await;
                        }
                    }
                }
            }

            // ── Price tick: for on_tick() trailing stop updates
            Some((sym, price)) = tick_rx.recv() => {
                current_prices.insert(sym.clone(), price);
                if let Some(strategy) = strategies.get_mut(&sym) {
                    let old_state = strategy.get_position_state();
                    strategy.on_tick(price);
                    let new_state = strategy.get_position_state();

                    let tick_size = strategy.as_any().downcast_ref::<SpotStrategy>()
                        .map(|s| s.wallet.filters.tick_size)
                        .unwrap_or(0.01);
                    sync_broker_state(&sym, &old_state, &new_state, &broker, &state_manager, tick_size).await;
                }
                broadcast_dashboard(&strategies, &current_prices, config.margin, &risk_manager, &ks, &dash_tx);
            }

            // ── OBI update
            Some((sym, obi)) = obi_rx.recv() => {
                if let Some(strategy) = strategies.get_mut(&sym) {
                    strategy.set_order_book_imbalance(obi);
                }
            }

            // ── Spread update
            Some((sym, spread_pct)) = spread_rx.recv() => {
                if let Some(strategy) = strategies.get_mut(&sym) {
                    strategy.set_spread_pct(spread_pct);
                }
            }

            // ── Fear & Greed update
            Some((val, class)) = fg_rx.recv() => {
                for strategy in strategies.values_mut() {
                    strategy.set_fear_greed(val, class.clone());
                }
            }
        }
    }
}

fn broadcast_dashboard(
    strategies: &std::collections::HashMap<String, Box<dyn TradingStrategy>>,
    current_prices: &std::collections::HashMap<String, f64>,
    initial_margin: f64,
    risk_manager: &RiskManager,
    ks: &KillSwitch,
    dash_tx: &tokio::sync::broadcast::Sender<DashboardState>,
) {
    let mut total_equity = 0.0f64;
    let mut total_daily_pnl = 0.0f64;
    let mut total_trades_all = 0u32;
    let mut total_wins_all = 0u32;
    let mut fg_index = 50.0;
    let mut open_positions = Vec::new();

    for (s, strategy) in strategies {
        let current_price = current_prices.get(s).copied().unwrap_or(0.0);
        let eq = strategy.final_equity(current_price);
        total_equity += eq;
        
        if let Some(spot) = strategy.as_any().downcast_ref::<SpotStrategy>() {
            total_daily_pnl += spot.daily_pnl;
            total_trades_all += spot.reporter.trade_history.len() as u32;
            total_wins_all += spot.reporter.trade_history.iter().filter(|t| t.pnl_pct > 0.0).count() as u32;
            fg_index = spot.entry_filters.fear_greed_index; // Take from any strategy

            if spot.position.is_holding_asset {
                let qty = spot.wallet.crypto_balance;
                let entry = spot.position.buy_price;
                let unrealized_pnl = (current_price - entry) * qty;
                let unrealized_pnl_pct = if entry > 0.0 { (current_price - entry) / entry * 100.0 } else { 0.0 };
                open_positions.push(PositionInfo {
                    symbol: s.clone(),
                    qty,
                    entry_price: entry,
                    current_price,
                    unrealized_pnl,
                    unrealized_pnl_pct,
                    side: "Long".to_string(), // For Spot, it's always Long
                });
            }
        }
    }

    let pnl_pct = (total_equity - initial_margin) / initial_margin * 100.0;
    let pnl_usdt = total_equity - initial_margin;
    let win_rate = if total_trades_all > 0 { (total_wins_all as f64 / total_trades_all as f64) * 100.0 } else { 0.0 };
    
    let drawdown = if risk_manager.peak_portfolio_equity > 0.0 {
        (risk_manager.peak_portfolio_equity - total_equity) / risk_manager.peak_portfolio_equity * 100.0
    } else { 0.0 };

    let dash_state = DashboardState {
        total_equity,
        initial_margin,
        pnl_pct,
        pnl_usdt,
        daily_pnl_usdt: total_daily_pnl,
        drawdown_pct: drawdown,
        total_trades: total_trades_all,
        win_rate,
        fear_greed_index: fg_index,
        is_kill_switch_active: ks.is_halted(),
        open_positions,
    };
    let _ = dash_tx.send(dash_state);
}
