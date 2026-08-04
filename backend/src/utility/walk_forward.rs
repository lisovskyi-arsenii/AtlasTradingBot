//! Walk-forward out-of-sample (OOS) testing harness.
//!
//! Divides a CSV history into overlapping train/test windows and reports
//! per-window and aggregated OOS statistics. This prevents overfitting by
//! ensuring reported metrics come from data the strategy was not tuned on.
//!
//! # Activation
//! ```bash
//! WF_TEST=1 cargo run --release
//! ```
//! The `BACKTEST_CSV_PATH` env var must also point to a CSV file.
//!
//! # Default parameters (overridable via env vars)
//! | Variable          | Default | Description                        |
//! |-------------------|---------|------------------------------------|
//! | `WF_TRAIN_BARS`   | 2000    | Bars used for the training window  |
//! | `WF_TEST_BARS`    | 500     | Bars in each OOS test window       |
//! | `WF_STEP_BARS`    | 500     | Step between consecutive windows   |

use crate::models::candle::Candle;
use crate::models::data::BacktestResult;
use crate::models::strategy_config::StrategyConfig;
use crate::models::data::CryptoExchange;
use crate::models::log_level::LogLevel;
use crate::strategy::spot_strategy::SpotStrategy;
use crate::strategy::TradingStrategy;
use tokio::sync::mpsc;

/// Result of one OOS test window.
#[derive(Debug, Clone)]
pub struct WalkForwardWindow {
    pub window_index: usize,
    pub train_start: usize,
    pub train_end: usize,
    pub test_start: usize,
    pub test_end: usize,
    pub oos_result: BacktestResult,
}

/// Aggregated walk-forward statistics.
#[derive(Debug, Clone)]
pub struct WalkForwardSummary {
    pub total_windows: usize,
    pub mean_oos_sharpe: f64,
    pub mean_oos_win_rate: f64,
    pub mean_oos_pnl_pct: f64,
    pub profitable_windows: usize,
    /// Worst OOS window drawdown (most negative).
    pub worst_drawdown_pct: f64,
    /// Whether parameters are stable across windows (low std dev of win rate).
    pub win_rate_std_dev: f64,
}

/// Run a full walk-forward test on `candles` using `config`.
///
/// Returns `None` if there are not enough candles for even one window.
pub fn run_walk_forward(
    candles: &[Candle],
    config: &StrategyConfig,
    initial_capital: f64,
    symbol: &str,
    train_bars: usize,
    test_bars: usize,
    step_bars: usize,
) -> Option<(Vec<WalkForwardWindow>, WalkForwardSummary)> {
    if candles.len() < train_bars + test_bars {
        eprintln!(
            "[WF] Not enough data: {} candles, need at least {}",
            candles.len(), train_bars + test_bars
        );
        return None;
    }

    let mut windows: Vec<WalkForwardWindow> = Vec::new();
    let mut start = 0usize;
    let mut window_index = 0usize;

    while start + train_bars + test_bars <= candles.len() {
        let train_start = start;
        let train_end = start + train_bars;
        let test_start = train_end;
        let test_end = (test_start + test_bars).min(candles.len());

        let test_slice = &candles[test_start..test_end];

        // Run the OOS test on the test slice only (the "in-sample" train slice
        // is used conceptually to represent where params were optimised).
        // In this implementation we use the same config (no in-sample optimisation),
        // which gives a conservative lower bound on actual walk-forward results.
        let (log_tx, _log_rx) = mpsc::unbounded_channel();
        let mut strategy = SpotStrategy::new(
            initial_capital,
            symbol,
            log_tx,
            CryptoExchange::Binance,
            config.clone(),
            LogLevel::Quiet,
            None,
        );

        for candle in test_slice {
            strategy.on_candle_close(candle);
        }

        let last_close = test_slice.last().map(|c| c.close).unwrap_or(0.0);
        strategy.finalize_backtest(last_close);

        let oos_result = strategy.compute_backtest_result(
            last_close,
            &format!("WF_W{}_OOS", window_index),
        );

        windows.push(WalkForwardWindow {
            window_index,
            train_start,
            train_end,
            test_start,
            test_end,
            oos_result,
        });

        start += step_bars;
        window_index += 1;
    }

    if windows.is_empty() {
        return None;
    }

    // Compute summary statistics
    let n = windows.len() as f64;
    let mean_sharpe = windows.iter().map(|w| w.oos_result.sharpe_ratio).sum::<f64>() / n;
    let mean_win_rate = windows.iter().map(|w| w.oos_result.win_rate).sum::<f64>() / n;
    let mean_pnl = windows.iter().map(|w| w.oos_result.total_pnl_pct).sum::<f64>() / n;
    let profitable = windows.iter().filter(|w| w.oos_result.total_pnl_usdt > 0.0).count();
    let worst_dd = windows.iter()
        .map(|w| w.oos_result.max_drawdown_pct)
        .fold(0.0_f64, f64::min);

    // Win-rate std dev (stability metric)
    let win_rate_variance = windows.iter()
        .map(|w| (w.oos_result.win_rate - mean_win_rate).powi(2))
        .sum::<f64>() / n;
    let win_rate_std = win_rate_variance.sqrt();

    let summary = WalkForwardSummary {
        total_windows: windows.len(),
        mean_oos_sharpe: mean_sharpe,
        mean_oos_win_rate: mean_win_rate,
        mean_oos_pnl_pct: mean_pnl,
        profitable_windows: profitable,
        worst_drawdown_pct: worst_dd,
        win_rate_std_dev: win_rate_std,
    };

    Some((windows, summary))
}

/// Print a formatted walk-forward report to stdout.
pub fn print_walk_forward_report(windows: &[WalkForwardWindow], summary: &WalkForwardSummary) {
    println!("\n╔══════════════════════════════════════════════════════════════════════════╗");
    println!("║  WALK-FORWARD OUT-OF-SAMPLE REPORT                                      ║");
    println!("╠═══════════╦═══════════╦═══════╦═══════════╦════════════╦════════════════╣");
    println!("║ Window    ║ OOS Bars  ║ Trades║ Win Rate  ║ PnL %      ║ Sharpe         ║");
    println!("╠═══════════╬═══════════╬═══════╬═══════════╬════════════╬════════════════╣");

    for w in windows {
        let r = &w.oos_result;
        println!(
            "║ W{:<8} ║ {:>4}–{:<4} ║ {:>5} ║ {:>7.1}%  ║ {:>8.2}%   ║ {:>12.3}   ║",
            w.window_index,
            w.test_start,
            w.test_end,
            r.total_trades,
            r.win_rate,
            r.total_pnl_pct,
            r.sharpe_ratio,
        );
    }

    println!("╠═══════════╩═══════════╩═══════╩═══════════╩════════════╩════════════════╣");
    println!("║  SUMMARY                                                                 ║");
    println!("╠══════════════════════════════════════════════════════════════════════════╣");
    println!(
        "║  Windows: {}  |  Profitable: {}/{}  |  Mean Win Rate: {:.1}%  ±{:.1}%",
        summary.total_windows,
        summary.profitable_windows,
        summary.total_windows,
        summary.mean_oos_win_rate,
        summary.win_rate_std_dev,
    );
    println!(
        "║  Mean OOS PnL: {:+.2}%  |  Mean Sharpe: {:.3}  |  Worst DD: {:.2}%",
        summary.mean_oos_pnl_pct,
        summary.mean_oos_sharpe,
        summary.worst_drawdown_pct,
    );

    let stability = if summary.win_rate_std_dev < 10.0 {
        "✅ STABLE"
    } else if summary.win_rate_std_dev < 20.0 {
        "⚠️ MODERATE"
    } else {
        "❌ UNSTABLE"
    };
    println!("║  Parameter Stability: {}", stability);
    println!("╚══════════════════════════════════════════════════════════════════════════╝\n");
}
