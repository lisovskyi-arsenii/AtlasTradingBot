//! Criterion benchmarks for the hot path in `SpotStrategy`.
//!
//! Run with:
//! ```bash
//! cargo bench
//! # Or for a specific benchmark:
//! cargo bench -- on_candle_close
//! ```
//! HTML reports are generated in `target/criterion/`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};

// Re-use the bot's modules
use RustBot::models::candle::Candle;
use RustBot::models::data::CryptoExchange;
use RustBot::models::log_level::LogLevel;
use RustBot::models::strategy_config::StrategyConfig;
use RustBot::strategy::spot_strategy::SpotStrategy;
use RustBot::strategy::TradingStrategy;

/// Generate a synthetic candle sequence for benchmarking.
fn synthetic_candles(n: usize) -> Vec<Candle> {
    let mut price = 50_000.0f64;
    let mut candles = Vec::with_capacity(n);

    // Simple random walk. `high`/`low` are derived from `open`/`close` (not
    // independently offset from `price`) so low <= open,close <= high always
    // holds — `ta::DataItem::builder().build()` rejects any bar that
    // violates that invariant, which an earlier version of this generator
    // could produce (open computed from `change` while high/low were offset
    // from `price` by unrelated random terms).
    let mut seed = 42u64;
    for _ in 0..n {
        // LCG pseudo-random
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let change = ((seed >> 33) as f64 / u32::MAX as f64 - 0.5) * 100.0;
        let open = price;
        price = (price + change).max(1.0);
        let close = price;

        let wick_up = ((seed >> 16) as f64 / u32::MAX as f64) * 50.0;
        let wick_down = ((seed >> 8) as f64 / u32::MAX as f64) * 50.0;
        let high = open.max(close) + wick_up;
        let low = (open.min(close) - wick_down).max(0.01);

        candles.push(Candle {
            open,
            high,
            low,
            close,
            volume: ((seed & 0xFFFF) as f64) + 100.0,
        });
    }
    candles
}

fn build_strategy() -> SpotStrategy {
    let (log_tx, _log_rx) = tokio::sync::mpsc::unbounded_channel();
    let config = StrategyConfig::default();
    SpotStrategy::new(
        10_000.0,
        "BTCUSDT",
        log_tx,
        CryptoExchange::Binance,
        config,
        LogLevel::Quiet,
        None,
    )
}

/// Benchmark: 10 000 candle replay (the primary hot path).
fn bench_on_candle_close(c: &mut Criterion) {
    let candles = synthetic_candles(10_000);

    c.bench_function("on_candle_close_10k", |b| {
        b.iter(|| {
            let mut strategy = build_strategy();
            for candle in &candles {
                strategy.on_candle_close(black_box(candle));
            }
            black_box(strategy.final_equity(50_000.0))
        })
    });
}

/// Benchmark: single candle (measures per-candle overhead).
fn bench_single_candle(c: &mut Criterion) {
    let candles = synthetic_candles(500); // warm up

    c.bench_function("on_candle_close_single", |b| {
        b.iter_batched(
            || {
                let mut s = build_strategy();
                // Warm up indicators
                for candle in &candles { s.on_candle_close(candle); }
                s
            },
            |mut strategy| {
                let candle = Candle {
                    open: 50_000.0, high: 51_000.0, low: 49_500.0,
                    close: 50_200.0, volume: 250.0,
                };
                strategy.on_candle_close(black_box(&candle));
                black_box(strategy.final_equity(50_200.0))
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

/// Benchmark: walk-forward harness (measures allocations per window).
fn bench_walk_forward_100_windows(c: &mut Criterion) {
    use RustBot::utility::walk_forward::run_walk_forward;

    let candles = synthetic_candles(60_000); // ~60k candles ≈ 6.8 years of 1h data
    let config = StrategyConfig::default();

    c.bench_function("walk_forward_100_windows", |b| {
        b.iter(|| {
            run_walk_forward(
                black_box(&candles),
                &config,
                10_000.0,
                "BTCUSDT",
                500,  // train
                100,  // test
                100,  // step
            )
        })
    });
}

criterion_group!(
    benches,
    bench_on_candle_close,
    bench_single_candle,
    bench_walk_forward_100_windows,
);
criterion_main!(benches);
