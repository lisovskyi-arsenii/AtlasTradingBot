use crate::models::candle::Candle;
use ta::indicators::AverageTrueRange as Atr;
use ta::indicators::ExponentialMovingAverage as Ema;
use ta::indicators::RelativeStrengthIndex as Rsi;
use ta::indicators::SimpleMovingAverage as Sma;
use ta::{DataItem, Next};

pub mod spot_strategy;
pub mod futures_strategy;
pub mod order_book;

/// Core interface shared by all strategy implementations.
///
/// Trait methods are intentionally minimal: each concrete strategy (Spot,
/// Futures, …) implements `on_tick` and `on_candle_close` in its own `impl`
/// block and calls `update_indicators` here for the shared TA bookkeeping.
pub trait TradingStrategy {
    fn loop_count(&mut self) -> &mut usize;
    fn slow_sma(&mut self)   -> &mut Sma;
    fn fast_sma(&mut self)   -> &mut Sma;
    fn trend_ema(&mut self)  -> &mut Ema;
    fn rsi(&mut self)        -> &mut Rsi;
    fn atr(&mut self)        -> &mut Atr;
    fn macro_ema(&mut self)  -> &mut Ema;
    fn vol_sma(&mut self)    -> &mut Sma;
    fn warmup_period(&self)  -> usize;
    fn final_equity(&self, current_price: f64) -> f64;
    fn total_trades(&self) -> usize;

    /// Latest indicator values for metrics reporting.
    /// Returns (rsi, z_score_placeholder, atr, last_atr_pct).
    /// z_score is not stored on the trait; strategies can override.
    fn latest_indicators(&self) -> (f64, f64, f64, f64) { (0.0, 0.0, 0.0, 0.0) }
    /// Current equity, peak equity, current drawdown %.
    fn equity_state(&self, current_price: f64) -> (f64, f64, f64) {
        let eq = self.final_equity(current_price);
        (eq, eq, 0.0)
    }
    /// Position side as integer: 0=flat, 1=long, -1=short.
    fn position_side_int(&self) -> i64 { 0 }
    /// Wallet USDT balance.
    fn wallet_usdt_balance(&self) -> f64 { 0.0 }
    /// Snapshot of current position state for persistence
    fn get_position_state(&self) -> Option<crate::execution::state::PositionState> { None }

    fn on_tick(&mut self, current_price: f64);
    fn on_candle_close(&mut self, candle: &Candle);

    /// Feed the latest live order-book imbalance (range -1..1) into the strategy.
    /// Default is a no-op so strategies that ignore microstructure (and the
    /// backtest path, which has no order book) need not implement it.
    fn set_order_book_imbalance(&mut self, _obi: f64) {}

    /// Feed the latest BTC price into the strategy for circuit-breaker filtering.
    /// Default is a no-op so strategies that don't use the BTC filter need not implement it.
    fn update_btc_price(&mut self, _btc_price: f64) {}

    /// Feed the latest bid-ask spread percentage.
    fn set_spread_pct(&mut self, _spread_pct: f64) {}

    /// Feed the latest Fear & Greed index.
    fn set_fear_greed(&mut self, _value: f64, _classification: String) {}

    /// Return a reference to self as `Any` for downcasting.
    /// This enables accessing concrete strategy methods from trait objects.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Return a mutable reference to self as `Any` for downcasting.
    /// Needed to inject runtime configuration (e.g. exchangeInfo filters) into
    /// concrete strategy types through the trait object.
    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> { None }

    /// Feed one closed candle through all indicators.
    ///
    /// Returns `None` during the warm-up period so callers can early-return
    /// without acting on unreliable indicator values.
    ///
    /// Return order:
    ///   `(slow_sma, fast_sma, trend_ema, rsi, atr, macro_ema, vol_sma)`
    fn update_indicators(
        &mut self,
        candle: Candle,
    ) -> Option<(f64, f64, f64, f64, f64, f64, f64)> {
        *self.loop_count() += 1;

        let slow_value    = f64::from(self.slow_sma().next(candle.close));
        let fast_value    = f64::from(self.fast_sma().next(candle.close));
        let trend_value   = f64::from(self.trend_ema().next(candle.close));
        let rsi_value     = f64::from(self.rsi().next(candle.close));
        let macro_ema_val = f64::from(self.macro_ema().next(candle.close));
        let vol_sma_val   = f64::from(self.vol_sma().next(candle.volume));

        let item = DataItem::builder()
            .open(candle.open)
            .high(candle.high)
            .low(candle.low)
            .close(candle.close)
            .volume(candle.volume)
            .build()
            .expect("Failed to build DataItem");
        let atr_value = f64::from(self.atr().next(&item));

        let warmup = self.warmup_period();
        if *self.loop_count() < warmup {
            // Warm-up is silent: this runs once per candle (thousands of times in
            // batch backtests) so it must never touch stdout on the hot path.
            return None;
        }

        Some((slow_value, fast_value, trend_value, rsi_value, atr_value, macro_ema_val, vol_sma_val))
    }
}
