use crate::models::candle::Candle;
use ta::indicators::AverageTrueRange as Atr;
use ta::indicators::ExponentialMovingAverage as Ema;
use ta::indicators::RelativeStrengthIndex as Rsi;
use ta::indicators::SimpleMovingAverage as Sma;
use ta::{DataItem, Next};

pub mod spot_strategy;
pub mod futures_strategy;

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

    fn on_tick(&mut self, current_price: f64);
    fn on_candle_close(&mut self, candle: &Candle);

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
            println!(
                "[Warming up indicators… {}/{}]",
                self.loop_count(),
                warmup
            );
            return None;
        }

        Some((slow_value, fast_value, trend_value, rsi_value, atr_value, macro_ema_val, vol_sma_val))
    }
}
