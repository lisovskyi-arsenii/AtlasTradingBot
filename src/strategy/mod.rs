use crate::models::candle::Candle;
use crate::models::data::MACRO_PERIOD;
use ta::indicators::AverageTrueRange as Atr;
use ta::indicators::ExponentialMovingAverage as Ema;
use ta::indicators::RelativeStrengthIndex as Rsi;
use ta::indicators::SimpleMovingAverage as Sma;
use ta::{DataItem, Next};

pub mod spot_strategy;
pub mod futures_strategy;

pub const WARMUP_PERIOD: usize = MACRO_PERIOD;

pub const ATR_MULTIPLIER: f64 = 3.0;
pub const RSI_BULLISH_MIN: f64 = 50.0;
pub const RSI_BULLISH_MAX: f64 = 70.0;
pub const RSI_BEARISH_MIN: f64 = 30.0;
pub const RSI_BEARISH_MAX: f64 = 50.0;
pub const VOLUME_CONFIRMATION_MULTIPLIER: f64 = 1.0;

pub const RSI_DIP_LEVEL: f64 = 32.0;
pub const RSI_EXIT_LEVEL: f64 = 72.0;
pub const MIN_PROFIT_FOR_RSI_EXIT_PCT: f64 = 0.004;
pub const BREAKEVEN_TRIGGER_R: f64 = 1.0;
pub const BREAKEVEN_LOCK_PCT: f64 = 0.003;

const DEFAULT_RISK_PER_TRADE_PCT: f64 = 0.0035;
const MAX_STRATEGY_DRAWDOWN_PCT: f64 = 12.0;
const TAKE_PROFIT_R_MULTIPLIER: f64 = 4.5;
const TAKE_PROFIT_COOLDOWN_BARS: usize = 3;
const LOSS_COOLDOWN_BARS: usize = 8;

pub const COOLDOWN_BARS: usize = 2;
pub const MIN_BARS_IN_POSITION: usize = 5;

pub trait TradingStrategy {
    fn loop_count(&mut self) -> &mut usize;
    fn slow_sma(&mut self) -> &mut Sma;
    fn fast_sma(&mut self) -> &mut Sma;
    fn trend_ema(&mut self) -> &mut Ema;
    fn rsi(&mut self) -> &mut Rsi;
    fn atr(&mut self) -> &mut Atr;
    fn macro_ema(&mut self) -> &mut Ema;
    fn vol_sma(&mut self) -> &mut Sma;

    fn on_tick(&mut self, current_price: f64);
    fn on_candle_close(&mut self, candle: &Candle);

    fn update_indicators(&mut self, candle: &Candle) -> Option<(f64, f64, f64, f64, f64, f64, f64)> {
        *self.loop_count() += 1;

        let slow_value: f64 = self.slow_sma().next(candle.close);
        let fast_value: f64 = self.fast_sma().next(candle.close);
        let trend_value: f64 = self.trend_ema().next(candle.close);
        let rsi_value: f64 = self.rsi().next(candle.close);
        let macro_ema_value: f64 = self.macro_ema().next(candle.close);
        let vol_sma_value: f64 = self.vol_sma().next(candle.volume);

        let item: DataItem = DataItem::builder()
            .open(candle.open)
            .high(candle.high)
            .low(candle.low)
            .close(candle.close)
            .volume(candle.volume)
            .build()
            .expect("Failed to build DataItem");

        let atr_value: f64 = self.atr().next(&item);

        if *self.loop_count() < WARMUP_PERIOD {
            println!(
                "Warming up indicators... Step {}/{}",
                *self.loop_count(),
                WARMUP_PERIOD
            );
            return None;
        }

        Some((slow_value, fast_value, trend_value, rsi_value, atr_value, macro_ema_value, vol_sma_value))
    }
}