use crate::models::data::Action;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct CandleLogEntry {
    pub timestamp: String,
    pub symbol: String,

    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,

    pub fast_sma: f64,
    pub slow_sma: f64,
    pub trend_ema: f64,
    pub rsi: f64,
    pub atr: f64,

    pub bullish_cross: bool,
    pub bearish_cross: bool,
    pub bullish_trend: bool,
    pub bullish_rsi: bool,

    pub fast_slow_diff: f64,
    pub price_trend_diff: f64,

    pub is_holding: bool,
    pub cooldown_bars_remaining: usize,

    pub action: Action,
    pub no_signal_reason: String,
}
