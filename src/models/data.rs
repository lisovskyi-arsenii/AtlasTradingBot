use serde::Serialize;
use std::time::Instant;


#[derive(Debug, Copy, Clone, PartialEq)]
pub enum CryptoExchange {
    Binance,
    Bybit,
    Whitebit,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PositionType {
    Long,
    Short,
    None
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct EquityPoint {
    pub bar_index: usize,
    pub equity: f64,
    pub phase: Phase,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Phase {
    BarClose,
    PostBuy,
    PostSell
}

#[derive(Debug, Clone)]
pub enum Mode {
    Spot,
    Futures,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Action {
    NoSignal,
    Buy,
    Sell,
    StopHit,
    ShortSell,
    CloseShort,
}

#[derive(Debug, Copy, Clone)]
pub enum ExitReason {
    InitialStop,
    /// Hard percentage panic stop; bypasses min_bars_in_position and exits at the breach price.
    PanicStop,
    TrailingStop,
    TakeProfit,
    WeakMomentumExit,
    BearishCross,
    /// Short: price rose to stop-loss
    ShortStop,
    /// Short: price fell to take-profit
    ShortTakeProfit,
    EndOfData,
}

impl ExitReason {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            ExitReason::InitialStop       => "INITIAL_STOP",
            ExitReason::PanicStop         => "PANIC_STOP",
            ExitReason::TrailingStop      => "TRAILING_STOP",
            ExitReason::TakeProfit        => "TAKE_PROFIT",
            ExitReason::WeakMomentumExit  => "WEAK_MOMENTUM_EXIT",
            ExitReason::BearishCross      => "BEARISH_CROSS",
            ExitReason::ShortStop         => "SHORT_STOP",
            ExitReason::ShortTakeProfit   => "SHORT_TAKE_PROFIT",
            ExitReason::EndOfData         => "END_OF_DATA",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SignalState {
    pub bullish_cross: bool,
    pub bearish_cross: bool,
    pub bullish_trend: bool,
    pub bullish_rsi: bool,
    pub strong_macro_trend: bool,
    pub strong_volume: bool,
    pub fast_slow_diff: f64,
    pub price_trend_diff: f64,
    pub no_signal_reason: String,
    // Short-side fields
    pub short_entry: bool,
    pub bearish_trend: bool,
    pub short_no_signal_reason: String,
}

#[derive(Debug, Clone)]
pub struct BacktestResult {
    pub csv_file: String,
    pub symbol: String,
    pub total_trades: usize,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub total_pnl_pct: f64,
    pub total_pnl_usdt: f64,
    pub avg_pnl_usdt: f64,
    pub max_drawdown_pct: f64,
    pub sharpe_ratio: f64,
    pub recovery_factor: f64,
    pub max_consecutive_losses: usize,
    pub final_equity: f64,
    pub initial_capital: f64,
}

#[derive(Debug, Clone)]
pub struct CandleBuilder {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub start_time: Instant,
}

impl CandleBuilder {
    pub fn new(price: f64) -> Self {
        Self { open: price, high: price, low: price, close: price, start_time: Instant::now() }
    }
    pub fn update(&mut self, price: f64) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
    }
}
