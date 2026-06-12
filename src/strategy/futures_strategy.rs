use crate::futures::futures_wallet::FuturesWallet;
use crate::models::candle::Candle;
use crate::models::data::{ATR_PERIOD, FAST_PERIOD, MACRO_PERIOD, RSI_PERIOD, SLOW_PERIOD, TREND_PERIOD, VOL_SMA_PERIOD};
use crate::strategy::{
    TradingStrategy,
    COOLDOWN_BARS,
};
use ta::indicators::AverageTrueRange as Atr;
use ta::indicators::ExponentialMovingAverage as Ema;
use ta::indicators::RelativeStrengthIndex as Rsi;
use ta::indicators::SimpleMovingAverage as Sma;

pub struct FuturesTradingStrategy {
    pub slow_sma: Sma,
    pub fast_sma: Sma,
    pub trend_ema: Ema,
    pub rsi: Rsi,
    pub atr: Atr,
    pub macro_ema: Ema,
    pub vol_sma: Sma,

    pub previous_slow_sma: Option<f64>,
    pub previous_fast_sma: Option<f64>,

    pub highest_price: f64,
    pub lowest_price: f64,

    pub bars_in_position: usize,
    pub cooldown_bars_remaining: usize,

    pub loop_count: usize,
    pub wallet: FuturesWallet,
}

impl FuturesTradingStrategy {
    pub fn new(start_margin: f64, leverage: f64) -> Self {
        Self {
            slow_sma: Sma::new(SLOW_PERIOD)
                .expect("Failed to create slow SMA"),
            fast_sma: Sma::new(FAST_PERIOD)
                .expect("Failed to create fast SMA"),
            trend_ema: Ema::new(TREND_PERIOD)
                .expect("Failed to create trend EMA"),
            rsi: Rsi::new(RSI_PERIOD)
                .expect("Failed to create rsi"),
            atr: Atr::new(ATR_PERIOD)
                .expect("Failed to create atr"),
            macro_ema: Ema::new(MACRO_PERIOD)
                .expect("Failed to create macro EMA"),
            vol_sma: Sma::new(VOL_SMA_PERIOD)
                .expect("Failed to create volume SMA"),

            previous_slow_sma: None,
            previous_fast_sma: None,

            highest_price: 0.0,
            lowest_price: f64::MAX,

            bars_in_position: 0,
            cooldown_bars_remaining: 0,

            loop_count: 0,
            wallet: FuturesWallet::new(start_margin, leverage),
        }
    }

    fn reset_position_state(&mut self) {
        self.highest_price = 0.0;
        self.lowest_price = f64::MAX;
        self.bars_in_position = 0;
        self.cooldown_bars_remaining = COOLDOWN_BARS;
    }
}

impl TradingStrategy for FuturesTradingStrategy {
    fn loop_count(&mut self) -> &mut usize {
        &mut self.loop_count
    }

    fn slow_sma(&mut self) -> &mut Sma {
        &mut self.slow_sma
    }

    fn fast_sma(&mut self) -> &mut Sma {
        &mut self.fast_sma
    }

    fn trend_ema(&mut self) -> &mut Ema {
        &mut self.trend_ema
    }

    fn rsi(&mut self) -> &mut Rsi {
        &mut self.rsi
    }

    fn atr(&mut self) -> &mut Atr {
        &mut self.atr
    }

    fn macro_ema(&mut self) -> &mut Ema {
        &mut self.macro_ema
    }

    fn vol_sma(&mut self) -> &mut Sma {
        &mut self.vol_sma
    }

    fn on_tick(&mut self, _current_price: f64) {
        // Futures tick processing - to be fully implemented
    }

    fn on_candle_close(&mut self, _candle: &Candle) {
        // Futures candle close processing - to be fully implemented
    }
}