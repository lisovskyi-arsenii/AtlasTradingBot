use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub slow_period: usize,
    pub fast_period: usize,
    pub trend_period: usize,
    pub rsi_period: usize,
    pub atr_period: usize,
    pub macro_period: usize,
    pub vol_sma_period: usize,
    pub z_score_period: usize,
    pub ema_period: usize,
    pub z_entry: f64,
    pub short_z_entry: f64,
    pub atr_multiplier: f64,
    pub short_stop_atr_mult: f64,
    pub short_tp_atr_mult: f64,
    pub short_cooldown_bars: usize,
    pub cooldown_bars: usize,
    pub loss_cooldown_bars: usize,
    pub take_profit_cooldown_bars: usize,
    pub take_profit_r_multiplier: f64,
    pub min_bars_in_position: usize,
    pub min_profit_for_rsi_exit_pct: f64,
    pub default_risk_per_trade_pct: f64,
    pub max_strategy_drawdown_pct: f64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            slow_period: 30,
            fast_period: 10,
            trend_period: 50,
            rsi_period: 14,
            atr_period: 14,
            macro_period: 200,
            vol_sma_period: 20,
            z_score_period: 40,
            ema_period: 200,
            z_entry: -1.0,
            short_z_entry: 1.0,
            atr_multiplier: 3.0,
            short_stop_atr_mult: 2.0,
            short_tp_atr_mult: 4.0,
            short_cooldown_bars: 4,
            cooldown_bars: 2,
            loss_cooldown_bars: 8,
            take_profit_cooldown_bars: 3,
            take_profit_r_multiplier: 4.5,
            min_bars_in_position: 5,
            min_profit_for_rsi_exit_pct: 0.004,
            default_risk_per_trade_pct: 0.0035,
            max_strategy_drawdown_pct: 12.0,
        }
    }
}

impl StrategyConfig {
    pub fn warmup_period(&self) -> usize {
        self.slow_period
            .max(self.fast_period)
            .max(self.trend_period)
            .max(self.rsi_period)
            .max(self.atr_period)
            .max(self.macro_period)
            .max(self.vol_sma_period)
            .max(self.z_score_period)
            .max(self.ema_period)
            .max(1)
    }

    pub fn sanitized(&self) -> Self {
        let defaults = Self::default();
        Self {
            slow_period: self.slow_period.max(1),
            fast_period: self.fast_period.max(1),
            trend_period: self.trend_period.max(1),
            rsi_period: self.rsi_period.max(1),
            atr_period: self.atr_period.max(1),
            macro_period: self.macro_period.max(1),
            vol_sma_period: self.vol_sma_period.max(1),
            z_score_period: self.z_score_period.max(1),
            ema_period: self.ema_period.max(1),
            z_entry: self.z_entry,
            short_z_entry: self.short_z_entry,
            atr_multiplier: self.atr_multiplier,
            short_stop_atr_mult: self.short_stop_atr_mult,
            short_tp_atr_mult: self.short_tp_atr_mult,
            short_cooldown_bars: self.short_cooldown_bars,
            cooldown_bars: self.cooldown_bars,
            loss_cooldown_bars: self.loss_cooldown_bars,
            take_profit_cooldown_bars: self.take_profit_cooldown_bars,
            take_profit_r_multiplier: self.take_profit_r_multiplier,
            min_bars_in_position: self.min_bars_in_position,
            min_profit_for_rsi_exit_pct: self.min_profit_for_rsi_exit_pct,
            default_risk_per_trade_pct: self.default_risk_per_trade_pct,
            max_strategy_drawdown_pct: self.max_strategy_drawdown_pct.max(0.0),
        }
        .with_fallbacks(defaults)
    }

    fn with_fallbacks(mut self, defaults: Self) -> Self {
        if self.atr_multiplier <= 0.0 {
            self.atr_multiplier = defaults.atr_multiplier;
        }
        if self.short_stop_atr_mult <= 0.0 {
            self.short_stop_atr_mult = defaults.short_stop_atr_mult;
        }
        if self.short_tp_atr_mult <= 0.0 {
            self.short_tp_atr_mult = defaults.short_tp_atr_mult;
        }
        if self.take_profit_r_multiplier <= 0.0 {
            self.take_profit_r_multiplier = defaults.take_profit_r_multiplier;
        }
        if self.default_risk_per_trade_pct <= 0.0 {
            self.default_risk_per_trade_pct = defaults.default_risk_per_trade_pct;
        }
        self
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct StrategyFileConfig {
    pub slow_period: Option<usize>,
    pub fast_period: Option<usize>,
    pub trend_period: Option<usize>,
    pub rsi_period: Option<usize>,
    pub atr_period: Option<usize>,
    pub macro_period: Option<usize>,
    pub vol_sma_period: Option<usize>,
    pub z_score_period: Option<usize>,
    pub ema_period: Option<usize>,
    pub z_entry: Option<f64>,
    pub short_z_entry: Option<f64>,
    pub atr_multiplier: Option<f64>,
    pub short_stop_atr_mult: Option<f64>,
    pub short_tp_atr_mult: Option<f64>,
    pub short_cooldown_bars: Option<usize>,
    pub cooldown_bars: Option<usize>,
    pub loss_cooldown_bars: Option<usize>,
    pub take_profit_cooldown_bars: Option<usize>,
    pub take_profit_r_multiplier: Option<f64>,
    pub min_bars_in_position: Option<usize>,
    pub min_profit_for_rsi_exit_pct: Option<f64>,
    pub default_risk_per_trade_pct: Option<f64>,
    pub max_strategy_drawdown_pct: Option<f64>,
}

impl StrategyConfig {
    pub fn from_file(file: StrategyFileConfig) -> Self {
        let defaults = Self::default();
        Self {
            slow_period: file.slow_period.unwrap_or(defaults.slow_period),
            fast_period: file.fast_period.unwrap_or(defaults.fast_period),
            trend_period: file.trend_period.unwrap_or(defaults.trend_period),
            rsi_period: file.rsi_period.unwrap_or(defaults.rsi_period),
            atr_period: file.atr_period.unwrap_or(defaults.atr_period),
            macro_period: file.macro_period.unwrap_or(defaults.macro_period),
            vol_sma_period: file.vol_sma_period.unwrap_or(defaults.vol_sma_period),
            z_score_period: file.z_score_period.unwrap_or(defaults.z_score_period),
            ema_period: file.ema_period.unwrap_or(defaults.ema_period),
            z_entry: file.z_entry.unwrap_or(defaults.z_entry),
            short_z_entry: file.short_z_entry.unwrap_or(defaults.short_z_entry),
            atr_multiplier: file.atr_multiplier.unwrap_or(defaults.atr_multiplier),
            short_stop_atr_mult: file.short_stop_atr_mult.unwrap_or(defaults.short_stop_atr_mult),
            short_tp_atr_mult: file.short_tp_atr_mult.unwrap_or(defaults.short_tp_atr_mult),
            short_cooldown_bars: file.short_cooldown_bars.unwrap_or(defaults.short_cooldown_bars),
            cooldown_bars: file.cooldown_bars.unwrap_or(defaults.cooldown_bars),
            loss_cooldown_bars: file.loss_cooldown_bars.unwrap_or(defaults.loss_cooldown_bars),
            take_profit_cooldown_bars: file.take_profit_cooldown_bars.unwrap_or(defaults.take_profit_cooldown_bars),
            take_profit_r_multiplier: file.take_profit_r_multiplier.unwrap_or(defaults.take_profit_r_multiplier),
            min_bars_in_position: file.min_bars_in_position.unwrap_or(defaults.min_bars_in_position),
            min_profit_for_rsi_exit_pct: file.min_profit_for_rsi_exit_pct.unwrap_or(defaults.min_profit_for_rsi_exit_pct),
            default_risk_per_trade_pct: file.default_risk_per_trade_pct.unwrap_or(defaults.default_risk_per_trade_pct),
            max_strategy_drawdown_pct: file.max_strategy_drawdown_pct.unwrap_or(defaults.max_strategy_drawdown_pct),
        }
        .sanitized()
    }
}
