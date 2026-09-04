use crate::models::candle::Candle;
use crate::models::candle_log_entry::CandleLogEntry;
use crate::models::data::{
    Action, BacktestResult, CryptoExchange, ExitReason, Phase, PositionType,
    SignalState,
};
use crate::models::log_level::LogLevel;
use crate::models::strategy_config::StrategyConfig;
use crate::models::trade::Trade;
use crate::spot::spot_wallet::Wallet;
use crate::strategy::entry_filters::EntryFilters;
use crate::strategy::position_manager::PositionManager;
use crate::strategy::reporter::Reporter;
use crate::strategy::risk_gate::RiskGate;
use crate::strategy::{TradingStrategy, VolatilityRegime};
use std::collections::VecDeque;
use ta::indicators::AverageTrueRange as Atr;
use ta::indicators::ExponentialMovingAverage as Ema;
use ta::indicators::RelativeStrengthIndex as Rsi;
use ta::indicators::SimpleMovingAverage as Sma;
use ta::Next;
use tokio::sync::mpsc::UnboundedSender;

pub struct SpotStrategy {
    pub slow_sma: Sma,
    pub fast_sma: Sma,
    pub trend_ema: Ema,
    pub rsi: Rsi,
    pub atr: Atr,
    pub macro_ema: Ema,
    pub vol_sma: Sma,
    pub previous_slow_sma: Option<f64>,
    pub previous_fast_sma: Option<f64>,
    pub previous_rsi: Option<f64>,
    pub previous_macro_ema: Option<f64>,

    pub initial_capital: f64,

    /// Long/short position lifecycle state — see
    /// `position_manager::PositionManager` doc comment for why this was
    /// split out, and for why the entry/exit *decision* logic stayed here.
    pub position: PositionManager,

    pub loop_count: usize,
    pub wallet: Wallet,
    pub symbol: String,
    /// Trade history, equity curve, and drawdown-halt state — see
    /// `reporter::Reporter` doc comment for why this was split out.
    pub reporter: Reporter,
    pub last_atr_value: f64,
    pub last_rsi_value: f64,
    pub price_history: VecDeque<f64>,
    pub ema_value: Option<f64>,
    pub config: StrategyConfig,
    pub log_tx: UnboundedSender<CandleLogEntry>,
    pub log_level: LogLevel,

    // --- Entry confirmation gates (MTF, OBI, spread, time-of-day,
    // Fear & Greed, BTC circuit-breaker) — see `entry_filters::EntryFilters`
    // doc comment for why this was split out.
    pub entry_filters: EntryFilters,

    // --- Risk sizing / adaptive risk / order splitting ---
    /// Position sizing, adaptive-risk, and order-splitting state — see
    /// `risk_gate::RiskGate` doc comment for why this was split out.
    pub risk_gate: RiskGate,

    // --- Safety Limits ---
    /// Daily loss limit in USDT
    pub daily_loss_limit_usdt: f64,
    /// Daily PnL tracking
    pub daily_pnl: f64,
    /// Daily trade count
    pub daily_trade_count: usize,
    /// Last reset timestamp for daily limits
    pub daily_reset_timestamp: i64,

    // --- Volatility regime detection ---
    /// ATR history for regime detection.
    pub atr_history: VecDeque<f64>,
    /// Current volatility regime.
    pub current_regime: VolatilityRegime,
}

impl SpotStrategy {
    pub fn new(
        initial_capital: f64,
        symbol: &str,
        log_tx: UnboundedSender<CandleLogEntry>,
        exchange: CryptoExchange,
        config: StrategyConfig,
        log_level: LogLevel,
        initial_state: Option<crate::execution::state::PositionState>,
    ) -> Self {
        let config = config.sanitized();

        let mut strat = Self {
            slow_sma: Sma::new(config.slow_period).expect("Failed to create slow SMA"),
            fast_sma: Sma::new(config.fast_period).expect("Failed to create fast SMA"),
            trend_ema: Ema::new(config.trend_period).expect("Failed to create trend EMA"),
            rsi: Rsi::new(config.rsi_period).expect("Failed to create RSI"),
            atr: Atr::new(config.atr_period).expect("Failed to create ATR"),
            macro_ema: Ema::new(config.macro_period).expect("Failed to create macro EMA"),
            vol_sma: Sma::new(config.vol_sma_period).expect("Failed to create volume SMA"),
            previous_slow_sma: None,
            previous_fast_sma: None,
            previous_rsi: None,
            previous_macro_ema: None,

            initial_capital,
            position: PositionManager::new(),

            loop_count: 0,
            wallet: Wallet::new(initial_capital, exchange),
            symbol: symbol.to_string(),
            reporter: Reporter::new(initial_capital),
            last_atr_value: 0.0,
            last_rsi_value: 0.0,
            price_history: VecDeque::with_capacity(config.warmup_period()),
            ema_value: None,
            atr_history: VecDeque::with_capacity(50),
            current_regime: VolatilityRegime::Normal,
            log_tx,
            log_level,
            entry_filters: EntryFilters::new(config.mtf_ema_period, config.btc_crash_lookback_bars),
            risk_gate: RiskGate::new(config.default_risk_per_trade_pct),
            daily_loss_limit_usdt: 500.0,
            daily_pnl: 0.0,
            daily_trade_count: 0,
            daily_reset_timestamp: chrono::Utc::now().timestamp(),
            config,
        };

        if let Some(state) = initial_state {
            if state.is_holding {
                strat.position.is_holding_asset = true;
                strat.position.buy_price = state.entry_price;
                strat.position.initial_stop_price = state.initial_stop_price;
                strat.position.dynamic_trailing_stop = state.trailing_stop_price;
                strat.position.use_dynamic_trailing_stop = true;
                strat.position.highest_price = state.entry_price;
                strat.wallet.crypto_balance = state.qty;
                strat.wallet.usdt_balance = initial_capital.max(0.0);
                strat.position.entry_equity = strat.wallet.usdt_balance + (state.qty * state.entry_price);
                strat.reporter.current_equity = strat.position.entry_equity;
                strat.reporter.peak_equity = strat.position.entry_equity;
            } else if state.is_short {
                strat.position.is_short = true;
                strat.position.short_entry_price = state.entry_price;
                strat.position.short_stop_price = state.initial_stop_price;
                strat.position.short_tp_price = 0.0; // Assume 0 if not saved
                strat.position.dynamic_trailing_stop = state.trailing_stop_price;
                strat.position.short_margin_usdt = state.qty; // For short, qty is margin
                strat.wallet.usdt_balance = initial_capital.max(0.0);
                strat.position.entry_equity = strat.wallet.usdt_balance;
                strat.reporter.current_equity = strat.position.entry_equity;
                strat.reporter.peak_equity = strat.position.entry_equity;
            }
        }
        
        strat
    }

    /// Aggregate base candles into a higher-timeframe candle and, once one
    /// completes, refresh the HTF trend verdict from its EMA. Called once per
    /// base candle so it also works in backtests. Delegates to `EntryFilters`.
    fn update_htf_trend(&mut self, close: f64) {
        self.entry_filters.update_htf_trend(close, self.config.mtf_bars);
    }

    /// Update 4H timeframe trend (4H = 4 * 1H candles). Delegates to `EntryFilters`.
    fn update_4h_trend(&mut self, close: f64) {
        self.entry_filters.update_4h_trend(close);
    }

    /// Update volatility regime based on ATR history.
    /// Low: ATR < 1.5% of price, Normal: 1.5-3%, High: > 3%
    fn update_volatility_regime(&mut self, atr_value: f64, current_price: f64) {
        let atr_pct = (atr_value / current_price) * 100.0;
        
        self.atr_history.push_back(atr_pct);
        if self.atr_history.len() > 50 {
            self.atr_history.pop_front();
        }

        // Use average of recent ATR values for regime detection
        if self.atr_history.len() >= 20 {
            let avg_atr_pct: f64 = self.atr_history.iter().sum::<f64>() / self.atr_history.len() as f64;
            
            self.current_regime = if avg_atr_pct < 1.5 {
                VolatilityRegime::Low
            } else if avg_atr_pct < 3.0 {
                VolatilityRegime::Normal
            } else {
                VolatilityRegime::High
            };
        }
    }

    /// Update dynamic trailing stop for open positions
    fn update_dynamic_trailing_stop(&mut self, current_price: f64, atr_value: f64) {
        if !self.position.use_dynamic_trailing_stop || !self.position.is_holding_asset {
            return;
        }

        // Initialize trailing stop on first update if it's 0
        if self.position.dynamic_trailing_stop == 0.0 {
            self.position.dynamic_trailing_stop = self.position.initial_stop_price;
            return;
        }

        // Only start trailing if we are in profit by at least 1 ATR
        let profit_threshold = self.position.buy_price + atr_value;
        if current_price < profit_threshold {
            return; // Give it room to breathe
        }

        // Calculate new trailing stop: current price - 3 * ATR (wider to avoid noise)
        let new_trailing_stop = current_price - (atr_value * 3.0);
        
        // Only move stop up (for long positions), never down
        if new_trailing_stop > self.position.dynamic_trailing_stop {
            self.position.dynamic_trailing_stop = new_trailing_stop;
            
            if self.log_debug() {
                println!(
                    "[TRAILING-STOP] Updated to ${:.2} (price: ${:.2}, ATR: ${:.2})",
                    self.position.dynamic_trailing_stop, current_price, atr_value
                );
            }
        }
    }

    /// Update Fear & Greed Index (LIVE only - called from external API).
    /// Delegates to `EntryFilters`; the debug log line stays here since only
    /// `SpotStrategy` knows the configured `LogLevel`.
    pub fn update_fear_greed_index(&mut self, index: f64) {
        self.entry_filters.update_fear_greed_index(index);
        if self.log_debug() {
            println!("[FEAR-GREED] Updated to {:.0}", self.entry_filters.fear_greed_index);
        }
    }

    /// Record a trade result for adaptive parameter tuning
    /// Record a trade result for adaptive parameter tuning (delegates to
    /// `RiskGate`; the debug log line stays here since only `SpotStrategy`
    /// knows the configured `LogLevel`).
    pub fn record_trade_result(&mut self, pnl_pct: f64) {
        if let Some((win_rate, avg_pnl)) = self.risk_gate.record_trade_result(pnl_pct) {
            if self.log_debug() && self.risk_gate.adaptive_risk_multiplier != 1.0 {
                println!(
                    "[ADAPTIVE-RISK] Win rate: {:.1}%, Avg PnL: {:.2}%, Risk multiplier: {:.2}",
                    win_rate * 100.0, avg_pnl * 100.0, self.risk_gate.adaptive_risk_multiplier
                );
            }
        }
    }

    /// Check if daily limits allow new entry
    fn check_daily_limits(&mut self) -> bool {
        let now: i64 = chrono::Utc::now().timestamp();
        let day_seconds: i64 = 86400;
        
        // Reset daily counters if new day
        if now - self.daily_reset_timestamp > day_seconds {
            self.daily_pnl = 0.0;
            self.daily_trade_count = 0;
            self.daily_reset_timestamp = now;
            
            if self.log_debug() {
                println!("[DAILY-LIMITS] Reset daily counters");
            }
        }
        
        // Check daily loss limit
        if self.daily_pnl < -self.daily_loss_limit_usdt {
            if self.log_debug() {
                println!(
                    "[DAILY-LIMITS] BLOCKED: Daily loss ${:.2} exceeds limit ${:.2}",
                    -self.daily_pnl, self.daily_loss_limit_usdt
                );
            }
            return false;
        }
        
        true
    }


    /// Update BTC price history for circuit-breaker filter. Called once per
    /// candle close (backtest) or tick (live) to maintain the price window.
    /// Delegates to `EntryFilters`.
    pub fn update_btc_price(&mut self, btc_price: f64) {
        self.entry_filters.update_btc_price(btc_price, self.config.btc_crash_lookback_bars);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    fn log_normal(&self) -> bool {
        matches!(self.log_level, LogLevel::Normal | LogLevel::Debug)
    }

    fn log_debug(&self) -> bool {
        matches!(self.log_level, LogLevel::Debug)
    }

    /// Push a completed trade into history (delegates to `Reporter`).
    /// For LONG trades call this *after* resetting position state.
    fn push_trade(
        &mut self,
        entry_price: f64,
        exit_price: f64,
        profit_percent: f64,
        pnl_usdt: f64,
        bars: usize,
        exit_reason: &str,
        side: &str,
    ) {
        self.reporter.push_trade(entry_price, exit_price, profit_percent, pnl_usdt, bars, exit_reason, side);
    }

    /// Unrealised PnL of the open simulated short, in USDT.
    fn short_unrealized_pnl_usdt(&self, current_price: f64) -> f64 {
        if !self.position.is_short || self.position.short_entry_price <= 0.0 || self.position.short_margin_usdt <= 0.0 {
            return 0.0;
        }

        self.position.short_margin_usdt * (self.position.short_entry_price - current_price) / self.position.short_entry_price
    }

    /// Mark-to-market equity: wallet value + reserved short margin (still our
    /// collateral, was subtracted from the wallet on entry) + open short PnL.
    /// Without adding the reserved margin back, equity is understated for the whole
    /// time a short is open, producing a fake drawdown the size of the margin.
    ///
    /// Stays on `SpotStrategy` (not `Reporter`) because it needs `wallet` and
    /// the short-position fields, which `Reporter` deliberately doesn't own.
    fn mark_to_market_equity(&self, current_price: f64) -> f64 {
        self.wallet.total_value(current_price)
            + self.position.short_margin_usdt
            + self.short_unrealized_pnl_usdt(current_price)
    }

    fn update_equity_curve(&mut self, current_price: f64, phase: Phase) {
        let equity = self.mark_to_market_equity(current_price);
        self.reporter.update_equity_curve(equity, self.loop_count, phase);
    }

    pub fn expectancy_per_trade_usdt(&self) -> f64 {
        self.reporter.expectancy_per_trade_usdt()
    }

    pub fn max_drawdown_pct(&self) -> f64 {
        self.reporter.calculate_max_drawdown()
    }

    pub fn final_equity(&self, last_price: f64) -> f64 {
        self.mark_to_market_equity(last_price)
    }

    pub fn drawdown_stop_active(&self) -> bool {
        self.reporter.drawdown_stop_active
    }

    /// Refresh drawdown-halt state (delegates to `Reporter`); the one-shot
    /// activation log line stays here since only `SpotStrategy` knows the
    /// configured `LogLevel`.
    fn refresh_drawdown_state(&mut self, current_price: f64) {
        let equity = self.mark_to_market_equity(current_price);
        if let Some(drawdown_pct) = self.reporter.refresh_drawdown_state(equity, self.config.max_strategy_drawdown_pct) {
            if self.log_normal() {
                println!(
                    "[RISK] MAX_DRAWDOWN_STOP activated at {:.2}% drawdown. New entries blocked.",
                    drawdown_pct
                );
            }
        }
    }

    // ── signal computation ───────────────────────────────────────────────────

    fn compute_signal_state(
        &self,
        current_price: f64,
        z_score: f64,
        ema_fast: f64,
        ema_slow: f64,
        in_cooldown: bool,
        rsi_val: f64,
        candle: &Candle,
        vol_sma_val: f64,
    ) -> SignalState {
        // Regime gate: a confirmed uptrend needs the fast EMA above the slow EMA AND
        // price above the slow EMA; downtrend is the mirror.
        // Local trend filter removed to allow buying deep panic dips.
        let bullish_trend: bool = true;
        let bearish_trend: bool = true;

        // RSI guard: don't buy a dip that is already deeply overbought, and don't
        // short a rip that is already deeply oversold.
        let rsi_ok_long: bool = rsi_val < 70.0;
        let rsi_ok_short: bool = rsi_val > 30.0;
        
        // 1. Volume Confirmation: 
        let vol_ok: bool = true;
        
        // 3. Momentum Check: Price action momentum
        // Relaxed for 1h mean reversion
        let bullish_momentum: bool = true;
        let bearish_momentum: bool = true;

        // Optional regime filter: only fade dips in an uptrend / rips in a downtrend.
        let long_regime_ok: bool = !self.config.use_trend_filter || bullish_trend;
        let short_regime_ok: bool = !self.config.use_trend_filter || bearish_trend;

        // Mean-reversion entries: long when oversold (z low), short when overbought (z high).
        let bullish_cross: bool = long_regime_ok && z_score < self.config.z_entry && rsi_ok_long && vol_ok && bullish_momentum;
        let bearish_cross: bool = short_regime_ok && z_score > self.config.short_z_entry && rsi_ok_short && vol_ok && bearish_momentum;

        let no_signal_reason = if self.position.is_holding_asset {
            "HOLDING_LONG".into()
        } else if self.position.is_short {
            "SHORT_ACTIVE".into()
        } else if in_cooldown {
            "COOLDOWN".into()
        } else if !long_regime_ok {
            "NOT_IN_UPTREND".into()
        } else if !bullish_cross {
            format!("Z_ENTRY_FAILED {:.2}", z_score)
        } else {
            "READY".into()
        };

        SignalState {
            z_score,
            bullish_cross,
            bearish_cross,
            bullish_trend,
            bullish_rsi: rsi_ok_long,
            strong_macro_trend: bullish_trend,
            strong_volume: vol_ok,
            fast_slow_diff: 0.0,
            price_trend_diff: 0.0,
            no_signal_reason,
            short_entry: self.config.enable_short
                && !self.position.is_short
                && !self.position.is_holding_asset
                && bearish_cross
                && self.position.short_cooldown_bars == 0,
            bearish_trend,
            short_no_signal_reason: if self.position.is_short {
                "SHORT_ACTIVE".into()
            } else {
                "SHORT_READY".into()
            },
        }
    }

    // ── entry / exit helpers ─────────────────────────────────────────────────

    fn apply_exit_cooldown(&mut self, exit_reason: ExitReason, profit_percent: f64) {
        self.position.cooldown_bars_remaining = if profit_percent < 0.0 {
            self.config.loss_cooldown_bars
        } else {
            match exit_reason {
                ExitReason::TakeProfit => self.config.take_profit_cooldown_bars,
                _ => self.config.cooldown_bars,
            }
        };
    }

    /// Entry-confirmation gate chain shared by `try_enter_position` and
    /// `try_enter_short` — was duplicated verbatim between the two (only
    /// `want_long` differed) before this consolidation. Short-circuits in
    /// the same order as the pre-consolidation code, so behavior (and log
    /// output) is unchanged.
    fn entry_filters_confirm(&mut self, want_long: bool) -> bool {
        // Higher-timeframe trend gate: only trade with the macro trend.
        if !self.entry_filters.mtf_confirms(want_long, &self.config) {
            return false;
        }
        // LIVE-only microstructure confirmation. No-op in backtests (filter
        // off / no book).
        if !self.entry_filters.order_book_confirms(want_long, &self.config, self.log_debug()) {
            return false;
        }
        // Bid-ask spread confirmation: avoid entries during wide spreads.
        if !self.entry_filters.spread_confirms(self.log_debug()) {
            return false;
        }
        // Time-of-day pattern confirmation: avoid trading during low-liquidity hours.
        if !self.entry_filters.time_of_day_confirms(self.log_debug()) {
            return false;
        }
        // Fear & Greed Index confirmation.
        if !self.entry_filters.fear_greed_confirms(want_long, self.log_debug()) {
            return false;
        }
        // Daily limits check: stop trading if daily loss limit exceeded.
        if !self.check_daily_limits() {
            return false;
        }
        // BTC circuit-breaker: block entries when BTC is crashing to reduce
        // drawdown during market-wide sell-offs.
        if !self.entry_filters.btc_circuit_breaker_confirms(&self.config, self.log_normal()) {
            return false;
        }
        true
    }

    /// Volatility/regime-adjusted position size in USDT (long `position_usdt`
    /// or short `margin_usdt` — the formula is identical, only what the
    /// caller does with the result differs), sized off the tighter of the
    /// ATR stop and the hard panic stop. Returns `None` when any gate blocks
    /// the entry: volatility too low, over the safety-limit ceiling, or
    /// below the symbol's min-notional. Was duplicated verbatim between
    /// `try_enter_position` and `try_enter_short` before this consolidation.
    fn calculate_entry_size(&self, current_price: f64, atr_value: f64, stop_distance: f64) -> Option<f64> {
        // Noise filter: block entry if volatility is too low (less than 0.15% ATR).
        let atr_pct = (atr_value / current_price) * 100.0;
        if atr_pct < 0.15 {
            if self.log_debug() {
                println!("[ATR] BLOCKED: Volatility too low ({:.2}% < 0.15%)", atr_pct);
            }
            return None;
        }

        // Base risk amount with adaptive adjustment based on recent performance.
        let base_risk_amount = self.reporter.current_equity * self.risk_gate.get_adaptive_risk_per_trade();

        // Volatility adjustment factor: higher ATR = smaller position.
        // Normalize ATR around 2% (typical for crypto), clamp 0.5x-2x.
        let volatility_factor = (2.0 / atr_pct.max(0.5)).min(2.0).max(0.5);

        // Regime-based adjustment.
        let regime_factor = match self.current_regime {
            VolatilityRegime::Low => 1.2,    // Increase position in low vol
            VolatilityRegime::Normal => 1.0,  // Standard risk
            VolatilityRegime::High => 0.7,    // Reduce position in high vol
        };

        let risk_amount = base_risk_amount * volatility_factor * regime_factor;

        // Size off the stop that will actually trigger first: the tighter of the ATR
        // stop and the hard panic stop. Otherwise, for coins whose ATR stop is wider
        // than panic_stop_loss_pct, the panic stop fires first and the realised loss
        // far exceeds default_risk_per_trade_pct.
        let risk_distance = self.risk_gate.effective_stop_distance(current_price, stop_distance, self.config.panic_stop_loss_pct);
        let size_usdt = (risk_amount / (risk_distance / current_price)).min(self.wallet.usdt_balance * 0.98);

        // Check position size limit (safety feature for live trading).
        if !self.risk_gate.check_position_size_limit(size_usdt, self.log_debug()) {
            return None;
        }
        if size_usdt < self.wallet.filters.min_notional {
            return None;
        }

        Some(size_usdt)
    }

    fn try_enter_position(
        &mut self,
        current_price: f64,
        atr_value: f64,
        signal: SignalState,
        in_cooldown: bool,
    ) -> Action {
        if !self.config.enable_long || !signal.bullish_cross {
            return Action::NoSignal;
        }

        // Long and short are mutually exclusive: never stack a long on top of an
        // open short. Otherwise both legs share the single `current_equity`
        // baseline and each leg's realised PnL absorbs the other's swings.
        if self.position.is_holding_asset || self.position.is_short || in_cooldown || self.reporter.drawdown_stop_active {
            return Action::NoSignal;
        }

        if !self.entry_filters_confirm(true) {
            return Action::NoSignal;
        }

        let stop_distance = atr_value * self.config.atr_multiplier;
        let position_usdt = match self.calculate_entry_size(current_price, atr_value, stop_distance) {
            Some(sz) => sz,
            None => return Action::NoSignal,
        };

        // Calculate split orders if enabled
        let order_sizes: Vec<f64> = self.risk_gate.calculate_split_orders(position_usdt, self.log_debug());

        // Execute orders (in backtesting, we simulate as single order for simplicity)
        // In live trading, this would execute multiple smaller orders
        let total_executed_usdt: f64 = if order_sizes.len() > 1 && self.log_debug() {
            println!("[ORDER-SPLIT] Executing {} split orders", order_sizes.len());
            order_sizes.iter().sum()
        } else {
            position_usdt
        };

        if let Some(executed_price) = self.wallet.buy(current_price, total_executed_usdt, true) {
            self.position.is_holding_asset = true;
            self.position.buy_price = executed_price;
            self.position.highest_price = executed_price;
            self.position.initial_stop_price = executed_price - stop_distance;
            self.position.panic_stop_price = if self.config.panic_stop_loss_pct > 0.0 {
                executed_price * (1.0 - self.config.panic_stop_loss_pct)
            } else {
                0.0
            };
            self.position.target_price =
                executed_price + atr_value * self.config.take_profit_r_multiplier; // Use raw ATR, not stop_distance, for R-multiplier to avoid compounding multipliers
            self.position.entry_equity = self.reporter.current_equity;
            self.position.bars_in_position = 0;
            return Action::Buy;
        }
        Action::NoSignal
    }

    fn try_enter_short(
        &mut self,
        current_price: f64,
        atr_value: f64,
        signal: &SignalState,
    ) -> Action {
        if !signal.short_entry {
            return Action::NoSignal;
        }

        // Mutually exclusive with an open long (see try_enter_position).
        if self.position.is_short || self.position.is_holding_asset || self.reporter.drawdown_stop_active {
            return Action::NoSignal;
        }

        if !self.entry_filters_confirm(false) {
            return Action::NoSignal;
        }

        let stop_distance = atr_value * self.config.short_stop_atr_mult;
        let margin_usdt = match self.calculate_entry_size(current_price, atr_value, stop_distance) {
            Some(sz) => sz,
            None => return Action::NoSignal,
        };

        // Резервуємо маржу:
        self.wallet.usdt_balance -= margin_usdt;
        self.position.short_margin_usdt = margin_usdt;

        self.position.is_short = true;
        self.position.short_entry_price = current_price;
        self.position.short_stop_price = current_price + stop_distance;
        self.position.short_tp_price = current_price - atr_value * self.config.short_tp_atr_mult;
        self.position.short_panic_price = if self.config.panic_stop_loss_pct > 0.0 {
            current_price * (1.0 + self.config.panic_stop_loss_pct)
        } else {
            0.0
        };

        Action::ShortSell
    }

    /// Close a simulated short position.
    ///
    /// # Accounting (was the root cause of the -33% bug):
    ///
    ///  Opening  : wallet -= margin  (margin reserved)
    ///  Closing  : wallet += margin + pnl_usdt
    ///
    /// An earlier version only did `wallet += pnl_usdt`, so the margin was
    /// permanently destroyed on every short trade. When mixing long + short
    /// the wallet drained ~`margin × trade_count` regardless of PnL sign.
    fn execute_short_exit(&mut self, exit_price: f64, reason: ExitReason) -> Action {
        let pnl_pct: f64 = (self.position.short_entry_price - exit_price) / self.position.short_entry_price * 100.0;
        let pnl_usdt: f64 = self.position.short_margin_usdt * (pnl_pct / 100.0);

        self.wallet.usdt_balance += self.position.short_margin_usdt + pnl_usdt;

        // FIX (C8): track daily PnL on short exits — was previously skipped,
        // making the daily loss limit partially blind to short losses.
        self.daily_pnl += pnl_usdt;
        self.daily_trade_count += 1;

        // Record result for adaptive risk (same as long exits)
        self.record_trade_result(pnl_pct / 100.0);

        self.push_trade(
            self.position.short_entry_price,
            exit_price,
            pnl_pct,
            pnl_usdt,
            self.position.bars_in_position,
            reason.as_str(),
            "SHORT",
        );
        self.reset_short_state();
        Action::CloseShort
    }

    fn try_close_short(&self, candle: &Candle, z_score: f64) -> Option<(f64, ExitReason)> {
        if !self.position.is_short {
            return None;
        }

        // 1. Stop-loss: whichever stop is closer to entry (lower price for a short)
        //    is hit first as price rises, so exit there and book that smaller loss.
        let stop_price: f64 = if self.position.short_panic_price > 0.0 {
            self.position.short_stop_price.min(self.position.short_panic_price)
        } else {
            self.position.short_stop_price
        };
        if candle.high >= stop_price {
            let reason = if self.position.short_panic_price > 0.0
                && self.position.short_panic_price <= self.position.short_stop_price
            {
                ExitReason::PanicStop
            } else {
                ExitReason::ShortStop
            };
            return Some((stop_price, reason));
        }
        // 3. Mean-reversion take profit: shorted a rip (z high), exit once z reverts
        //    back to the mean and we are in profit (price below entry).
        if z_score <= 0.0 {
            return Some((candle.close, ExitReason::ReversionExit));
        }
        // Time stop: if we are stuck in a drawdown for 6 bars, momentum failed.
        if self.position.bars_in_position >= 6 && candle.close > self.position.short_entry_price {
            return Some((candle.close, ExitReason::WeakMomentumExit));
        }
        // 4. ATR "runner" target as a backstop.
        if candle.low <= self.position.short_tp_price {
            return Some((self.position.short_tp_price, ExitReason::ShortTakeProfit));
        }
        None
    }

    fn reset_short_state(&mut self) {
        self.position.reset_short_state();
    }

    /// Price at which a long exit for `reason` actually fills — was
    /// duplicated verbatim between `on_tick` and `on_candle_close`.
    fn exit_trigger_price(&self, reason: ExitReason, current_price: f64) -> f64 {
        match reason {
            ExitReason::PanicStop => self.position.panic_stop_price,
            ExitReason::InitialStop => self.position.initial_stop_price,
            ExitReason::TakeProfit => self.position.target_price,
            _ => current_price,
        }
    }

    fn determine_exit_reason(
        &self,
        current_price: f64,
        candle: Option<&Candle>,
        signal: Option<&SignalState>,
    ) -> Option<ExitReason> {
        if !self.position.is_holding_asset || self.position.buy_price == 0.0 {
            return None;
        }

        // Use the candle's intrabar extremes when available so stops/targets are
        // evaluated against the worst/best price within the bar, not just the close.
        let check_low = candle.map(|c| c.low).unwrap_or(current_price);
        let check_high = candle.map(|c| c.high).unwrap_or(current_price);

        // --- PRIORITY 1: stop-loss (never gated by min_bars) ---------------------
        // Two stops can be active: the ATR stop and the hard panic stop. Whichever
        // is *closer to entry* (higher price for a long) is hit first as price falls,
        // so exit there and book that (smaller) loss. Booking the farther stop when
        // both are breached in one candle overstates the loss.
        let stop_price = if self.position.use_dynamic_trailing_stop && self.position.dynamic_trailing_stop > 0.0 {
            self.position.dynamic_trailing_stop.max(self.position.panic_stop_price)
        } else {
            self.position.initial_stop_price.max(self.position.panic_stop_price)
        };
        if stop_price > 0.0 && check_low <= stop_price {
            let reason = if self.position.use_dynamic_trailing_stop && self.position.dynamic_trailing_stop > 0.0 && stop_price == self.position.dynamic_trailing_stop {
                ExitReason::TrailingStop
            } else if self.position.panic_stop_price > 0.0 && self.position.panic_stop_price >= self.position.initial_stop_price {
                ExitReason::PanicStop
            } else {
                ExitReason::InitialStop
            };
            return Some(reason);
        }

        // --- PRIORITY 2: mean-reversion take profit ------------------------------
        // The whole point of a mean-reversion long: we bought a dip (z below entry),
        // so we book profit once price reverts back to (or above) its mean.
        if let Some(signal) = signal {
            if signal.z_score >= self.config.z_exit {
                return Some(ExitReason::ReversionExit);
            }
        }

        // ATR "runner" target as a backstop in case of strong continuation.
        if self.position.target_price > 0.0 && check_high >= self.position.target_price {
            return Some(ExitReason::TakeProfit);
        }

        // --- PRIORITY 3: trend-reversal exit & Time Stop -------------------------
        // Time stop: if we are stuck in a drawdown for 6 bars, momentum failed.
        if self.position.bars_in_position >= 6 && current_price < self.position.buy_price {
            return Some(ExitReason::WeakMomentumExit);
        }
        
        if self.position.bars_in_position < self.config.min_bars_in_position {
            return None;
        }
        if let Some(signal) = signal {
            if signal.bearish_cross
                && current_price > self.position.buy_price * (1.0 + self.config.min_profit_for_rsi_exit_pct)
            {
                return Some(ExitReason::BearishCross);
            }
        }

        None
    }

    fn execute_exit(
        &mut self,
        trigger_price: f64,
        market_price: f64,
        reason: ExitReason,
        phase: Phase,
        source: &str,
    ) -> Action {
        if let Some(executed_price) = self.wallet.sell_all(trigger_price, false) {
            let profit_percent = if self.position.buy_price > 0.0 {
                (executed_price - self.position.buy_price) / self.position.buy_price * 100.0
            } else {
                0.0
            };

            if self.log_normal() {
                println!(
                    "[EXIT] {} {} {:?} at ${:.2}  PnL {:.2}%",
                    source, self.symbol, reason, executed_price, profit_percent
                );
            }

            // Record trade result for adaptive parameter tuning
            self.record_trade_result(profit_percent / 100.0); // Convert to decimal

            // Save before reset
            let ep: f64 = self.position.buy_price;
            let bars: usize = self.position.bars_in_position;
            let entry_eq: f64 = self.position.entry_equity;

            self.update_equity_curve(market_price, phase);
            self.apply_exit_cooldown(reason, profit_percent);
            self.reset_position_state();

            let pnl_usdt: f64 = self.reporter.current_equity - entry_eq;
            
            // Update daily PnL tracking
            self.daily_pnl += pnl_usdt;
            self.daily_trade_count += 1;
            
            if self.log_debug() {
                println!(
                    "[DAILY-PNL] Daily PnL: ${:.2}, Trades: {}, Loss limit: ${:.2}",
                    self.daily_pnl, self.daily_trade_count, self.daily_loss_limit_usdt
                );
            }
            
            self.push_trade(
                ep,
                executed_price,
                profit_percent,
                pnl_usdt,
                bars,
                reason.as_str(),
                "LONG",
            );

            return Action::Sell;
        }
        Action::NoSignal
    }

    pub fn finalize_backtest(&mut self, current_price: f64) {
        if self.position.is_holding_asset {
            let _ = self.execute_exit(
                current_price,
                current_price,
                ExitReason::EndOfData,
                Phase::PostSell,
                "BACKTEST_END",
            );
        }
        if self.position.is_short {
            let _ = self.execute_short_exit(current_price, ExitReason::EndOfData);
        }
    }

    fn reset_position_state(&mut self) {
        self.position.reset_position_state();
    }

    fn position_type(&self) -> PositionType {
        self.position.position_type()
    }

    // ── indicator helpers ────────────────────────────────────────────────────

    fn calculate_sma(&self, period: usize) -> f64 {
        let len = self.price_history.len();
        if len < period {
            return 0.0;
        }
        let sum: f64 = (len - period..len).map(|i| self.price_history[i]).sum();
        sum / period as f64
    }

    fn calculate_std_dev(&self, period: usize, sma: f64) -> f64 {
        let len = self.price_history.len();
        if len < period {
            return 0.0;
        }
        let var: f64 = (len - period..len)
            .map(|i| (self.price_history[i] - sma).powi(2))
            .sum::<f64>()
            / period as f64;
        var.sqrt()
    }

    fn update_ema(&mut self, current_price: f64) -> f64 {
        let k = 2.0 / (self.config.ema_period as f64 + 1.0);
        let new_ema = match self.ema_value {
            Some(prev) => current_price * k + prev * (1.0 - k),
            None => current_price,
        };
        self.ema_value = Some(new_ema);
        new_ema
    }

    // ── backtest reporting ───────────────────────────────────────────────────

    pub fn compute_backtest_result(&self, last_price: f64, csv_file: &str) -> BacktestResult {
        let total_trades: usize = self.reporter.trade_history.len();
        let initial_capital: f64 = self.initial_capital;
        let final_equity: f64 = self.mark_to_market_equity(last_price);
        let max_drawdown: f64 = self.reporter.calculate_max_drawdown();

        if total_trades == 0 {
            return BacktestResult {
                csv_file: csv_file.to_string(),
                symbol: self.symbol.clone(),
                total_trades: 0,
                win_rate: 0.0,
                profit_factor: 0.0,
                total_pnl_pct: 0.0,
                total_pnl_usdt: 0.0,
                avg_pnl_usdt: 0.0,
                max_drawdown_pct: max_drawdown,
                sharpe_ratio: 0.0,
                recovery_factor: 0.0,
                max_consecutive_losses: 0,
                final_equity,
                initial_capital,
            };
        }

        let winning: Vec<&Trade> = self.reporter
            .trade_history
            .iter()
            .filter(|t| t.pnl_usdt > 0.0)
            .collect();
        let losing: Vec<&Trade> = self.reporter
            .trade_history
            .iter()
            .filter(|t| t.pnl_usdt <= 0.0)
            .collect();
        let win_rate = winning.len() as f64 / total_trades as f64 * 100.0;
        let total_pnl_usdt: f64 = self.reporter.trade_history.iter().map(|t| t.pnl_usdt).sum();
        let gross_profit: f64 = winning.iter().map(|t| t.pnl_usdt).sum();
        let gross_loss: f64 = losing.iter().map(|t| t.pnl_usdt.abs()).sum();
        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        let returns: Vec<f64> = self.reporter
            .trade_history
            .iter()
            .map(|t| t.pnl_pct / 100.0)
            .collect();
        let n: f64 = returns.len() as f64;
        let mean_ret: f64 = returns.iter().sum::<f64>() / n;
        let variance: f64 = if n > 1.0 {
            returns.iter().map(|r| (r - mean_ret).powi(2)).sum::<f64>() / (n - 1.0)
        } else {
            0.0
        };
        let std_dev = variance.sqrt();
        // Per-trade Sharpe (mean/std of trade returns). The previous code multiplied
        // by sqrt(8760) as if each trade were an hourly return, which produced absurd
        // values (e.g. -8000); trade returns are not hourly so we do not annualise.
        let sharpe_ratio = if std_dev > 0.0 {
            mean_ret / std_dev
        } else {
            0.0
        };

        let total_pnl_pct: f64 = if initial_capital > 0.0 {
            (final_equity - initial_capital) / initial_capital * 100.0
        } else {
            0.0
        };

        let recovery_factor: f64 = if max_drawdown.abs() > 0.0 {
            total_pnl_pct.abs() / max_drawdown.abs()
        } else {
            0.0
        };

        let mut max_consecutive_losses = 0usize;
        let mut streak: usize = 0usize;
        for t in &self.reporter.trade_history {
            if t.pnl_usdt < 0.0 {
                streak += 1;
                if streak > max_consecutive_losses {
                    max_consecutive_losses = streak;
                }
            } else {
                streak = 0;
            }
        }

        BacktestResult {
            csv_file: csv_file.to_string(),
            symbol: self.symbol.clone(),
            total_trades,
            win_rate,
            profit_factor,
            total_pnl_pct,
            total_pnl_usdt,
            avg_pnl_usdt: total_pnl_usdt / total_trades as f64,
            max_drawdown_pct: max_drawdown,
            sharpe_ratio,
            recovery_factor,
            max_consecutive_losses,
            final_equity,
            initial_capital,
        }
    }

    pub fn print_backtest_summary(&self, last_price: f64) {
        if self.reporter.trade_history.is_empty() {
            println!("No trades were executed.");
            return;
        }
        let long_n: usize = self.reporter
            .trade_history
            .iter()
            .filter(|t| t.side == "LONG")
            .count();
        let short_n: usize = self.reporter
            .trade_history
            .iter()
            .filter(|t| t.side == "SHORT")
            .count();
        let total: usize = self.reporter.trade_history.len();
        let wins: usize = self.reporter
            .trade_history
            .iter()
            .filter(|t| t.pnl_usdt > 0.0)
            .count();
        let total_pnl: f64 = self.reporter.trade_history.iter().map(|t| t.pnl_usdt).sum();

        let long_pnl: f64 = self.reporter
            .trade_history
            .iter()
            .filter(|t| t.side == "LONG")
            .map(|t| t.pnl_usdt)
            .sum();
        let short_pnl: f64 = self.reporter
            .trade_history
            .iter()
            .filter(|t| t.side == "SHORT")
            .map(|t| t.pnl_usdt)
            .sum();

        println!("\n====== BACKTEST SUMMARY ======");
        println!(
            "Total trades : {} (Long: {} ${:.2}  |  Short: {} ${:.2})",
            total, long_n, long_pnl, short_n, short_pnl
        );
        println!("Win rate     : {:.2}%", wins as f64 / total as f64 * 100.0);
        println!("Total PnL    : ${:.2}", total_pnl);
        println!("Max drawdown : {:.2}%", self.reporter.calculate_max_drawdown());
        println!(
            "Final equity : ${:.2}",
            self.mark_to_market_equity(last_price)
        );
        println!("==============================\n");
    }

    /// Create a metrics snapshot for Prometheus export.
    pub fn to_metrics_snapshot(&self, current_price: f64) -> crate::metrics::SymbolSnapshot {
        let equity: f64 = self.mark_to_market_equity(current_price);
        let realized_pnl_usdt: f64 = self.reporter.trade_history.iter().map(|t| t.pnl_usdt).sum();
        let pnl_pct: f64 = if self.initial_capital > 0.0 {
            (equity - self.initial_capital) / self.initial_capital * 100.0
        } else {
            0.0
        };
        let drawdown_pct: f64 = if self.reporter.peak_equity > 0.0 {
            (self.reporter.peak_equity - equity) / self.reporter.peak_equity * 100.0
        } else {
            0.0
        };
        let position_side: i64 = if self.position.is_short {
            -1
        } else if self.position.is_holding_asset {
            1
        } else {
            0
        };
        let unrealized_pnl_usdt = if self.position.is_holding_asset {
            (current_price - self.position.buy_price) / self.position.buy_price * self.wallet.crypto_balance * current_price
        } else if self.position.is_short {
            self.short_unrealized_pnl_usdt(current_price)
        } else {
            0.0
        };
        let atr_pct: f64 = if current_price > 0.0 {
            (self.last_atr_value / current_price) * 100.0
        } else {
            0.0
        };

        crate::metrics::SymbolSnapshot {
            symbol: self.symbol.clone(),
            equity,
            realized_pnl_usdt,
            pnl_pct,
            drawdown_pct,
            rsi: self.last_rsi_value,
            z_score: 0.0, // Would need to compute from current state
            atr: self.last_atr_value,
            atr_pct,
            trade_count: self.reporter.trade_history.len() as i64,
            unrealized_pnl_usdt,
            position_side,
            wallet_usdt: self.wallet.usdt_balance,
            last_price: current_price,
            peak_equity: self.reporter.peak_equity,
            candle_count: self.loop_count as i64,
            drawdown_stop_active: self.reporter.drawdown_stop_active,
        }
    }
}

// ── TradingStrategy impl ─────────────────────────────────────────────────────

impl TradingStrategy for SpotStrategy {
    fn loop_count(&mut self) -> &mut usize {
        &mut self.loop_count
    }

    fn get_position_state(&self) -> Option<crate::execution::state::PositionState> {
        if self.position.is_holding_asset {
            Some(crate::execution::state::PositionState {
                symbol: self.symbol.clone(),
                is_holding: true,
                is_short: false,
                entry_price: self.position.buy_price,
                qty: self.wallet.crypto_balance,
                initial_stop_price: self.position.initial_stop_price,
                trailing_stop_price: self.position.dynamic_trailing_stop,
                // Strategies don't know about broker-side resting orders —
                // `sync_broker_state` fills this in from persisted state.
                stop_order_id: None,
            })
        } else if self.position.is_short {
            Some(crate::execution::state::PositionState {
                symbol: self.symbol.clone(),
                is_holding: false,
                is_short: true,
                entry_price: self.position.short_entry_price,
                qty: self.position.short_margin_usdt,
                initial_stop_price: self.position.short_stop_price,
                trailing_stop_price: self.position.dynamic_trailing_stop,
                stop_order_id: None,
            })
        } else {
            None
        }
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
    fn warmup_period(&self) -> usize {
        self.config.warmup_period()
    }

    fn final_equity(&self, current_price: f64) -> f64 {
        self.mark_to_market_equity(current_price)
    }
    fn total_trades(&self) -> usize {
        self.reporter.trade_history.len()
    }

    fn latest_indicators(&self) -> (f64, f64, f64, f64) {
        let atr_pct: f64 = if self.last_atr_value > 0.0 { self.last_atr_value / 100.0 } else { 0.0 };
        (self.last_rsi_value, 0.0, self.last_atr_value, atr_pct)
    }

    fn equity_state(&self, current_price: f64) -> (f64, f64, f64) {
        let equity: f64 = self.mark_to_market_equity(current_price);
        let dd: f64 = if self.reporter.peak_equity > 0.0 {
            (self.reporter.peak_equity - equity) / self.reporter.peak_equity * 100.0
        } else {
            0.0
        };
        (equity, self.reporter.peak_equity, dd)
    }

    fn position_side_int(&self) -> i64 {
        if self.position.is_holding_asset { 1 } else if self.position.is_short { -1 } else { 0 }
    }

    fn wallet_usdt_balance(&self) -> f64 {
        self.wallet.usdt_balance
    }

    fn set_order_book_imbalance(&mut self, obi: f64) {
        self.entry_filters.set_order_book_imbalance(obi);
    }

    fn update_btc_price(&mut self, btc_price: f64) {
        // Calls the inherent `SpotStrategy::update_btc_price` above (Rust
        // resolves inherent methods before trait methods), not this trait
        // method recursively.
        self.update_btc_price(btc_price);
    }

    fn set_spread_pct(&mut self, spread_pct: f64) {
        self.entry_filters.set_spread_pct(spread_pct);
    }

    fn set_fear_greed(&mut self, value: f64, _classification: String) {
        self.entry_filters.update_fear_greed_index(value);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }

    fn on_tick(&mut self, current_price: f64) {
        if self.position.is_holding_asset && self.last_atr_value > 0.0 {
            self.position.highest_price = self.position.highest_price.max(current_price);
            if let Some(reason) = self.determine_exit_reason(current_price, None, None) {
                let trigger = self.exit_trigger_price(reason, current_price);
                let _ = self.execute_exit(trigger, current_price, reason, Phase::PostSell, "TICK");
            }
        }

        if self.position.is_short {
            if current_price >= self.position.short_stop_price {
                let _ = self.execute_short_exit(self.position.short_stop_price, ExitReason::ShortStop);
            } else if current_price <= self.position.short_tp_price {
                let _ = self.execute_short_exit(self.position.short_tp_price, ExitReason::ShortTakeProfit);
            }
        }
    }

    fn on_candle_close(&mut self, candle: &Candle) {
        let current_price: f64 = candle.close;
        // NOTE: loop_count is incremented inside `update_indicators` below; do not
        // increment it here too or the warm-up period completes twice as fast.

        // Update the higher-timeframe trend every base candle (also in backtests),
        // before any early return, so HTF aggregation stays aligned with the data.
        self.update_htf_trend(current_price);
        
        // Update 4H timeframe trend for additional confirmation
        self.update_4h_trend(current_price);

        self.price_history.push_back(current_price);
        if self.price_history.len() > self.config.warmup_period() {
            self.price_history.pop_front();
        }
        if self.price_history.len() < self.config.z_score_period {
            return;
        }

        let ema_value: f64 = self.update_ema(current_price);
        let z_window: usize = self.config.z_score_period;
        let z_score_sma: f64 = self.calculate_sma(z_window);
        let z_score_std: f64 = self.calculate_std_dev(z_window, z_score_sma);
        let z_score: f64 = if z_score_std > 0.0 {
            (current_price - z_score_sma) / z_score_std
        } else {
            0.0
        };

        let indicators = self.update_indicators(candle.clone());
        self.update_equity_curve(current_price, Phase::BarClose);
        self.refresh_drawdown_state(current_price);

        let (
            slow_value,
            fast_value,
            trend_value,
            rsi_value,
            atr_value,
            macro_ema_value,
            vol_sma_value,
        ) = match indicators {
            Some(v) => v,
            None => return,
        };

        self.last_atr_value = atr_value;
        self.last_rsi_value = rsi_value;
        
        // Update volatility regime detection
        self.update_volatility_regime(atr_value, current_price);
        
        // Adjust parameters based on current volatility regime
        self.risk_gate.adjust_for_regime(self.current_regime);
        
        // Update dynamic trailing stop for open positions
        self.update_dynamic_trailing_stop(current_price, atr_value);
        
        let in_cooldown = self.position.cooldown_bars_remaining > 0;

        // Regime uses the EMA cross: trend_value (fast EMA) vs macro_ema_value (slow EMA).
        let signal: SignalState = self.compute_signal_state(
            current_price,
            z_score,
            trend_value,
            macro_ema_value,
            in_cooldown,
            rsi_value,
            candle,
            vol_sma_value,
        );

        let mut action: Action = Action::NoSignal;

        // ── long exit check ──────────────────────────────────────────────────
        if self.position.is_holding_asset {
            self.position.bars_in_position += 1;
            self.position.highest_price = self.position.highest_price.max(candle.high);
            if let Some(reason) =
                self.determine_exit_reason(current_price, Some(candle), Some(&signal))
            {
                let trigger = self.exit_trigger_price(reason, current_price);
                action =
                    self.execute_exit(trigger, current_price, reason, Phase::PostSell, "CANDLE");
            }
        }

        // ── short exit check ─────────────────────────────────────────────────
        if self.position.is_short {
            self.position.bars_in_position += 1;
            if let Some((exit_price, reason)) = self.try_close_short(candle, z_score) {
                action = self.execute_short_exit(exit_price, reason);
            }
        }

        // ── entry signals (only when no position was just closed) ────────────
        if matches!(action, Action::NoSignal) {
            action = self.try_enter_position(current_price, atr_value, signal.clone(), in_cooldown);
            if matches!(action, Action::NoSignal) {
                action = self.try_enter_short(current_price, atr_value, &signal);
            }
        }

        if matches!(action, Action::NoSignal) && self.log_debug() {
            println!(
                "[CANDLE] No signal. Long: {}  Short: {}  Z={:.2}  EMA{}=${:.2}",
                signal.no_signal_reason,
                signal.short_no_signal_reason,
                z_score,
                self.config.ema_period,
                ema_value
            );
        }

        // ── build and send log entry ─────────────────────────────────────────
        let atr_pct: f64 = if current_price > 0.0 {
            atr_value / current_price * 100.0
        } else {
            0.0
        };
        let price_vs_sma_pct: f64 = if z_score_sma != 0.0 {
            (current_price - z_score_sma) / z_score_sma * 100.0
        } else {
            0.0
        };
        let price_vs_ema_pct: f64 = if ema_value != 0.0 {
            (current_price - ema_value) / ema_value * 100.0
        } else {
            0.0
        };

        let realized_pnl_usdt: f64 = self.reporter.trade_history.iter().map(|t| t.pnl_usdt).sum();

        let (unrealized_pnl_usdt, unrealized_pnl_pct) = if self.position.is_holding_asset {
            let u = self.wallet.total_value(current_price) - self.position.entry_equity;
            let p = if self.position.entry_equity > 0.0 {
                u / self.position.entry_equity * 100.0
            } else {
                0.0
            };
            (u, p)
        } else if self.position.is_short {
            let u: f64 = self.short_unrealized_pnl_usdt(current_price);
            let p: f64 = if self.position.short_margin_usdt > 0.0 {
                u / self.position.short_margin_usdt * 100.0
            } else {
                0.0
            };
            (u, p)
        } else {
            (0.0, 0.0)
        };

        let drawdown_pct: f64 = if self.reporter.peak_equity > 0.0 {
            (self.reporter.peak_equity - self.reporter.current_equity) / self.reporter.peak_equity * 100.0
        } else {
            0.0
        };

        let total_pnl_usdt: f64 = self.reporter.current_equity - self.initial_capital;
        let total_pnl_pct: f64 = if self.initial_capital > 0.0 {
            total_pnl_usdt / self.initial_capital * 100.0
        } else {
            0.0
        };

        let position_exposure_usdt: f64 = if self.position.is_holding_asset {
            self.wallet.crypto_balance.max(0.0) * current_price
        } else if self.position.is_short {
            self.position.short_margin_usdt
        } else {
            0.0
        };

        let position_type: PositionType = self.position_type();

        let log_entry = CandleLogEntry {
            bar_index: self.loop_count,
            timestamp: chrono::Utc::now().to_rfc3339(),
            symbol: self.symbol.clone(),
            phase: Phase::BarClose,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            current_price,

            z_score,
            z_score_period: self.config.z_score_period,
            z_score_sma,
            z_score_std_dev: z_score_std,
            ema_value,
            ema_period: self.config.ema_period,

            fast_sma: fast_value,
            slow_sma: slow_value,
            trend_ema: trend_value,
            macro_ema: macro_ema_value,
            vol_sma: vol_sma_value,
            rsi: rsi_value,
            atr: atr_value,
            atr_pct,
            price_vs_sma_pct,
            price_vs_ema_pct,

            bullish_cross: signal.bullish_cross,
            bearish_cross: signal.bearish_cross,
            bullish_trend: signal.bullish_trend,
            bullish_rsi: signal.bullish_rsi,
            strong_macro_trend: signal.strong_macro_trend,
            strong_volume: signal.strong_volume,
            short_entry: signal.short_entry,
            bearish_trend: signal.bearish_trend,

            fast_slow_diff: signal.fast_slow_diff,
            price_trend_diff: signal.price_trend_diff,

            position_type,
            is_holding: self.position.is_holding_asset,
            is_short: self.position.is_short,
            is_drawdown_stop_active: self.reporter.drawdown_stop_active,
            in_cooldown,
            cooldown_bars_remaining: self.position.cooldown_bars_remaining,
            short_cooldown_bars_remaining: self.position.short_cooldown_bars,
            bars_in_position: self.position.bars_in_position,

            wallet_usdt_balance: self.wallet.usdt_balance,
            wallet_crypto_balance: self.wallet.crypto_balance,
            position_exposure_usdt,
            entry_equity: self.position.entry_equity,
            equity: self.reporter.current_equity,
            realized_pnl_usdt,
            unrealized_pnl_usdt,
            unrealized_pnl_pct,
            pnl_usdt: total_pnl_usdt,
            pnl_pct: total_pnl_pct,
            peak_equity: self.reporter.peak_equity,
            drawdown_pct,
            trade_count: self.reporter.trade_history.len(),

            buy_price: self.position.buy_price,
            initial_stop_price: self.position.initial_stop_price,
            target_price: self.position.target_price,
            highest_price: self.position.highest_price,
            short_entry_price: self.position.short_entry_price,
            short_stop_price: self.position.short_stop_price,
            short_tp_price: self.position.short_tp_price,
            short_margin_usdt: self.position.short_margin_usdt,

            action,
            no_signal_reason: signal.no_signal_reason.clone(),
            short_no_signal_reason: signal.short_no_signal_reason.clone(),
        };

        let _ = self.log_tx.send(log_entry);

        // ── cooldown tickers ─────────────────────────────────────────────────
        if self.position.cooldown_bars_remaining > 0 {
            self.position.cooldown_bars_remaining -= 1;
        }
        if self.position.short_cooldown_bars > 0 {
            self.position.short_cooldown_bars -= 1;
        }

        // ── previous-bar snapshots ───────────────────────────────────────────
        self.previous_rsi = Some(rsi_value);
        self.previous_slow_sma = Some(slow_value);
        self.previous_fast_sma = Some(fast_value);
        self.previous_macro_ema = Some(trend_value);
    }
}
