use crate::models::candle::Candle;
use crate::models::candle_log_entry::CandleLogEntry;
use crate::models::data::{
    Action, BacktestResult, CryptoExchange, EquityPoint, ExitReason, Phase, PositionType,
    SignalState,
};
use crate::models::log_level::LogLevel;
use crate::models::strategy_config::StrategyConfig;
use crate::models::trade::Trade;
use crate::spot::spot_wallet::Wallet;
use crate::strategy::TradingStrategy;
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

    pub is_holding_asset: bool,
    pub buy_price: f64,
    pub highest_price: f64,
    pub entry_equity: f64,
    pub initial_capital: f64,
    pub bars_in_position: usize,
    pub cooldown_bars_remaining: usize,

    // --- Short simulation fields ---
    pub is_short: bool,
    pub short_entry_price: f64,
    pub short_stop_price: f64,
    pub short_tp_price: f64,
    pub short_panic_price: f64,
    pub short_cooldown_bars: usize,
    pub short_margin_usdt: f64,

    pub loop_count: usize,
    pub wallet: Wallet,
    pub symbol: String,
    pub trade_history: Vec<Trade>,
    pub equity_curve: Vec<EquityPoint>,
    pub current_equity: f64,
    pub peak_equity: f64,
    pub last_atr_value: f64,
    pub last_rsi_value: f64,
    pub drawdown_stop_active: bool,
    pub initial_stop_price: f64,
    pub panic_stop_price: f64,
    pub target_price: f64,
    pub price_history: VecDeque<f64>,
    pub ema_value: Option<f64>,
    pub config: StrategyConfig,
    pub log_tx: UnboundedSender<CandleLogEntry>,
    pub log_level: LogLevel,
    /// Latest live order-book imbalance (range -1..1). `None` until the live
    /// depth stream delivers a book; always `None` in backtests.
    pub latest_obi: Option<f64>,

    // --- Multi-timeframe macro filter ---
    /// EMA over higher-timeframe closes (aggregated from base candles).
    pub htf_ema: Ema,
    /// Count of base candles seen toward the current HTF candle.
    pub htf_bar_count: usize,
    /// Latest HTF trend verdict: `Some(true)` = bullish, `Some(false)` = bearish,
    /// `None` until the first HTF candle closes.
    pub htf_trend_bullish: Option<bool>,
}

impl SpotStrategy {
    pub fn new(
        initial_capital: f64,
        symbol: &str,
        log_tx: UnboundedSender<CandleLogEntry>,
        exchange: CryptoExchange,
        config: StrategyConfig,
        log_level: LogLevel,
    ) -> Self {
        let config = config.sanitized();

        Self {
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

            is_holding_asset: false,
            buy_price: 0.0,
            highest_price: 0.0,
            entry_equity: 0.0,
            initial_capital,
            bars_in_position: 0,
            cooldown_bars_remaining: 0,

            is_short: false,
            short_entry_price: 0.0,
            short_stop_price: 0.0,
            short_tp_price: 0.0,
            short_panic_price: 0.0,
            short_cooldown_bars: 0,
            short_margin_usdt: 0.0,

            loop_count: 0,
            wallet: Wallet::new(initial_capital, exchange),
            symbol: symbol.to_string(),
            trade_history: Vec::new(),
            equity_curve: vec![EquityPoint {
                bar_index: 0,
                equity: initial_capital,
                phase: Phase::BarClose,
            }],
            current_equity: initial_capital,
            peak_equity: initial_capital,
            last_atr_value: 0.0,
            last_rsi_value: 0.0,
            drawdown_stop_active: false,
            initial_stop_price: 0.0,
            panic_stop_price: 0.0,
            target_price: 0.0,
            price_history: VecDeque::with_capacity(config.warmup_period()),
            ema_value: None,
            htf_ema: Ema::new(config.mtf_ema_period).expect("Failed to create HTF EMA"),
            htf_bar_count: 0,
            htf_trend_bullish: None,
            config,
            log_tx,
            log_level,
            latest_obi: None,
        }
    }

    /// Aggregate base candles into a higher-timeframe candle and, once one
    /// completes, refresh the HTF trend verdict from its EMA. Called once per
    /// base candle so it also works in backtests.
    fn update_htf_trend(&mut self, close: f64) {
        self.htf_bar_count += 1;
        if self.htf_bar_count >= self.config.mtf_bars {
            self.htf_bar_count = 0;
            let htf_ema_value = self.htf_ema.next(close);
            self.htf_trend_bullish = Some(close > htf_ema_value);
        }
    }

    /// Multi-timeframe confirmation gate. Returns `true` when the filter is off
    /// (backtest/live unaffected) or no HTF candle has closed yet; otherwise a
    /// long needs a bullish HTF trend and a short needs a bearish one.
    fn mtf_confirms(&self, want_long: bool) -> bool {
        if !self.config.use_mtf_filter {
            return true;
        }
        match self.htf_trend_bullish {
            Some(bullish) => bullish == want_long,
            None => true,
        }
    }

    /// Order-book confirmation gate (LIVE only). Returns `true` when the filter
    /// is disabled (so backtests and unconfigured live runs are unaffected), or
    /// when the latest imbalance backs the requested side.
    fn order_book_confirms(&self, want_long: bool) -> bool {
        if !self.config.use_order_book_filter {
            return true;
        }
        match self.latest_obi {
            Some(obi) if want_long => obi >= self.config.obi_threshold,
            Some(obi) => obi <= -self.config.obi_threshold,
            None => false,
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    fn log_normal(&self) -> bool {
        matches!(self.log_level, LogLevel::Normal | LogLevel::Debug)
    }

    fn log_debug(&self) -> bool {
        matches!(self.log_level, LogLevel::Debug)
    }

    /// Push a completed trade into history.
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
        self.trade_history.push(Trade {
            entry_price,
            exit_price,
            pnl_pct: profit_percent,
            pnl_usdt,
            bars_held: bars,
            side: side.to_string(),
            equity_after_trade: self.current_equity,
            exit_reason: exit_reason.to_string(),
        });
    }

    /// Unrealised PnL of the open simulated short, in USDT.
    fn short_unrealized_pnl_usdt(&self, current_price: f64) -> f64 {
        if !self.is_short || self.short_entry_price <= 0.0 || self.short_margin_usdt <= 0.0 {
            return 0.0;
        }
        self.short_margin_usdt * (self.short_entry_price - current_price) / self.short_entry_price
    }

    /// Mark-to-market equity: wallet value + reserved short margin (still our
    /// collateral, was subtracted from the wallet on entry) + open short PnL.
    /// Without adding the reserved margin back, equity is understated for the whole
    /// time a short is open, producing a fake drawdown the size of the margin.
    fn mark_to_market_equity(&self, current_price: f64) -> f64 {
        self.wallet.total_value(current_price)
            + self.short_margin_usdt
            + self.short_unrealized_pnl_usdt(current_price)
    }

    fn update_equity_curve(&mut self, current_price: f64, phase: Phase) {
        self.current_equity = self.mark_to_market_equity(current_price);
        self.equity_curve.push(EquityPoint {
            bar_index: self.loop_count,
            equity: self.current_equity,
            phase,
        });
    }

    fn calculate_max_drawdown(&self) -> f64 {
        if self.equity_curve.is_empty() {
            return 0.0;
        }
        let mut peak = self.equity_curve[0].equity;
        let mut max_dd = 0.0;
        for pt in &self.equity_curve {
            if pt.equity > peak {
                peak = pt.equity;
            }
            let dd = (pt.equity - peak) / peak * 100.0;
            if dd < max_dd {
                max_dd = dd;
            }
        }
        max_dd
    }

    pub fn expectancy_per_trade_usdt(&self) -> f64 {
        let n = self.trade_history.len();
        if n == 0 {
            return 0.0;
        }
        self.trade_history.iter().map(|t| t.pnl_usdt).sum::<f64>() / n as f64
    }

    pub fn max_drawdown_pct(&self) -> f64 {
        self.calculate_max_drawdown()
    }

    pub fn final_equity(&self, last_price: f64) -> f64 {
        self.mark_to_market_equity(last_price)
    }

    pub fn drawdown_stop_active(&self) -> bool {
        self.drawdown_stop_active
    }

    fn refresh_drawdown_state(&mut self, current_price: f64) {
        self.current_equity = self.mark_to_market_equity(current_price);
        if self.current_equity > self.peak_equity {
            self.peak_equity = self.current_equity;
        }
        if self.peak_equity == 0.0 {
            return;
        }

        let drawdown_pct = (self.peak_equity - self.current_equity) / self.peak_equity * 100.0;
        if drawdown_pct > self.config.max_strategy_drawdown_pct {
            if !self.drawdown_stop_active && self.log_normal() {
                println!(
                    "[RISK] MAX_DRAWDOWN_STOP activated at {:.2}% drawdown. New entries blocked.",
                    drawdown_pct
                );
            }
            self.drawdown_stop_active = true;
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
        vol_val: f64,
        vol_sma_val: f64,
    ) -> SignalState {
        // Regime gate: a confirmed uptrend needs the fast EMA above the slow EMA AND
        // price above the slow EMA; downtrend is the mirror. This double check keeps
        // mean-reversion longs out of bear-market bounces (buying dips that keep
        // falling) and shorts out of bull-market pullbacks.
        let bullish_trend = ema_fast > ema_slow && current_price > ema_slow;
        let bearish_trend = ema_fast < ema_slow && current_price < ema_slow;

        // RSI guard: don't buy a dip that is already deeply overbought, and don't
        // short a rip that is already deeply oversold.
        let rsi_ok_long = rsi_val < 70.0;
        let rsi_ok_short = rsi_val > 30.0;
        let vol_ok = vol_val > vol_sma_val;

        // Optional regime filter: only fade dips in an uptrend / rips in a downtrend.
        let long_regime_ok = !self.config.use_trend_filter || bullish_trend;
        let short_regime_ok = !self.config.use_trend_filter || bearish_trend;

        // Mean-reversion entries: long when oversold (z low), short when overbought (z high).
        let bullish_cross = long_regime_ok && z_score < self.config.z_entry && rsi_ok_long;
        let bearish_cross = short_regime_ok && z_score > self.config.short_z_entry && rsi_ok_short;

        let no_signal_reason = if self.is_holding_asset {
            "HOLDING_LONG".into()
        } else if self.is_short {
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
                && !self.is_short
                && !self.is_holding_asset
                && bearish_cross
                && self.short_cooldown_bars == 0,
            bearish_trend,
            short_no_signal_reason: if self.is_short {
                "SHORT_ACTIVE".into()
            } else {
                "SHORT_READY".into()
            },
        }
    }

    // ── entry / exit helpers ─────────────────────────────────────────────────

    fn apply_exit_cooldown(&mut self, exit_reason: ExitReason, profit_percent: f64) {
        self.cooldown_bars_remaining = if profit_percent < 0.0 {
            self.config.loss_cooldown_bars
        } else {
            match exit_reason {
                ExitReason::TakeProfit => self.config.take_profit_cooldown_bars,
                _ => self.config.cooldown_bars,
            }
        };
    }

    /// Price distance of the stop that will trigger first: the tighter of the ATR
    /// stop and the hard panic stop. Used for position sizing so realised loss stays
    /// close to default_risk_per_trade_pct regardless of the coin's volatility.
    fn effective_stop_distance(&self, current_price: f64, atr_stop_distance: f64) -> f64 {
        let panic_distance = if self.config.panic_stop_loss_pct > 0.0 {
            current_price * self.config.panic_stop_loss_pct
        } else {
            f64::INFINITY
        };
        atr_stop_distance.min(panic_distance).max(f64::MIN_POSITIVE)
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
        if self.is_holding_asset || self.is_short || in_cooldown || self.drawdown_stop_active {
            return Action::NoSignal;
        }

        // Higher-timeframe trend gate: only buy dips when the macro trend is up.
        if !self.mtf_confirms(true) {
            return Action::NoSignal;
        }

        // LIVE-only microstructure confirmation: don't fade a dip into a wall of
        // sellers. No-op in backtests (filter off / no book).
        if !self.order_book_confirms(true) {
            return Action::NoSignal;
        }

        let stop_distance = atr_value * self.config.atr_multiplier;
        let risk_amount = self.current_equity * self.config.default_risk_per_trade_pct;
        // Size off the stop that will actually trigger first: the tighter of the ATR
        // stop and the hard panic stop. Otherwise, for coins whose ATR stop is wider
        // than panic_stop_loss_pct, the panic stop fires first and the realised loss
        // far exceeds default_risk_per_trade_pct.
        let risk_distance = self.effective_stop_distance(current_price, stop_distance);
        let position_usdt =
            (risk_amount / (risk_distance / current_price)).min(self.wallet.usdt_balance * 0.98);

        if position_usdt < self.wallet.filters.min_notional {
            return Action::NoSignal;
        }

        if let Some(executed_price) = self.wallet.buy(current_price, position_usdt, true) {
            self.is_holding_asset = true;
            self.buy_price = executed_price;
            self.highest_price = executed_price;
            self.initial_stop_price = executed_price - stop_distance;
            self.panic_stop_price = if self.config.panic_stop_loss_pct > 0.0 {
                executed_price * (1.0 - self.config.panic_stop_loss_pct)
            } else {
                0.0
            };
            self.target_price =
                executed_price + stop_distance * self.config.take_profit_r_multiplier;
            self.entry_equity = self.current_equity;
            self.bars_in_position = 0;
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
        if self.is_short || self.is_holding_asset || self.drawdown_stop_active {
            return Action::NoSignal;
        }

        // Higher-timeframe trend gate: only short rips when the macro trend is down.
        if !self.mtf_confirms(false) {
            return Action::NoSignal;
        }

        // LIVE-only microstructure confirmation: don't short a rip into a wall of
        // buyers. No-op in backtests (filter off / no book).
        if !self.order_book_confirms(false) {
            return Action::NoSignal;
        }

        let stop_distance = atr_value * self.config.short_stop_atr_mult;
        // Size off the tighter of the ATR stop and the hard panic stop (see
        // try_enter_position), and cap the reserved margin to the available wallet
        // balance so a tiny ATR can't reserve more collateral than we have.
        let risk_distance = self.effective_stop_distance(current_price, stop_distance);
        let margin_usdt = ((self.current_equity * self.config.default_risk_per_trade_pct)
            / (risk_distance / current_price))
            .min(self.wallet.usdt_balance * 0.98);

        if margin_usdt < self.wallet.filters.min_notional {
            return Action::NoSignal;
        }

        // Резервуємо маржу:
        self.wallet.usdt_balance -= margin_usdt;
        self.short_margin_usdt = margin_usdt;

        self.is_short = true;
        self.short_entry_price = current_price;
        self.short_stop_price = current_price + stop_distance;
        self.short_tp_price = current_price - atr_value * self.config.short_tp_atr_mult;
        self.short_panic_price = if self.config.panic_stop_loss_pct > 0.0 {
            current_price * (1.0 + self.config.panic_stop_loss_pct)
        } else {
            0.0
        };

        Action::ShortSell
    }

    fn execute_short_exit(&mut self, exit_price: f64, reason: ExitReason) -> Action {
        let pnl_pct = (self.short_entry_price - exit_price) / self.short_entry_price * 100.0;
        let pnl_usdt = self.short_margin_usdt * (pnl_pct / 100.0);

        self.wallet.usdt_balance += self.short_margin_usdt + pnl_usdt;

        self.push_trade(
            self.short_entry_price,
            exit_price,
            pnl_pct,
            pnl_usdt,
            self.bars_in_position,
            reason.as_str(),
            "SHORT",
        );
        self.reset_short_state();
        Action::CloseShort
    }

    fn try_close_short(&self, candle: &Candle, z_score: f64) -> Option<(f64, ExitReason)> {
        if !self.is_short {
            return None;
        }

        // 1. Stop-loss: whichever stop is closer to entry (lower price for a short)
        //    is hit first as price rises, so exit there and book that smaller loss.
        let stop_price = if self.short_panic_price > 0.0 {
            self.short_stop_price.min(self.short_panic_price)
        } else {
            self.short_stop_price
        };
        if candle.high >= stop_price {
            let reason = if self.short_panic_price > 0.0
                && self.short_panic_price <= self.short_stop_price
            {
                ExitReason::PanicStop
            } else {
                ExitReason::ShortStop
            };
            return Some((stop_price, reason));
        }
        // 3. Mean-reversion take profit: shorted a rip (z high), exit once z reverts
        //    back to the mean and we are in profit (price below entry).
        if z_score <= -self.config.z_exit && candle.close < self.short_entry_price {
            return Some((candle.close, ExitReason::ReversionExit));
        }
        // 4. ATR "runner" target as a backstop.
        if candle.low <= self.short_tp_price {
            return Some((self.short_tp_price, ExitReason::ShortTakeProfit));
        }
        None
    }

    /// Close a simulated short position.
    ///
    /// # Accounting (was the root cause of the -33% bug):
    ///
    ///  Opening  : wallet -= margin  (margin reserved)
    ///  Closing  : wallet += margin + pnl_usdt
    ///
    /// The original code only did `wallet += pnl_usdt`, so the margin was
    /// permanently destroyed on every short trade.  When mixing long + short
    /// the wallet drained ~`margin × trade_count` regardless of PnL sign.
    // fn execute_short_exit(
    //     &mut self,
    //     exit_price:    f64,
    //     reason:        ExitReason,
    //     current_price: f64,
    // ) -> Action {
    //     if !self.is_short { return Action::NoSignal; }
    //
    //     // ── save state before we mutate anything ───────────────────────────
    //     let entry_price   = self.short_entry_price;
    //     let margin        = self.short_margin_usdt;
    //     let bars          = self.bars_in_position;
    //     let saved_entry_eq = self.entry_equity;
    //
    //     // ── compute PnL ────────────────────────────────────────────────────
    //     let pnl_pct = if entry_price > 0.0 {
    //         (entry_price - exit_price) / entry_price * 100.0
    //     } else { 0.0 };
    //     let pnl_usdt = margin * (pnl_pct / 100.0);
    //
    //     // ── FIX 1: return margin AND pnl to wallet in one step ─────────────
    //     // Previously only `pnl_usdt` was added, so margin was silently lost.
    //     self.wallet.usdt_balance += margin + pnl_usdt;
    //
    //     if self.log_normal() {
    //         println!(
    //             "[EXIT-SHORT] {} at ${:.2}  PnL {:.2}% (${:.2})  Reason: {}",
    //             self.symbol, exit_price, pnl_pct, pnl_usdt, reason.as_str()
    //         );
    //     }
    //
    //     // ── FIX 2: reset short state BEFORE computing equity ───────────────
    //     // The old code called mark_to_market() while is_short was still true
    //     // AND after already crediting pnl_usdt to the wallet — that caused
    //     // triple-counting (wallet pnl + short_unrealized_pnl + manual add).
    //     self.reset_short_state();
    //
    //     // ── now mark_to_market is clean: just wallet balance + crypto ──────
    //     self.current_equity = self.mark_to_market_equity(current_price);
    //     self.equity_curve.push(EquityPoint {
    //         bar_index: self.loop_count,
    //         equity:    self.current_equity,
    //         phase:     Phase::PostSell,
    //     });
    //
    //     // ── record trade ───────────────────────────────────────────────────
    //     // Use the actual equity delta (more accurate than margin * pct when
    //     // fees or rounding are involved).
    //     let actual_pnl = self.current_equity - saved_entry_eq;
    //     self.push_trade(entry_price, exit_price, pnl_pct, actual_pnl, bars, reason.as_str(), "SHORT");
    //
    //     self.short_cooldown_bars = self.config.short_cooldown_bars;
    //     Action::CloseShort
    // }

    fn reset_short_state(&mut self) {
        self.is_short = false;
        self.short_entry_price = 0.0;
        self.short_stop_price = 0.0;
        self.short_tp_price = 0.0;
        self.short_panic_price = 0.0;
        self.short_margin_usdt = 0.0;
        self.bars_in_position = 0;
        self.entry_equity = 0.0;
    }

    // fn determine_exit_reason(
    //     &self,
    //     current_price: f64,
    //     candle:        Option<&Candle>,
    //     signal:        Option<&SignalState>,
    // ) -> Option<ExitReason> {
    //     if !self.is_holding_asset || self.buy_price == 0.0 || self.last_atr_value == 0.0 {
    //         return None;
    //     }
    //
    //     let check_low  = candle.map(|c| c.low).unwrap_or(current_price);
    //     let check_high = candle.map(|c| c.high).unwrap_or(current_price);
    //
    //     if check_low <= self.initial_stop_price {
    //         return Some(ExitReason::InitialStop);
    //     }
    //     if self.bars_in_position < self.config.min_bars_in_position {
    //         return None;
    //     }
    //
    //     if let Some(sig) = signal {
    //         if sig.bearish_cross
    //             && current_price > self.buy_price * (1.0 + self.config.min_profit_for_rsi_exit_pct)
    //         {
    //             return Some(ExitReason::BearishCross);
    //         }
    //     }
    //
    //     if self.target_price > 0.0 && check_high >= self.target_price {
    //         return Some(ExitReason::TakeProfit);
    //     }
    //
    //     None
    // }

    fn determine_exit_reason(
        &self,
        current_price: f64,
        candle: Option<&Candle>,
        signal: Option<&SignalState>,
    ) -> Option<ExitReason> {
        if !self.is_holding_asset || self.buy_price == 0.0 {
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
        let stop_price = self.initial_stop_price.max(self.panic_stop_price);
        if stop_price > 0.0 && check_low <= stop_price {
            let reason = if self.panic_stop_price > 0.0
                && self.panic_stop_price >= self.initial_stop_price
            {
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
            if signal.z_score >= self.config.z_exit && current_price > self.buy_price {
                return Some(ExitReason::ReversionExit);
            }
        }

        // ATR "runner" target as a backstop in case of strong continuation.
        if self.target_price > 0.0 && check_high >= self.target_price {
            return Some(ExitReason::TakeProfit);
        }

        // --- PRIORITY 3: trend-reversal exit (the only exit gated by min_bars) ----
        if self.bars_in_position < self.config.min_bars_in_position {
            return None;
        }
        if let Some(signal) = signal {
            if signal.bearish_cross
                && current_price > self.buy_price * (1.0 + self.config.min_profit_for_rsi_exit_pct)
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
            let profit_percent = if self.buy_price > 0.0 {
                (executed_price - self.buy_price) / self.buy_price * 100.0
            } else {
                0.0
            };

            if self.log_normal() {
                println!(
                    "[EXIT] {} {} {:?} at ${:.2}  PnL {:.2}%",
                    source, self.symbol, reason, executed_price, profit_percent
                );
            }

            // Save before reset
            let ep = self.buy_price;
            let bars = self.bars_in_position;
            let entry_eq = self.entry_equity;

            self.update_equity_curve(market_price, phase);
            self.apply_exit_cooldown(reason, profit_percent);
            self.reset_position_state();

            let pnl_usdt = self.current_equity - entry_eq;
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
        if self.is_holding_asset {
            let _ = self.execute_exit(
                current_price,
                current_price,
                ExitReason::EndOfData,
                Phase::PostSell,
                "BACKTEST_END",
            );
        }
        if self.is_short {
            let _ = self.execute_short_exit(current_price, ExitReason::EndOfData);
        }
    }

    fn reset_position_state(&mut self) {
        self.is_holding_asset = false;
        self.buy_price = 0.0;
        self.highest_price = 0.0;
        self.entry_equity = 0.0;
        self.bars_in_position = 0;
        self.initial_stop_price = 0.0;
        self.panic_stop_price = 0.0;
        self.target_price = 0.0;
    }

    fn position_type(&self) -> PositionType {
        if self.is_short {
            PositionType::Short
        } else if self.is_holding_asset {
            PositionType::Long
        } else {
            PositionType::None
        }
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
        let total_trades = self.trade_history.len();
        let initial_capital = self.initial_capital;
        let final_equity = self.mark_to_market_equity(last_price);
        let max_drawdown = self.calculate_max_drawdown();

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

        let winning: Vec<&Trade> = self
            .trade_history
            .iter()
            .filter(|t| t.pnl_usdt > 0.0)
            .collect();
        let losing: Vec<&Trade> = self
            .trade_history
            .iter()
            .filter(|t| t.pnl_usdt <= 0.0)
            .collect();
        let win_rate = winning.len() as f64 / total_trades as f64 * 100.0;
        let total_pnl_usdt: f64 = self.trade_history.iter().map(|t| t.pnl_usdt).sum();
        let gross_profit: f64 = winning.iter().map(|t| t.pnl_usdt).sum();
        let gross_loss: f64 = losing.iter().map(|t| t.pnl_usdt.abs()).sum();
        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        let returns: Vec<f64> = self
            .trade_history
            .iter()
            .map(|t| t.pnl_pct / 100.0)
            .collect();
        let n = returns.len() as f64;
        let mean_ret = returns.iter().sum::<f64>() / n;
        let variance = if n > 1.0 {
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

        let total_pnl_pct = if initial_capital > 0.0 {
            (final_equity - initial_capital) / initial_capital * 100.0
        } else {
            0.0
        };

        let recovery_factor = if max_drawdown.abs() > 0.0 {
            total_pnl_pct.abs() / max_drawdown.abs()
        } else {
            0.0
        };

        let mut max_consecutive_losses = 0usize;
        let mut streak = 0usize;
        for t in &self.trade_history {
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
        if self.trade_history.is_empty() {
            println!("No trades were executed.");
            return;
        }
        let long_n = self
            .trade_history
            .iter()
            .filter(|t| t.side == "LONG")
            .count();
        let short_n = self
            .trade_history
            .iter()
            .filter(|t| t.side == "SHORT")
            .count();
        let total = self.trade_history.len();
        let wins = self
            .trade_history
            .iter()
            .filter(|t| t.pnl_usdt > 0.0)
            .count();
        let total_pnl: f64 = self.trade_history.iter().map(|t| t.pnl_usdt).sum();

        let long_pnl: f64 = self
            .trade_history
            .iter()
            .filter(|t| t.side == "LONG")
            .map(|t| t.pnl_usdt)
            .sum();
        let short_pnl: f64 = self
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
        println!("Max drawdown : {:.2}%", self.calculate_max_drawdown());
        println!(
            "Final equity : ${:.2}",
            self.mark_to_market_equity(last_price)
        );
        println!("==============================\n");
    }
}

// ── TradingStrategy impl ─────────────────────────────────────────────────────

impl TradingStrategy for SpotStrategy {
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
    fn warmup_period(&self) -> usize {
        self.config.warmup_period()
    }

    fn final_equity(&self, current_price: f64) -> f64 {
        self.mark_to_market_equity(current_price)
    }
    fn total_trades(&self) -> usize {
        self.trade_history.len()
    }

    fn set_order_book_imbalance(&mut self, obi: f64) {
        self.latest_obi = Some(obi);
    }

    fn on_tick(&mut self, current_price: f64) {
        if self.is_holding_asset && self.last_atr_value > 0.0 {
            self.highest_price = self.highest_price.max(current_price);
            if let Some(reason) = self.determine_exit_reason(current_price, None, None) {
                let trigger = match reason {
                    ExitReason::PanicStop => self.panic_stop_price,
                    ExitReason::InitialStop => self.initial_stop_price,
                    ExitReason::TakeProfit => self.target_price,
                    _ => current_price,
                };
                let _ = self.execute_exit(trigger, current_price, reason, Phase::PostSell, "TICK");
            }
        }

        if self.is_short {
            if current_price >= self.short_stop_price {
                let _ = self.execute_short_exit(self.short_stop_price, ExitReason::ShortStop);
            } else if current_price <= self.short_tp_price {
                let _ = self.execute_short_exit(self.short_tp_price, ExitReason::ShortTakeProfit);
            }
        }
    }

    fn on_candle_close(&mut self, candle: &Candle) {
        let current_price = candle.close;
        // NOTE: loop_count is incremented inside `update_indicators` below; do not
        // increment it here too or the warm-up period completes twice as fast.

        // Update the higher-timeframe trend every base candle (also in backtests),
        // before any early return, so HTF aggregation stays aligned with the data.
        self.update_htf_trend(current_price);

        self.price_history.push_back(current_price);
        if self.price_history.len() > self.config.warmup_period() {
            self.price_history.pop_front();
        }
        if self.price_history.len() < self.config.z_score_period {
            return;
        }

        let ema_value = self.update_ema(current_price);
        let z_window = self.config.z_score_period;
        let z_score_sma = self.calculate_sma(z_window);
        let z_score_std = self.calculate_std_dev(z_window, z_score_sma);
        let z_score = if z_score_std > 0.0 {
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
        let in_cooldown = self.cooldown_bars_remaining > 0;

        // Regime uses the EMA cross: trend_value (fast EMA) vs macro_ema_value (slow EMA).
        let signal = self.compute_signal_state(
            current_price,
            z_score,
            trend_value,
            macro_ema_value,
            in_cooldown,
            rsi_value,
            candle.volume,
            vol_sma_value,
        );

        let mut action = Action::NoSignal;

        // ── long exit check ──────────────────────────────────────────────────
        if self.is_holding_asset {
            self.bars_in_position += 1;
            self.highest_price = self.highest_price.max(candle.high);
            if let Some(reason) =
                self.determine_exit_reason(current_price, Some(candle), Some(&signal))
            {
                let trigger = match reason {
                    ExitReason::PanicStop => self.panic_stop_price,
                    ExitReason::InitialStop => self.initial_stop_price,
                    ExitReason::TakeProfit => self.target_price,
                    _ => current_price,
                };
                action =
                    self.execute_exit(trigger, current_price, reason, Phase::PostSell, "CANDLE");
            }
        }

        // ── short exit check ─────────────────────────────────────────────────
        if self.is_short {
            self.bars_in_position += 1;
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
        let atr_pct = if current_price > 0.0 {
            atr_value / current_price * 100.0
        } else {
            0.0
        };
        let price_vs_sma_pct = if z_score_sma != 0.0 {
            (current_price - z_score_sma) / z_score_sma * 100.0
        } else {
            0.0
        };
        let price_vs_ema_pct = if ema_value != 0.0 {
            (current_price - ema_value) / ema_value * 100.0
        } else {
            0.0
        };

        let realized_pnl_usdt: f64 = self.trade_history.iter().map(|t| t.pnl_usdt).sum();

        let (unrealized_pnl_usdt, unrealized_pnl_pct) = if self.is_holding_asset {
            let u = self.wallet.total_value(current_price) - self.entry_equity;
            let p = if self.entry_equity > 0.0 {
                u / self.entry_equity * 100.0
            } else {
                0.0
            };
            (u, p)
        } else if self.is_short {
            let u = self.short_unrealized_pnl_usdt(current_price);
            let p = if self.short_margin_usdt > 0.0 {
                u / self.short_margin_usdt * 100.0
            } else {
                0.0
            };
            (u, p)
        } else {
            (0.0, 0.0)
        };

        let drawdown_pct = if self.peak_equity > 0.0 {
            (self.peak_equity - self.current_equity) / self.peak_equity * 100.0
        } else {
            0.0
        };

        let total_pnl_usdt = self.current_equity - self.initial_capital;
        let total_pnl_pct = if self.initial_capital > 0.0 {
            total_pnl_usdt / self.initial_capital * 100.0
        } else {
            0.0
        };

        let position_exposure_usdt = if self.is_holding_asset {
            self.wallet.crypto_balance.max(0.0) * current_price
        } else if self.is_short {
            self.short_margin_usdt
        } else {
            0.0
        };

        let position_type = self.position_type();

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
            is_holding: self.is_holding_asset,
            is_short: self.is_short,
            is_drawdown_stop_active: self.drawdown_stop_active,
            in_cooldown,
            cooldown_bars_remaining: self.cooldown_bars_remaining,
            short_cooldown_bars_remaining: self.short_cooldown_bars,
            bars_in_position: self.bars_in_position,

            wallet_usdt_balance: self.wallet.usdt_balance,
            wallet_crypto_balance: self.wallet.crypto_balance,
            position_exposure_usdt,
            entry_equity: self.entry_equity,
            equity: self.current_equity,
            realized_pnl_usdt,
            unrealized_pnl_usdt,
            unrealized_pnl_pct,
            pnl_usdt: total_pnl_usdt,
            pnl_pct: total_pnl_pct,
            peak_equity: self.peak_equity,
            drawdown_pct,
            trade_count: self.trade_history.len(),

            buy_price: self.buy_price,
            initial_stop_price: self.initial_stop_price,
            target_price: self.target_price,
            highest_price: self.highest_price,
            short_entry_price: self.short_entry_price,
            short_stop_price: self.short_stop_price,
            short_tp_price: self.short_tp_price,
            short_margin_usdt: self.short_margin_usdt,

            action,
            no_signal_reason: signal.no_signal_reason.clone(),
            short_no_signal_reason: signal.short_no_signal_reason.clone(),
        };

        let _ = self.log_tx.send(log_entry);

        // ── cooldown tickers ─────────────────────────────────────────────────
        if self.cooldown_bars_remaining > 0 {
            self.cooldown_bars_remaining -= 1;
        }
        if self.short_cooldown_bars > 0 {
            self.short_cooldown_bars -= 1;
        }

        // ── previous-bar snapshots ───────────────────────────────────────────
        self.previous_rsi = Some(rsi_value);
        self.previous_slow_sma = Some(slow_value);
        self.previous_fast_sma = Some(fast_value);
        self.previous_macro_ema = Some(trend_value);
    }
}
