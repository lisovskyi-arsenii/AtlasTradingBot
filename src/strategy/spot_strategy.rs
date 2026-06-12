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

    pub is_short: bool,
    pub short_entry_price: f64,
    pub short_stop_price: f64,
    pub short_tp_price: f64,
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
    pub target_price: f64,
    pub price_history: VecDeque<f64>,
    pub ema_value: Option<f64>,
    pub config: StrategyConfig,
    pub log_tx: UnboundedSender<CandleLogEntry>,
    pub log_level: LogLevel,
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
            target_price: 0.0,
            price_history: VecDeque::with_capacity(config.warmup_period()),
            ema_value: None,
            config,
            log_tx,
            log_level,
        }
    }

    fn log_normal(&self) -> bool {
        matches!(self.log_level, LogLevel::Normal | LogLevel::Debug)
    }

    fn log_debug(&self) -> bool {
        matches!(self.log_level, LogLevel::Debug)
    }

    fn push_trade(&mut self, executed_price: f64, profit_percent: f64, exit_reason: &str, side: &str) {
        let pnl_usdt = self.current_equity - self.entry_equity;
        self.trade_history.push(Trade {
            entry_price: if side == "LONG" { self.buy_price } else { self.short_entry_price },
            exit_price: executed_price,
            pnl_pct: profit_percent,
            pnl_usdt,
            bars_held: self.bars_in_position,
            side: side.to_string(),
            equity_after_trade: self.current_equity,
            exit_reason: exit_reason.to_string(),
        });
    }

    fn short_unrealized_pnl_usdt(&self, current_price: f64) -> f64 {
        if !self.is_short || self.short_entry_price <= 0.0 || self.short_margin_usdt <= 0.0 {
            return 0.0;
        }
        self.short_margin_usdt * (self.short_entry_price - current_price) / self.short_entry_price
    }

    fn mark_to_market_equity(&self, current_price: f64) -> f64 {
        self.wallet.total_value(current_price) + self.short_unrealized_pnl_usdt(current_price)
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

        let mut peak_equity = self.equity_curve[0].equity;
        let mut max_drawdown = 0.0;

        for point in &self.equity_curve {
            let equity = point.equity;
            if equity > peak_equity {
                peak_equity = equity;
            }
            let drawdown = (equity - peak_equity) / peak_equity * 100.0;
            if drawdown < max_drawdown {
                max_drawdown = drawdown;
            }
        }
        max_drawdown
    }

    pub fn total_trades(&self) -> usize {
        self.trade_history.len()
    }

    pub fn expectancy_per_trade_usdt(&self) -> f64 {
        let total_trades = self.trade_history.len();
        if total_trades == 0 {
            return 0.0;
        }
        self.trade_history.iter().map(|t| t.pnl_usdt).sum::<f64>() / total_trades as f64
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
                    "[RISK] MAX_DRAWDOWN_STOP activated at {:.2}% drawdown. New entries are blocked.",
                    drawdown_pct
                );
            }
            self.drawdown_stop_active = true;
        }
    }

    fn compute_signal_state(
        &self,
        current_price: f64,
        z_score: f64,
        ema_value: f64,
        in_cooldown: bool,
    ) -> SignalState {
        let bullish_trend = current_price > ema_value;
        let bullish_cross = z_score < self.config.z_entry;

        let no_signal_reason = if self.is_holding_asset {
            "HOLDING_POSITION".to_string()
        } else if self.is_short {
            "SHORT_ACTIVE".to_string()
        } else if self.drawdown_stop_active {
            "MAX_DRAWDOWN_STOP".to_string()
        } else if in_cooldown {
            "COOLDOWN".to_string()
        } else if !bullish_trend {
            "NOT_IN_UPTREND".to_string()
        } else if !bullish_cross {
            format!("Z_SCORE_TOO_HIGH {:.2}", z_score)
        } else {
            "READY".to_string()
        };

        let bearish_trend = current_price < ema_value;
        let short_entry = !self.is_short
            && !self.is_holding_asset
            && !self.drawdown_stop_active
            && self.short_cooldown_bars == 0
            && bearish_trend
            && z_score > self.config.short_z_entry;

        let short_no_signal_reason = if self.is_short {
            "SHORT_ACTIVE".to_string()
        } else if self.is_holding_asset {
            "LONG_ACTIVE".to_string()
        } else if self.drawdown_stop_active {
            "MAX_DRAWDOWN_STOP".to_string()
        } else if self.short_cooldown_bars > 0 {
            format!("SHORT_COOLDOWN {}", self.short_cooldown_bars)
        } else if !bearish_trend {
            "NOT_IN_DOWNTREND".to_string()
        } else if z_score <= self.config.short_z_entry {
            format!("Z_SCORE_TOO_LOW {:.2}", z_score)
        } else {
            "SHORT_READY".to_string()
        };

        SignalState {
            bullish_cross,
            bearish_cross: false,
            bullish_trend,
            bullish_rsi: false,
            strong_macro_trend: bullish_trend,
            strong_volume: true,
            fast_slow_diff: 0.0,
            price_trend_diff: 0.0,
            no_signal_reason,
            short_entry,
            bearish_trend,
            short_no_signal_reason,
        }
    }

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

    fn try_enter_position(
        &mut self,
        current_price: f64,
        atr_value: f64,
        signal: SignalState,
        in_cooldown: bool,
    ) -> Action {
        if self.is_holding_asset || in_cooldown || self.drawdown_stop_active {
            return Action::NoSignal;
        }
        if !signal.bearish_cross || !signal.bullish_trend {
            return Action::NoSignal;
        }
        if current_price <= 0.0 || atr_value <= 0.0 {
            return Action::NoSignal;
        }

        let stop_distance = atr_value * self.config.atr_multiplier;
        if stop_distance <= 0.0 {
            return Action::NoSignal;
        }

        let risk_amount = self.current_equity * self.config.default_risk_per_trade_pct;
        let stop_loss_pct = stop_distance / current_price;
        if stop_loss_pct <= 0.0 {
            return Action::NoSignal;
        }

        let mut position_usdt = risk_amount / stop_loss_pct;
        position_usdt = position_usdt.min(self.wallet.usdt_balance * 0.98);

        if position_usdt < self.wallet.filters.min_notional {
            return Action::NoSignal;
        }

        if self.log_normal() {
            println!(
                "[ENTRY-LONG] {} Price ${:.2}. Allocating ${:.2}",
                self.symbol, current_price, position_usdt
            );
        }

        if let Some(executed_price) = self.wallet.buy(current_price, position_usdt, false) {
            self.is_holding_asset = true;
            self.buy_price = executed_price;
            self.highest_price = executed_price;
            self.bars_in_position = 0;
            self.initial_stop_price = executed_price - stop_distance;
            self.target_price = executed_price + stop_distance * self.config.take_profit_r_multiplier;
            self.update_equity_curve(current_price, Phase::PostBuy);
            self.entry_equity = self.current_equity;
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
        if current_price <= 0.0 || atr_value <= 0.0 {
            return Action::NoSignal;
        }

        let stop_distance = atr_value * self.config.short_stop_atr_mult;
        let risk_amount = self.current_equity * self.config.default_risk_per_trade_pct;
        let stop_loss_pct = stop_distance / current_price;
        if stop_loss_pct <= 0.0 {
            return Action::NoSignal;
        }

        let margin_usdt = (risk_amount / stop_loss_pct).min(self.wallet.usdt_balance * 0.98);
        if margin_usdt < self.wallet.filters.min_notional {
            return Action::NoSignal;
        }

        self.is_short = true;
        self.short_entry_price = current_price;
        self.short_stop_price = current_price + stop_distance;
        self.short_tp_price = current_price - atr_value * self.config.short_tp_atr_mult;
        self.short_margin_usdt = margin_usdt;
        self.bars_in_position = 0;
        self.entry_equity = self.current_equity;

        if self.log_normal() {
            println!(
                "[ENTRY-SHORT] {} Entry ${:.2} Stop ${:.2} TP ${:.2} Margin ${:.2}",
                self.symbol, current_price, self.short_stop_price, self.short_tp_price, margin_usdt
            );
        }

        Action::ShortSell
    }

    fn try_close_short(&mut self, candle: &Candle) -> Option<(f64, ExitReason)> {
        if !self.is_short {
            return None;
        }

        let check_high = candle.high;
        let check_low = candle.low;

        if check_high >= self.short_stop_price {
            return Some((self.short_stop_price, ExitReason::ShortStop));
        }
        if check_low <= self.short_tp_price {
            return Some((self.short_tp_price, ExitReason::ShortTakeProfit));
        }
        None
    }

    fn execute_short_exit(&mut self, exit_price: f64, reason: ExitReason, current_price: f64) -> Action {
        let pnl_pct = if self.short_entry_price > 0.0 {
            (self.short_entry_price - exit_price) / self.short_entry_price * 100.0
        } else {
            0.0
        };

        let pnl_usdt = self.short_margin_usdt * (pnl_pct / 100.0);
        self.wallet.usdt_balance += pnl_usdt;
        self.current_equity = self.mark_to_market_equity(current_price) + pnl_usdt;

        if self.log_normal() {
            println!(
                "[EXIT-SHORT] {} at ${:.2}. PnL {:.2}% ${:.2} Reason: {:?}",
                self.symbol, exit_price, pnl_pct, pnl_usdt, reason
            );
        }

        self.update_equity_curve(current_price, Phase::PostSell);
        self.push_trade(exit_price, pnl_pct, reason.as_str(), "SHORT");
        self.short_cooldown_bars = self.config.short_cooldown_bars;
        self.reset_short_state();
        Action::CloseShort
    }

    fn reset_short_state(&mut self) {
        self.is_short = false;
        self.short_entry_price = 0.0;
        self.short_stop_price = 0.0;
        self.short_tp_price = 0.0;
        self.short_margin_usdt = 0.0;
        self.bars_in_position = 0;
        self.entry_equity = 0.0;
    }

    fn determine_exit_reason(
        &self,
        current_price: f64,
        candle: Option<&Candle>,
        signal: Option<&SignalState>,
    ) -> Option<ExitReason> {
        if !self.is_holding_asset || self.buy_price == 0.0 || self.last_atr_value == 0.0 {
            return None;
        }

        let check_low = candle.map(|c| c.low).unwrap_or(current_price);
        let check_high = candle.map(|c| c.high).unwrap_or(current_price);

        if check_low <= self.initial_stop_price {
            return Some(ExitReason::InitialStop);
        }
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

        if self.target_price > 0.0 && check_high >= self.target_price {
            return Some(ExitReason::TakeProfit);
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
                    "[EXIT] {} {} {:?} at ${:.2}. PnL {:.2}%",
                    source, self.symbol, reason, executed_price, profit_percent
                );
            }

            self.update_equity_curve(market_price, phase);
            self.push_trade(executed_price, profit_percent, reason.as_str(), "LONG");
            self.apply_exit_cooldown(reason, profit_percent);
            self.reset_position_state();
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
            let _ = self.execute_short_exit(current_price, ExitReason::EndOfData, current_price);
        }
    }

    fn reset_position_state(&mut self) {
        self.is_holding_asset = false;
        self.buy_price = 0.0;
        self.highest_price = 0.0;
        self.entry_equity = 0.0;
        self.bars_in_position = 0;
        self.initial_stop_price = 0.0;
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

    fn calculate_sma(&self, period: usize) -> f64 {
        let len = self.price_history.len();
        if len < period {
            return 0.0;
        }
        let mut sum = 0.0;
        for i in (len - period)..len {
            sum += self.price_history[i];
        }
        sum / period as f64
    }

    fn calculate_std_dev(&self, period: usize, sma: f64) -> f64 {
        let len = self.price_history.len();
        if len < period {
            return 0.0;
        }
        let mut variance_sum = 0.0;
        for i in (len - period)..len {
            variance_sum += (self.price_history[i] - sma).powi(2);
        }
        (variance_sum / period as f64).sqrt()
    }

    fn update_ema(&mut self, current_price: f64) -> f64 {
        let period = self.config.ema_period as f64;
        let k = 2.0 / (period + 1.0);
        let new_ema = match self.ema_value {
            Some(prev) => current_price * k + prev * (1.0 - k),
            None => current_price,
        };
        self.ema_value = Some(new_ema);
        new_ema
    }

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

        let winning_trades: Vec<&Trade> = self.trade_history.iter().filter(|t| t.pnl_usdt > 0.0).collect();
        let losing_trades: Vec<&Trade> = self.trade_history.iter().filter(|t| t.pnl_usdt <= 0.0).collect();
        let win_rate = winning_trades.len() as f64 / total_trades as f64 * 100.0;
        let total_pnl_usdt: f64 = self.trade_history.iter().map(|t| t.pnl_usdt).sum();
        let gross_profit: f64 = winning_trades.iter().map(|t| t.pnl_usdt).sum();
        let gross_loss: f64 = losing_trades.iter().map(|t| t.pnl_usdt.abs()).sum();
        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        let returns: Vec<f64> = self.trade_history.iter().map(|t| t.pnl_pct / 100.0).collect();
        let n = returns.len() as f64;
        let mean_return = returns.iter().sum::<f64>() / n;
        let variance = if n > 1.0 {
            returns.iter().map(|r| (r - mean_return).powi(2)).sum::<f64>() / (n - 1.0)
        } else {
            0.0
        };
        let std_dev = variance.sqrt();
        let sharpe_ratio = if std_dev > 0.0 {
            mean_return / std_dev * (total_trades as f64).sqrt()
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
        let mut current_streak = 0usize;
        for trade in &self.trade_history {
            if trade.pnl_usdt < 0.0 {
                current_streak += 1;
                if current_streak > max_consecutive_losses {
                    max_consecutive_losses = current_streak;
                }
            } else {
                current_streak = 0;
            }
        }

        let avg_pnl_usdt = total_pnl_usdt / total_trades as f64;

        BacktestResult {
            csv_file: csv_file.to_string(),
            symbol: self.symbol.clone(),
            total_trades,
            win_rate,
            profit_factor,
            total_pnl_pct,
            total_pnl_usdt,
            avg_pnl_usdt,
            max_drawdown_pct: max_drawdown,
            sharpe_ratio,
            recovery_factor,
            max_consecutive_losses: max_consecutive_losses,
            final_equity,
            initial_capital,
        }
    }

    pub fn print_backtest_summary(&self, last_price: f64) {
        let total_trades = self.trade_history.len();
        if total_trades == 0 {
            println!("No trades were executed.");
            return;
        }

        let long_trades: Vec<&Trade> = self.trade_history.iter().filter(|t| t.side == "LONG").collect();
        let short_trades: Vec<&Trade> = self.trade_history.iter().filter(|t| t.side == "SHORT").collect();
        let winning = self.trade_history.iter().filter(|t| t.pnl_usdt > 0.0).count();
        let win_rate = winning as f64 / total_trades as f64 * 100.0;
        let total_pnl: f64 = self.trade_history.iter().map(|t| t.pnl_usdt).sum();
        let max_dd = self.calculate_max_drawdown();
        let final_eq = self.mark_to_market_equity(last_price);

        println!("\n====== BACKTEST SUMMARY ======");
        println!(
            "Total trades : {}  (Long: {}  Short: {})",
            total_trades,
            long_trades.len(),
            short_trades.len()
        );
        println!("Win rate     : {:.2}%", win_rate);
        println!("Total PnL    : ${:.2}", total_pnl);
        println!("Max drawdown : {:.2}%", max_dd);
        println!("Final equity : ${:.2}", final_eq);
        println!("==============================\n");
    }
}

impl TradingStrategy for SpotStrategy {
    fn loop_count(&mut self) -> &mut usize { &mut self.loop_count }
    fn slow_sma(&mut self) -> &mut Sma { &mut self.slow_sma }
    fn fast_sma(&mut self) -> &mut Sma { &mut self.fast_sma }
    fn trend_ema(&mut self) -> &mut Ema { &mut self.trend_ema }
    fn rsi(&mut self) -> &mut Rsi { &mut self.rsi }
    fn atr(&mut self) -> &mut Atr { &mut self.atr }
    fn macro_ema(&mut self) -> &mut Ema { &mut self.macro_ema }
    fn vol_sma(&mut self) -> &mut Sma { &mut self.vol_sma }
    fn warmup_period(&self) -> usize { self.config.warmup_period() }

    fn final_equity(&self, current_price: f64) -> f64 {
        self.mark_to_market_equity(current_price)
    }

    fn total_trades(&self) -> usize {
        self.trade_history.len()
    }

    fn on_tick(&mut self, current_price: f64) {
        if self.is_holding_asset && self.last_atr_value > 0.0 {
            self.highest_price = self.highest_price.max(current_price);
            if let Some(reason) = self.determine_exit_reason(current_price, None, None) {
                let trigger_price = match reason {
                    ExitReason::InitialStop => self.initial_stop_price,
                    ExitReason::TakeProfit => self.target_price,
                    _ => current_price,
                };
                let _ = self.execute_exit(trigger_price, current_price, reason, Phase::PostSell, "TICK");
            }
        }

        if self.is_short {
            if current_price >= self.short_stop_price {
                let _ = self.execute_short_exit(self.short_stop_price, ExitReason::ShortStop, current_price);
            } else if current_price <= self.short_tp_price {
                let _ = self.execute_short_exit(self.short_tp_price, ExitReason::ShortTakeProfit, current_price);
            }
        }
    }

    fn on_candle_close(&mut self, candle: &Candle) {
        let current_price = candle.close;
        self.loop_count += 1;

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
        let z_score_std_dev = self.calculate_std_dev(z_window, z_score_sma);
        let z_score = if z_score_std_dev > 0.0 {
            (current_price - z_score_sma) / z_score_std_dev
        } else {
            0.0
        };

        let indicators = self.update_indicators(candle.clone());
        self.update_equity_curve(current_price, Phase::BarClose);
        self.refresh_drawdown_state(current_price);

        let (slow_value, fast_value, trend_value, rsi_value, atr_value, _macro_ema_value, _vol_sma_value) =
            match indicators {
                Some(values) => values,
                None => return,
            };

        self.last_atr_value = atr_value;
        self.last_rsi_value = rsi_value;
        let in_cooldown = self.cooldown_bars_remaining > 0;

        let signal = self.compute_signal_state(current_price, z_score, ema_value, in_cooldown);
        let mut action = Action::NoSignal;

        if self.is_holding_asset {
            self.bars_in_position += 1;
            self.highest_price = self.highest_price.max(candle.high);
            if let Some(reason) = self.determine_exit_reason(current_price, Some(candle), Some(&signal)) {
                let trigger = match reason {
                    ExitReason::InitialStop => self.initial_stop_price,
                    ExitReason::TakeProfit => self.target_price,
                    _ => current_price,
                };
                action = self.execute_exit(trigger, current_price, reason, Phase::PostSell, "CANDLE");
            }
        }

        if self.is_short {
            self.bars_in_position += 1;
            if let Some((exit_price, reason)) = self.try_close_short(candle) {
                action = self.execute_short_exit(exit_price, reason, current_price);
            }
        }

        if matches!(action, Action::NoSignal) {
            action = self.try_enter_position(current_price, atr_value, signal.clone(), in_cooldown);
            if matches!(action, Action::NoSignal) {
                action = self.try_enter_short(current_price, atr_value, &signal);
            }
        }

        if matches!(action, Action::NoSignal) && self.log_debug() {
            println!(
                "[CANDLE] No signals. Long: {}. Short: {}. Z-Score {:.2}, EMA{}: ${:.2}",
                signal.no_signal_reason,
                signal.short_no_signal_reason,
                z_score,
                self.config.ema_period,
                ema_value
            );
        }

        let atr_pct = if current_price > 0.0 { atr_value / current_price * 100.0 } else { 0.0 };
        let price_vs_sma_pct = if z_score_sma != 0.0 { (current_price - z_score_sma) / z_score_sma * 100.0 } else { 0.0 };
        let price_vs_ema_pct = if ema_value != 0.0 { (current_price - ema_value) / ema_value * 100.0 } else { 0.0 };

        let realized_pnl_usdt: f64 = self.trade_history.iter().map(|t| t.pnl_usdt).sum();

        let (unrealized_pnl_usdt, unrealized_pnl_pct) = if self.is_holding_asset {
            let pnl_u = self.wallet.total_value(current_price) - self.entry_equity;
            let pnl_p = if self.entry_equity > 0.0 { pnl_u / self.entry_equity * 100.0 } else { 0.0 };
            (pnl_u, pnl_p)
        } else if self.is_short {
            let pnl_u = self.short_margin_usdt * (self.short_entry_price - current_price) / self.short_entry_price;
            let pnl_p = if self.short_margin_usdt > 0.0 { pnl_u / self.short_margin_usdt * 100.0 } else { 0.0 };
            (pnl_u, pnl_p)
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

        let position_type = if self.is_short {
            PositionType::Short
        } else if self.is_holding_asset {
            PositionType::Long
        } else {
            PositionType::None
        };

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
            z_score_std_dev,
            ema_value,
            ema_period: self.config.ema_period,

            fast_sma: fast_value,
            slow_sma: slow_value,
            trend_ema: trend_value,
            macro_ema: _macro_ema_value,
            vol_sma: _vol_sma_value,
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

        if self.cooldown_bars_remaining > 0 {
            self.cooldown_bars_remaining -= 1;
        }
        if self.short_cooldown_bars > 0 {
            self.short_cooldown_bars -= 1;
        }

        self.previous_rsi = Some(rsi_value);
        self.previous_slow_sma = Some(slow_value);
        self.previous_fast_sma = Some(fast_value);
        self.previous_macro_ema = Some(trend_value);
    }
}
