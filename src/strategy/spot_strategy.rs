use crate::models::candle::Candle;
use crate::models::candle_log_entry::CandleLogEntry;
use crate::models::data::{
    Action, BacktestResult, CryptoExchange, EquityPoint, ExitReason, Phase, SignalState, ATR_PERIOD, FAST_PERIOD, MACRO_PERIOD,
    RSI_PERIOD, SLOW_PERIOD, TREND_PERIOD, VOL_SMA_PERIOD,
};
use crate::models::trade::Trade;
use crate::spot::spot_wallet::Wallet;
use crate::strategy::{TradingStrategy, ATR_MULTIPLIER, BREAKEVEN_LOCK_PCT, BREAKEVEN_TRIGGER_R, COOLDOWN_BARS, DEFAULT_RISK_PER_TRADE_PCT, LOSS_COOLDOWN_BARS, MAX_STRATEGY_DRAWDOWN_PCT, MIN_BARS_IN_POSITION, MIN_PROFIT_FOR_RSI_EXIT_PCT, RSI_DIP_LEVEL, RSI_EXIT_LEVEL, TAKE_PROFIT_COOLDOWN_BARS, TAKE_PROFIT_R_MULTIPLIER};
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

    pub loop_count: usize,
    pub wallet: Wallet,
    pub symbol: String,

    pub trade_history: Vec<Trade>,
    pub equity_curve: Vec<EquityPoint>,
    pub current_equity: f64,
    pub peak_equity: f64,

    pub last_atr_value: f64,
    pub last_rsi_value: f64,

    pub risk_per_trade_pct: f64,
    pub max_strategy_drawdown_pct: f64,
    pub drawdown_stop_active: bool,
    pub initial_stop_price: f64,
    pub target_price: f64,

    pub log_tx: UnboundedSender<CandleLogEntry>,
}

impl SpotStrategy {
    pub fn new(
        initial_capital: f64,
        symbol: &str,
        log_tx: UnboundedSender<CandleLogEntry>,
        exchange: CryptoExchange,
    ) -> Self {
        Self {
            slow_sma: Sma::new(SLOW_PERIOD).expect("Failed to create slow SMA"),
            fast_sma: Sma::new(FAST_PERIOD).expect("Failed to create fast SMA"),
            trend_ema: Ema::new(TREND_PERIOD).expect("Failed to create trend EMA"),
            rsi: Rsi::new(RSI_PERIOD).expect("Failed to create RSI"),
            atr: Atr::new(ATR_PERIOD).expect("Failed to create ATR"),
            macro_ema: Ema::new(MACRO_PERIOD).expect("Failed to create macro EMA"),
            vol_sma: Sma::new(VOL_SMA_PERIOD).expect("Failed to create volume SMA"),

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

            risk_per_trade_pct: DEFAULT_RISK_PER_TRADE_PCT,
            max_strategy_drawdown_pct: MAX_STRATEGY_DRAWDOWN_PCT,
            drawdown_stop_active: false,
            initial_stop_price: 0.0,
            target_price: 0.0,

            log_tx,
        }
    }

    fn push_trade(&mut self, executed_price: f64, profit_percent: f64, exit_reason: &str) {
        let pnl_usdt = self.current_equity - self.entry_equity;

        self.trade_history.push(Trade {
            entry_price: self.buy_price,
            exit_price: executed_price,
            pnl_pct: profit_percent,
            pnl_usdt,
            bars_held: self.bars_in_position,
            side: "LONG".to_string(),
            equity_after_trade: self.current_equity,
            exit_reason: exit_reason.to_string(),
        });
    }

    fn update_equity_curve(&mut self, current_price: f64, phase: Phase) {
        self.current_equity = self.wallet.total_value(current_price);
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

            let drawdown = ((equity - peak_equity) / peak_equity) * 100.0;
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
        self.wallet.total_value(last_price)
    }

    pub fn drawdown_stop_active(&self) -> bool {
        self.drawdown_stop_active
    }

    fn refresh_drawdown_state(&mut self, current_price: f64) {
        self.current_equity = self.wallet.total_value(current_price);

        if self.current_equity > self.peak_equity {
            self.peak_equity = self.current_equity;
        }

        if self.peak_equity <= 0.0 {
            return;
        }

        let drawdown_pct =
            ((self.peak_equity - self.current_equity) / self.peak_equity) * 100.0;

        if drawdown_pct >= self.max_strategy_drawdown_pct {
            if !self.drawdown_stop_active {
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
        trend_value: f64,
        rsi_value: f64,
        prev_rsi: f64,
        macro_ema_value: f64,
        in_cooldown: bool,
        fast_value: f64,
        slow_value: f64,
        strong_volume: bool,
    ) -> SignalState {
        let macro_rising = self
            .previous_macro_ema
            .map(|prev| macro_ema_value > prev)
            .unwrap_or(false);

        let bullish_trend = current_price > macro_ema_value && macro_rising;

        // Buy on confirmed reversal: RSI was in dip zone and now exited upward
        let bullish_cross = prev_rsi <= RSI_DIP_LEVEL && rsi_value > RSI_DIP_LEVEL;

        // Exit when RSI becomes overbought / overextended
        let bearish_cross = rsi_value >= RSI_EXIT_LEVEL;

        // RSI is considered bullish in the 40-75 range (not oversold, not overbought)
        let bullish_rsi = rsi_value > 40.0 && rsi_value < 75.0;

        // Strong macro trend: price well above macro EMA and macro rising
        let strong_macro_trend = macro_rising && ((current_price - macro_ema_value) / macro_ema_value) > 0.01;

        // Fast/slow SMA diff: how far fast is above slow (momentum)
        let fast_slow_diff = if slow_value > 0.0 {
            ((fast_value - slow_value) / slow_value) * 100.0
        } else {
            0.0
        };

        let price_trend_diff = current_price - trend_value;

        let no_signal_reason = if self.is_holding_asset {
            "HOLDING_POSITION".to_string()
        } else if self.drawdown_stop_active {
            "MAX_DRAWDOWN_STOP".to_string()
        } else if in_cooldown {
            "COOLDOWN".to_string()
        } else if !bullish_trend {
            "NOT_IN_UPTREND".to_string()
        } else if !bullish_cross {
            "WAITING_FOR_DIP".to_string()
        } else {
            "READY".to_string()
        };

        SignalState {
            bullish_cross, bearish_cross, bullish_trend, bullish_rsi,
            strong_macro_trend, strong_volume, fast_slow_diff, price_trend_diff, no_signal_reason,
        }
    }

    fn apply_exit_cooldown(&mut self, exit_reason: ExitReason, profit_percent: f64) {
        self.cooldown_bars_remaining = if profit_percent < 0.0 {
            LOSS_COOLDOWN_BARS
        } else {
            match exit_reason {
                ExitReason::TakeProfit => TAKE_PROFIT_COOLDOWN_BARS,
                _ => COOLDOWN_BARS,
            }
        };
    }

    fn try_enter_position(
        &mut self,
        current_price: f64,
        atr_value: f64,
        signal: &SignalState,
        in_cooldown: bool,
    ) -> Action {
        if self.is_holding_asset || in_cooldown || self.drawdown_stop_active {
            return Action::NoSignal;
        }

        // Вхід ТІЛЬКИ якщо тренд сильний і відкат підтверджено
        if !signal.bullish_cross || !signal.bullish_trend {
            return Action::NoSignal;
        }

        if current_price <= 0.0 || atr_value <= 0.0 {
            return Action::NoSignal;
        }

        let stop_distance = atr_value * ATR_MULTIPLIER;
        if stop_distance <= 0.0 {
            return Action::NoSignal;
        }

        let risk_amount = self.current_equity * self.risk_per_trade_pct;
        let stop_loss_pct = stop_distance / current_price;

        if stop_loss_pct <= 0.0 {
            return Action::NoSignal;
        }

        let mut position_usdt = risk_amount / stop_loss_pct;
        position_usdt = position_usdt.min(self.wallet.usdt_balance * 0.98);

        if position_usdt > self.wallet.usdt_balance {
            position_usdt = self.wallet.usdt_balance;
        }

        if position_usdt < self.wallet.filters.min_notional {
            return Action::NoSignal;
        }

        println!(
            "[CANDLE] BUY SIGNAL {}. Price: ${:.2}. Allocating: ${:.2}",
            self.symbol, current_price, position_usdt
        );

        if let Some(executed_price) = self.wallet.buy(current_price, position_usdt, false) {
            self.is_holding_asset = true;
            self.buy_price = executed_price;
            self.highest_price = executed_price;
            self.bars_in_position = 0;

            self.initial_stop_price = executed_price - stop_distance;
            self.target_price = executed_price + (stop_distance * TAKE_PROFIT_R_MULTIPLIER);

            self.update_equity_curve(current_price, Phase::PostBuy);
            self.entry_equity = self.current_equity;

            return Action::Buy;
        }

        Action::NoSignal
    }

    fn determine_exit_reason(
        &self,
        current_price: f64,
        candle: Option<&Candle>,
        signal: Option<&SignalState>,
    ) -> Option<ExitReason> {
        if !self.is_holding_asset || self.buy_price <= 0.0 || self.last_atr_value <= 0.0 {
            return None;
        }

        let check_low = candle.map(|c| c.low).unwrap_or(current_price);
        let check_high = candle.map(|c| c.high).unwrap_or(current_price);

        // InitialStop завжди дозволений — це ризик-контроль
        if check_low <= self.initial_stop_price {
            return Some(ExitReason::InitialStop);
        }

        // Не виходимо по сигналу/TP допоки позиція не "подорослішає"
        if self.bars_in_position < MIN_BARS_IN_POSITION {
            return None;
        }

        // RSI-вихід тільки при сильній перекупленості (RSI >= 72, було 65)
        // Це дає прибутковим угодам більше часу дійти до TP
        if let Some(signal) = signal {
            if signal.bearish_cross
                && current_price >= self.buy_price * (1.0 + MIN_PROFIT_FOR_RSI_EXIT_PCT)
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
                ((executed_price - self.buy_price) / self.buy_price) * 100.0
            } else {
                0.0
            };

            println!(
                "[{}] {} EXIT {} at ${:.2}. PnL: {:.2}%",
                source,
                reason.as_str(),
                self.symbol,
                executed_price,
                profit_percent
            );

            self.update_equity_curve(market_price, phase);
            self.push_trade(executed_price, profit_percent, reason.as_str());
            self.apply_exit_cooldown(reason, profit_percent);
            self.reset_position_state();

            return Action::Sell;
        }

        Action::NoSignal
    }

    pub fn finalize_backtest(&mut self, current_price: f64) {
        if !self.is_holding_asset {
            return;
        }

        let _ = self.execute_exit(
            current_price,
            current_price,
            ExitReason::EndOfData,
            Phase::PostSell,
            "BACKTEST_END",
        );
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

    fn build_log_entry(
        &self,
        candle: &Candle,
        slow_value: f64,
        fast_value: f64,
        trend_value: f64,
        rsi_value: f64,
        atr_value: f64,
        signal: &SignalState,
        action: Action,
    ) -> CandleLogEntry {
        CandleLogEntry {
            timestamp: chrono::Utc::now().to_rfc3339(),
            symbol: self.symbol.clone(),
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            fast_sma: fast_value,
            slow_sma: slow_value,
            trend_ema: trend_value,
            rsi: rsi_value,
            atr: atr_value,
            bullish_cross: signal.bullish_cross,
            bearish_cross: signal.bearish_cross,
            bullish_trend: signal.bullish_trend,
            bullish_rsi: signal.bullish_rsi,
            fast_slow_diff: signal.fast_slow_diff,
            price_trend_diff: signal.price_trend_diff,
            is_holding: self.is_holding_asset,
            cooldown_bars_remaining: self.cooldown_bars_remaining,
            action,
            no_signal_reason: signal.no_signal_reason.clone(),
        }
    }

    /// Computes a detailed BacktestResult struct with advanced metrics:
    /// - Sharpe Ratio (annualized, assuming risk-free rate = 0)
    /// - Recovery Factor (net profit / max drawdown)
    /// - Max Consecutive Losses
    pub fn compute_backtest_result(&self, last_price: f64, csv_file: &str) -> BacktestResult {
        let total_trades = self.trade_history.len();
        let initial_capital = self.initial_capital;
        let final_equity = self.wallet.total_value(last_price);
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

        let winning_trades: Vec<&Trade> =
            self.trade_history.iter().filter(|t| t.pnl_usdt > 0.0).collect();
        let losing_trades: Vec<&Trade> =
            self.trade_history.iter().filter(|t| t.pnl_usdt < 0.0).collect();

        let win_count = winning_trades.len();
        let _loss_count = losing_trades.len();
        let win_rate = (win_count as f64 / total_trades as f64) * 100.0;

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

        // Sharpe Ratio based on trade returns (not daily returns)
        // We use the pnl_pct of each trade as return samples
        let returns: Vec<f64> = self.trade_history.iter().map(|t| t.pnl_pct / 100.0).collect();
        let n = returns.len() as f64;
        let mean_return = returns.iter().sum::<f64>() / n;

        let variance = if n > 1.0 {
            returns.iter().map(|r| (r - mean_return).powi(2)).sum::<f64>() / (n - 1.0)
        } else {
            0.0
        };

        let std_dev = variance.sqrt();
        // Annualize: assume ~17,520 15min bars/year = 365*24*4
        // For trade-based Sharpe, we approximate trades/bars ratio
        let sharpe_ratio = if std_dev > 0.0 {
            (mean_return / std_dev) * (total_trades as f64).sqrt()
        } else {
            0.0
        };

        // Recovery Factor
        let total_pnl_pct = if initial_capital > 0.0 {
            ((final_equity - initial_capital) / initial_capital) * 100.0
        } else {
            0.0
        };

        let recovery_factor = if max_drawdown.abs() > 0.0 {
            total_pnl_pct.abs() / max_drawdown.abs()
        } else {
            0.0
        };

        // Max consecutive losses
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
            max_consecutive_losses,
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

        let winning_trades: Vec<&Trade> =
            self.trade_history.iter().filter(|t| t.pnl_usdt > 0.0).collect();
        let losing_trades: Vec<&Trade> =
            self.trade_history.iter().filter(|t| t.pnl_usdt < 0.0).collect();

        let win_count = winning_trades.len();
        let loss_count = losing_trades.len();

        let win_rate = (win_count as f64 / total_trades as f64) * 100.0;
        let loss_rate = (loss_count as f64 / total_trades as f64) * 100.0;

        let total_pnl_usdt: f64 = self.trade_history.iter().map(|t| t.pnl_usdt).sum();
        let total_pnl_pct: f64 = self.trade_history.iter().map(|t| t.pnl_pct).sum();

        let avg_pnl_usdt = total_pnl_usdt / total_trades as f64;
        let avg_pnl_pct = total_pnl_pct / total_trades as f64;

        let gross_profit: f64 = winning_trades.iter().map(|t| t.pnl_usdt).sum();
        let gross_loss: f64 = losing_trades.iter().map(|t| t.pnl_usdt.abs()).sum();

        let avg_win = if win_count > 0 {
            gross_profit / win_count as f64
        } else {
            0.0
        };

        let avg_loss = if loss_count > 0 {
            gross_loss / loss_count as f64
        } else {
            0.0
        };

        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        let expectancy = (win_rate / 100.0 * avg_win) - (loss_rate / 100.0 * avg_loss);

        let best_trade = self
            .trade_history
            .iter()
            .max_by(|a, b| a.pnl_pct.partial_cmp(&b.pnl_pct).unwrap())
            .unwrap();

        let worst_trade = self
            .trade_history
            .iter()
            .min_by(|a, b| a.pnl_pct.partial_cmp(&b.pnl_pct).unwrap())
            .unwrap();

        let max_drawdown = self.calculate_max_drawdown();
        let final_equity = self.wallet.total_value(last_price);

        println!("\n=== BACKTEST SUMMARY ===");
        println!("Total trades: {}", total_trades);
        println!("Winning trades: {}", win_count);
        println!("Losing trades: {}", loss_count);
        println!("Win rate: {:.2}%", win_rate);
        println!("Average PnL per trade: ${:.2}", avg_pnl_usdt);
        println!("Average PnL per trade (%): {:.2}%", avg_pnl_pct);
        println!("Average win: ${:.2}", avg_win);
        println!("Average loss: ${:.2}", avg_loss);
        println!("Gross profit: ${:.2}", gross_profit);
        println!("Gross loss: ${:.2}", gross_loss);

        if profit_factor.is_finite() {
            println!("Profit factor: {:.2}", profit_factor);
        } else {
            println!("Profit factor: ∞ (no losing trades)");
        }

        println!("Expectancy per trade: ${:.2}", expectancy);
        println!(
            "Best trade: {:.2}% (${:.2})",
            best_trade.pnl_pct, best_trade.pnl_usdt
        );
        println!(
            "Worst trade: {:.2}% (${:.2})",
            worst_trade.pnl_pct, worst_trade.pnl_usdt
        );
        println!("Max drawdown: {:.2}%", max_drawdown);
        println!("Final equity: ${:.2}", final_equity);
    }
}

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

    fn on_tick(&mut self, current_price: f64) {
        if !self.is_holding_asset || self.last_atr_value <= 0.0 {
            return;
        }

        self.highest_price = self.highest_price.max(current_price);

        if let Some(reason) = self.determine_exit_reason(current_price, None, None) {
            let trigger_price = match reason {
                ExitReason::InitialStop => self.initial_stop_price,
                ExitReason::TakeProfit => self.target_price,
                _ => current_price,
            };

            let _ = self.execute_exit(
                trigger_price,
                current_price,
                reason,
                Phase::PostSell,
                "TICK",
            );
        }
    }

    fn on_candle_close(&mut self, candle: &Candle) {
        let current_price = candle.close;

        let indicators = self.update_indicators(candle);
        self.update_equity_curve(current_price, Phase::BarClose);
        self.refresh_drawdown_state(current_price);

        let (
            slow_value,
            fast_value,
            trend_value,
            rsi_value,
            atr_value,
            macro_ema_value,
            _vol_sma_value,
        ) = match indicators {
            Some(values) => values,
            None => return,
        };

        self.last_atr_value = atr_value;
        self.last_rsi_value = rsi_value;

        let prev_rsi = self.previous_rsi.unwrap_or(rsi_value);

        let in_cooldown = self.cooldown_bars_remaining > 0;

        // Volume confirmation: volume must be above SMA (live mode has 0 volume, skip)
        let strong_volume = candle.volume <= 0.0 || _vol_sma_value <= 0.0 || candle.volume >= _vol_sma_value;

        let signal = self.compute_signal_state(
            current_price,
            trend_value,
            rsi_value,
            prev_rsi,
            macro_ema_value,
            in_cooldown,
            fast_value,
            slow_value,
            strong_volume,
        );

        let mut action = Action::NoSignal;

        if self.is_holding_asset {
            self.bars_in_position += 1;
            self.highest_price = self.highest_price.max(candle.high);

            // Breakeven lock ВИМКНЕНО — він продавав позиції при 1R, 
            // не даючи їм вирости до 4.5R TakeProfit.
            
            if let Some(reason) = self.determine_exit_reason(current_price, Some(candle), Some(&signal)) {
                let trigger_price = match reason {
                    ExitReason::InitialStop => self.initial_stop_price,
                    ExitReason::TakeProfit => self.target_price,
                    _ => current_price,
                };

                action = self.execute_exit(
                    trigger_price,
                    current_price,
                    reason,
                    Phase::PostSell,
                    "CANDLE",
                );
            }
        }

        if matches!(action, Action::NoSignal) {
            action = self.try_enter_position(current_price, atr_value, &signal, in_cooldown);
        }

        if matches!(action, Action::NoSignal) {
            println!(
                "[CANDLE] No signals. Reason: {}. Trend EMA: ${:.2}, RSI: {:.2}, prev RSI: {:.2}",
                signal.no_signal_reason,
                trend_value,
                rsi_value,
                prev_rsi
            );
        }

        let log_entry = self.build_log_entry(
            candle,
            slow_value,
            fast_value,
            trend_value,
            rsi_value,
            atr_value,
            &signal,
            action,
        );

        let _ = self.log_tx.send(log_entry);

        if self.cooldown_bars_remaining > 0 {
            self.cooldown_bars_remaining -= 1;
        }

        self.previous_rsi = Some(rsi_value);
        self.previous_slow_sma = Some(slow_value);
        self.previous_fast_sma = Some(fast_value);
        self.previous_macro_ema = Some(macro_ema_value);
    }
}
