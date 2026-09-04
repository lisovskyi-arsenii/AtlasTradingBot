use crate::models::bot_config::BotConfig;
use crate::strategy::TradingStrategy;
use std::collections::HashMap;

/// Below this many simultaneously open positions, "concentration" is
/// meaningless (a 1-2 symbol setup is unavoidably 100% in one direction) —
/// the check in `evaluate_portfolio` only engages at or above this count.
const MIN_POSITIONS_FOR_CONCENTRATION_CHECK: usize = 3;

/// Manages portfolio-level risk limits.
pub struct RiskManager {
    pub peak_portfolio_equity: f64,
    pub max_drawdown_pct: f64,
    pub global_daily_loss_limit_usdt: f64,
    /// Max fraction of open positions allowed to point the same direction
    /// once there are enough of them to matter (see
    /// `MIN_POSITIONS_FOR_CONCENTRATION_CHECK`). Crypto alts move together
    /// with BTC, so "10 different symbols, all long" is not the
    /// diversification it looks like — it's one leveraged directional bet
    /// that a single correlated market move can hit all at once.
    pub max_directional_concentration_pct: f64,
}

impl RiskManager {
    pub fn new(config: &BotConfig) -> Self {
        Self {
            peak_portfolio_equity: config.margin, // Starts at initial margin
            max_drawdown_pct: 15.0, // Default 15% max global DD
            global_daily_loss_limit_usdt: config.margin * 0.05, // 5% of margin per day
            max_directional_concentration_pct: 0.8, // Halt if >80% of open positions are one-directional
        }
    }

    /// Evaluates the current portfolio state and returns an error message if a hard limit is breached.
    pub fn evaluate_portfolio(
        &mut self,
        strategies: &HashMap<String, Box<dyn TradingStrategy>>,
        initial_margin: f64,
        total_equity: f64,
    ) -> Result<(), String> {
        // Update peak equity
        if total_equity > self.peak_portfolio_equity {
            self.peak_portfolio_equity = total_equity;
        }

        // Check global drawdown
        if self.peak_portfolio_equity > 0.0 {
            let drawdown = (self.peak_portfolio_equity - total_equity) / self.peak_portfolio_equity * 100.0;
            if drawdown > self.max_drawdown_pct {
                return Err(format!(
                    "GLOBAL DRAWDOWN HALT: Portfolio DD {:.2}% exceeded limit {:.2}%",
                    drawdown, self.max_drawdown_pct
                ));
            }
        }

        // Check global daily loss (simplified: just checking absolute drop from margin)
        // A true daily loss would reset at midnight UTC.
        if total_equity < initial_margin - self.global_daily_loss_limit_usdt {
            return Err(format!(
                "GLOBAL DAILY LOSS HALT: Portfolio dropped below daily limit (${:.2})",
                initial_margin - self.global_daily_loss_limit_usdt
            ));
        }

        // Check directional concentration across open positions.
        let (mut long_count, mut short_count) = (0usize, 0usize);
        for strategy in strategies.values() {
            match strategy.position_side_int() {
                1 => long_count += 1,
                -1 => short_count += 1,
                _ => {}
            }
        }
        let open_count = long_count + short_count;
        if open_count >= MIN_POSITIONS_FOR_CONCENTRATION_CHECK {
            let dominant = long_count.max(short_count);
            let concentration = dominant as f64 / open_count as f64;
            if concentration > self.max_directional_concentration_pct {
                let direction = if long_count >= short_count { "LONG" } else { "SHORT" };
                return Err(format!(
                    "PORTFOLIO CONCENTRATION HALT: {}/{} open positions are {} ({:.0}% > {:.0}% limit) — correlated symbols are not diversification",
                    dominant, open_count, direction,
                    concentration * 100.0, self.max_directional_concentration_pct * 100.0
                ));
            }
        }

        Ok(())
    }
}
