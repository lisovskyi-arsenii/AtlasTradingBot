use crate::models::bot_config::BotConfig;
use crate::strategy::TradingStrategy;
use std::collections::HashMap;

/// Manages portfolio-level risk limits.
pub struct RiskManager {
    pub peak_portfolio_equity: f64,
    pub max_drawdown_pct: f64,
    pub global_daily_loss_limit_usdt: f64,
}

impl RiskManager {
    pub fn new(config: &BotConfig) -> Self {
        Self {
            peak_portfolio_equity: config.margin, // Starts at initial margin
            max_drawdown_pct: 15.0, // Default 15% max global DD
            global_daily_loss_limit_usdt: config.margin * 0.05, // 5% of margin per day
        }
    }

    /// Evaluates the current portfolio state and returns an error message if a hard limit is breached.
    pub fn evaluate_portfolio(
        &mut self,
        _strategies: &HashMap<String, Box<dyn TradingStrategy>>,
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

        Ok(())
    }
}
