#[derive(Debug, Clone)]
pub struct Trade {
    pub entry_price: f64,
    pub exit_price: f64,
    pub pnl_pct: f64,
    pub pnl_usdt: f64,
    pub bars_held: usize,
    pub side: String,
    pub equity_after_trade: f64,
    pub exit_reason: String,
}
