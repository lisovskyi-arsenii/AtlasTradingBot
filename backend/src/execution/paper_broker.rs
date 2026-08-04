//! In-memory paper trading broker — wraps the existing `Wallet` logic.
//!
//! `PaperBroker` satisfies the [`ExecutionBroker`] trait so strategies can
//! call `broker.place_order(...)` in backtests and paper-trading sessions
//! without knowing whether real exchange APIs are involved.
//!
//! All fills are immediate (next-tick slippage is NOT modelled here — that
//! responsibility belongs to `Wallet::buy` / `sell_all` which apply a
//! configurable slippage factor).

use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::execution::{
    Balances, ClientOrderId, ExecError, ExecutionBroker, Order, OrderAck,
    OrderRequest, OrderSide, OrderStatus,
};
use crate::spot::spot_wallet::{SymbolFilters, Wallet};
use crate::models::data::CryptoExchange;

/// Thread-safe in-memory broker backed by a single [`Wallet`].
pub struct PaperBroker {
    wallet: Mutex<Wallet>,
    symbol: String,
}

impl PaperBroker {
    /// Create a paper broker with the given starting USDT balance.
    pub fn new(initial_usdt: f64, symbol: &str, exchange: CryptoExchange) -> Self {
        Self {
            wallet: Mutex::new(Wallet::new(initial_usdt, exchange)),
            symbol: symbol.to_string(),
        }
    }

    /// Create with custom per-symbol filters (tick/step/min_notional).
    pub fn with_filters(
        initial_usdt: f64,
        symbol: &str,
        exchange: CryptoExchange,
        filters: SymbolFilters,
    ) -> Self {
        let mut wallet = Wallet::new(initial_usdt, exchange);
        wallet.filters = filters;
        Self {
            wallet: Mutex::new(wallet),
            symbol: symbol.to_string(),
        }
    }

    /// Read-only snapshot of the current USDT balance (for metrics).
    pub async fn usdt_balance(&self) -> f64 {
        self.wallet.lock().await.usdt_balance
    }

    /// Read-only snapshot of base-asset quantity held.
    pub async fn crypto_balance(&self) -> f64 {
        self.wallet.lock().await.crypto_balance
    }
}

#[async_trait]
impl ExecutionBroker for PaperBroker {
    fn name(&self) -> &'static str {
        "PaperBroker"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, ExecError> {
        let mut wallet = self.wallet.lock().await;

        match req.side {
            OrderSide::Buy => {
                // For market buys, use all available USDT * qty (treated as USDT amount)
                let usdt_to_spend = req.qty.min(wallet.usdt_balance);
                let price = req.price.unwrap_or_else(|| {
                    // Shouldn't happen for market orders without a price reference,
                    // but fall back to 0 which will be rejected by Wallet::buy
                    0.0
                });
                match wallet.buy(price, usdt_to_spend, true) {
                    Some(fill_price) => {
                        let filled_qty = usdt_to_spend / fill_price;
                        Ok(OrderAck {
                            client_id: req.client_id,
                            exchange_order_id: None,
                            symbol: req.symbol,
                            side: OrderSide::Buy,
                            avg_price: fill_price,
                            filled_qty,
                            status: OrderStatus::Filled,
                        })
                    }
                    None => Err(ExecError::Rejected {
                        code: -2010,
                        msg: "Insufficient balance (paper)".to_string(),
                    }),
                }
            }
            OrderSide::Sell => {
                let price = req.price.unwrap_or(0.0);
                match wallet.sell_all(price, false) {
                    Some(fill_price) => {
                        let filled_qty = req.qty;
                        Ok(OrderAck {
                            client_id: req.client_id,
                            exchange_order_id: None,
                            symbol: req.symbol,
                            side: OrderSide::Sell,
                            avg_price: fill_price,
                            filled_qty,
                            status: OrderStatus::Filled,
                        })
                    }
                    None => Err(ExecError::Rejected {
                        code: -2010,
                        msg: "No position to sell (paper)".to_string(),
                    }),
                }
            }
        }
    }

    async fn cancel(&self, _symbol: &str, _id: &ClientOrderId) -> Result<(), ExecError> {
        // Paper orders are always immediately filled; nothing to cancel.
        Ok(())
    }

    async fn get_balances(&self) -> Result<Balances, ExecError> {
        let wallet = self.wallet.lock().await;
        let mut assets = HashMap::new();
        let base_asset = self.symbol.trim_end_matches("USDT").to_string();
        if wallet.crypto_balance > 0.0 {
            assets.insert(base_asset, wallet.crypto_balance);
        }
        Ok(Balances {
            usdt_free: wallet.usdt_balance,
            usdt_locked: 0.0,
            assets,
        })
    }

    async fn get_open_orders(&self, _symbol: &str) -> Result<Vec<Order>, ExecError> {
        // Paper broker always fills immediately — no open orders.
        Ok(Vec::new())
    }
}
