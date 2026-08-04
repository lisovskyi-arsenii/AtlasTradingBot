//! Execution layer — broker abstraction and implementations.
//!
//! The `ExecutionBroker` trait decouples signal generation from order
//! placement. Strategies call `broker.place_order(...)` without knowing
//! whether they are running against a paper wallet or a real exchange.
//!
//! Two concrete implementations are provided:
//!
//! * [`PaperBroker`] — in-memory simulation (wraps the existing `Wallet`)
//! * [`BinanceBroker`] — HMAC-signed REST calls to Binance mainnet / testnet
//!
//! # Usage
//! ```no_run
//! let broker: Arc<dyn ExecutionBroker> = Arc::new(PaperBroker::new(10_000.0));
//! let ack = broker.place_order(OrderRequest { ... }).await?;
//! ```

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

pub mod paper_broker;
pub mod binance_broker;
pub mod state;
pub mod reconciliation;

// ── Domain types ─────────────────────────────────────────────────────────────

/// Deterministic client order ID.
///
/// Format: `ATLAS_{SYMBOL}_{SIDE}_{TIMESTAMP_MS}`
/// This makes every order idempotent: if a network error occurs after sending
/// but before receiving the ack, we can `GET /order` using this ID to check
/// the state before retrying.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientOrderId(pub String);

impl ClientOrderId {
    pub fn new(symbol: &str, side: OrderSide) -> Self {
        let ts = chrono::Utc::now().timestamp_millis();
        let side_str = match side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };
        Self(format!("ATLAS_{}_{}_{}", symbol.to_uppercase(), side_str, ts))
    }
}

impl fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    StopLossLimit,
    TakeProfitLimit,
}

/// A request to place an order.
#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub symbol: String,
    pub client_id: ClientOrderId,
    pub side: OrderSide,
    pub order_type: OrderType,
    /// Quantity in base asset (e.g. BTC for BTCUSDT).
    pub qty: f64,
    /// Limit price (ignored for Market orders).
    pub price: Option<f64>,
    /// Stop trigger price (for StopLossLimit / TakeProfitLimit).
    pub stop_price: Option<f64>,
}

/// Acknowledgement returned when an order is successfully placed.
#[derive(Debug, Clone)]
pub struct OrderAck {
    pub client_id: ClientOrderId,
    /// Exchange-assigned order ID.
    pub exchange_order_id: Option<u64>,
    pub symbol: String,
    pub side: OrderSide,
    /// Average fill price (`0.0` for open limit orders).
    pub avg_price: f64,
    /// Filled quantity.
    pub filled_qty: f64,
    pub status: OrderStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Filled,
    PartiallyFilled,
    New,
    Canceled,
    Rejected,
}

/// Asset balances snapshot.
#[derive(Debug, Clone)]
pub struct Balances {
    /// USDT (or quote currency) free balance.
    pub usdt_free: f64,
    /// USDT locked in open orders.
    pub usdt_locked: f64,
    /// Map of base-asset symbol → free balance (e.g. `"BTC" → 0.5`).
    pub assets: std::collections::HashMap<String, f64>,
}

/// An open order as returned by the exchange.
#[derive(Debug, Clone)]
pub struct Order {
    pub client_id: ClientOrderId,
    pub exchange_order_id: u64,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub price: f64,
    pub orig_qty: f64,
    pub executed_qty: f64,
    pub status: OrderStatus,
}

/// Errors from the execution layer.
#[derive(Debug)]
pub enum ExecError {
    /// HTTP / network failure.
    Network(String),
    /// Exchange rejected the order (bad params, insufficient funds, etc.).
    Rejected { code: i64, msg: String },
    /// Order not found (used after idempotency check).
    NotFound(ClientOrderId),
    /// Broker is halted — no orders accepted.
    Halted,
    /// Serialisation / parse failure.
    Parse(String),
}

impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecError::Network(e)       => write!(f, "Network error: {}", e),
            ExecError::Rejected { code, msg } => write!(f, "Rejected ({}): {}", code, msg),
            ExecError::NotFound(id)     => write!(f, "Order not found: {}", id),
            ExecError::Halted           => write!(f, "Broker halted"),
            ExecError::Parse(e)         => write!(f, "Parse error: {}", e),
        }
    }
}

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Core interface for all order execution backends.
///
/// Strategies and the risk layer interact exclusively through this trait,
/// enabling transparent swapping between paper, testnet, and live brokers.
#[async_trait]
pub trait ExecutionBroker: Send + Sync {
    /// Place a new order. Returns an [`OrderAck`] on success.
    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, ExecError>;

    /// Cancel an open order by its client ID.
    async fn cancel(&self, symbol: &str, id: &ClientOrderId) -> Result<(), ExecError>;

    /// Fetch current account balances.
    async fn get_balances(&self) -> Result<Balances, ExecError>;

    /// Fetch all open orders for a symbol.
    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>, ExecError>;

    /// Human-readable name (for logging).
    fn name(&self) -> &'static str;

    /// Downcast to Any for concrete type methods
    fn as_any(&self) -> &dyn std::any::Any;
}
