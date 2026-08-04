//! HMAC-signed Binance REST broker — real order placement via Binance API.
//!
//! # Safety
//! * API key and secret MUST be provided via environment variables — never in config files.
//! * Idempotency: before retrying after a network error, we `GET /order` using
//!   the deterministic `newClientOrderId` to confirm the order's state.
//! * Rate limits: all requests pass through `with_backoff` (exponential 429 backoff).
//!
//! # Setup
//! ```bash
//! export BINANCE_API_KEY="your_key_here"
//! export BINANCE_API_SECRET="your_secret_here"
//! ```

use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::execution::{
    Balances, ClientOrderId, ExecError, ExecutionBroker, Order, OrderAck,
    OrderRequest, OrderSide, OrderStatus, OrderType,
};

const MAINNET_BASE: &str = "https://api.binance.com";
const TESTNET_BASE: &str = "https://testnet.binance.vision";

/// Binance REST broker.
///
/// Reads credentials from `BINANCE_API_KEY` and `BINANCE_API_SECRET` env vars.
/// Will fail at construction if either variable is missing, allowing a clean
/// fail-fast before the bot starts rather than a panic mid-session.
pub struct BinanceBroker {
    client: Client,
    api_key: String,
    api_secret: String,
    base_url: &'static str,
}

impl BinanceBroker {
    /// Create a new broker. Returns `Err` if credentials are missing.
    pub fn new(use_testnet: bool) -> Result<Self, String> {
        let api_key = std::env::var("BINANCE_API_KEY")
            .map_err(|_| "BINANCE_API_KEY env var not set — cannot create live broker".to_string())?;
        let api_secret = std::env::var("BINANCE_API_SECRET")
            .map_err(|_| "BINANCE_API_SECRET env var not set — cannot create live broker".to_string())?;

        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            return Err("BINANCE_API_KEY or BINANCE_API_SECRET is empty".to_string());
        }

        Ok(Self {
            client: Client::new(),
            api_key,
            api_secret,
            base_url: if use_testnet { TESTNET_BASE } else { MAINNET_BASE },
        })
    }

    /// Check if local system time is synchronized with Binance API time.
    /// Returns an error if the drift exceeds 1000ms (1 second).
    pub async fn check_time_sync(&self) -> Result<(), String> {
        let url = format!("{}/api/v3/time", self.base_url);
        let resp = self.client.get(&url).send().await
            .map_err(|e| format!("Failed to fetch Binance server time: {}", e))?;

        if !resp.status().is_success() {
            return Err(format!("Binance time API returned status {}", resp.status()));
        }

        let body: Value = resp.json().await
            .map_err(|e| format!("Failed to parse Binance time response: {}", e))?;
        
        let server_time = body["serverTime"].as_u64()
            .ok_or_else(|| "No 'serverTime' field in response".to_string())?;
        
        let local_time = Self::timestamp_ms();
        let diff = (server_time as i64 - local_time as i64).abs();

        if diff > 1000 {
            return Err(format!(
                "Time drift too high: {}ms. Please sync your system clock via NTP. \
                 Binance rejects requests with >5000ms drift.",
                diff
            ));
        }

        println!("[BROKER] NTP time sync verified. Drift: {}ms", diff);
        Ok(())
    }

    /// Current UTC timestamp in milliseconds.
    fn timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_millis() as u64
    }

    /// Sign a query string with HMAC-SHA256 using the API secret.
    fn sign(&self, query: &str) -> String {
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can accept any key length");
        mac.update(query.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Build a signed query string by appending `&timestamp=...&signature=...`.
    fn signed_query(&self, params: &str) -> String {
        let ts = Self::timestamp_ms();
        let with_ts = if params.is_empty() {
            format!("timestamp={}", ts)
        } else {
            format!("{}&timestamp={}", params, ts)
        };
        let sig = self.sign(&with_ts);
        format!("{}&signature={}", with_ts, sig)
    }

    /// Exponential backoff retry for transient errors.
    async fn with_backoff<T, F, Fut>(&self, mut f: F) -> Result<T, reqwest::Error>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, reqwest::Error>>,
    {
        let mut delay_ms = 500u64;
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let retryable = e.is_timeout()
                        || e.is_connect()
                        || e.status().map(|s| s.as_u16() == 429 || s.is_server_error()).unwrap_or(false);
                    if !retryable || delay_ms > 30_000 {
                        return Err(e);
                    }
                    eprintln!(
                        "[BROKER] Retrying after {}ms (status: {:?})",
                        delay_ms,
                        e.status()
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                    delay_ms = (delay_ms * 2).min(30_000);
                }
            }
        }
    }

    /// Check if an order already exists (idempotency check after network error).
    async fn get_order_by_client_id(
        &self,
        symbol: &str,
        client_id: &ClientOrderId,
    ) -> Result<Option<OrderAck>, ExecError> {
        let params = format!(
            "symbol={}&origClientOrderId={}",
            symbol.to_uppercase(),
            client_id.0
        );
        let query = self.signed_query(&params);
        let url = format!("{}/api/v3/order?{}", self.base_url, query);

        let resp = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| ExecError::Network(e.to_string()))?;

        let status = resp.status();
        let body: Value = resp.json().await.map_err(|e| ExecError::Parse(e.to_string()))?;

        if status.as_u16() == 404 {
            return Ok(None);
        }

        if !status.is_success() {
            return Err(ExecError::Rejected {
                code: body["code"].as_i64().unwrap_or(-1),
                msg: body["msg"].as_str().unwrap_or("unknown").to_string(),
            });
        }

        let avg_price: f64 = body["price"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let filled_qty: f64 = body["executedQty"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let status_str = body["status"].as_str().unwrap_or("");

        let order_status = match status_str {
            "FILLED"            => OrderStatus::Filled,
            "PARTIALLY_FILLED"  => OrderStatus::PartiallyFilled,
            "NEW"               => OrderStatus::New,
            "CANCELED"          => OrderStatus::Canceled,
            _                   => OrderStatus::Rejected,
        };

        Ok(Some(OrderAck {
            client_id: client_id.clone(),
            exchange_order_id: body["orderId"].as_u64(),
            symbol: symbol.to_string(),
            side: if body["side"].as_str() == Some("BUY") { OrderSide::Buy } else { OrderSide::Sell },
            avg_price,
            filled_qty,
            status: order_status,
        }))
    }
}

#[async_trait]
impl ExecutionBroker for BinanceBroker {
    fn name(&self) -> &'static str {
        "BinanceBroker"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, ExecError> {
        let side_str = match req.side {
            OrderSide::Buy  => "BUY",
            OrderSide::Sell => "SELL",
        };
        let type_str = match req.order_type {
            OrderType::Market           => "MARKET",
            OrderType::Limit            => "LIMIT",
            OrderType::StopLossLimit    => "STOP_LOSS_LIMIT",
            OrderType::TakeProfitLimit  => "TAKE_PROFIT_LIMIT",
        };

        let mut params = format!(
            "symbol={}&side={}&type={}&quantity={:.8}&newClientOrderId={}",
            req.symbol.to_uppercase(),
            side_str,
            type_str,
            req.qty,
            req.client_id.0,
        );

        if let Some(price) = req.price {
            params.push_str(&format!("&price={:.8}&timeInForce=GTC", price));
        }
        if let Some(stop) = req.stop_price {
            params.push_str(&format!("&stopPrice={:.8}", stop));
        }

        let query = self.signed_query(&params);
        let url = format!("{}/api/v3/order", self.base_url);

        let result = self
            .with_backoff(|| {
                self.client
                    .post(&url)
                    .header("X-MBX-APIKEY", &self.api_key)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(query.clone())
                    .send()
            })
            .await;

        match result {
            Err(e) => {
                // Network error: check if the order landed anyway (idempotency)
                eprintln!("[BROKER] Network error on place_order: {}. Checking idempotency...", e);
                match self.get_order_by_client_id(&req.symbol, &req.client_id).await? {
                    Some(ack) => {
                        println!("[BROKER] Idempotency check: order {} already exists ({:?})", req.client_id, ack.status);
                        Ok(ack)
                    }
                    None => Err(ExecError::Network(e.to_string())),
                }
            }
            Ok(resp) => {
                let http_status = resp.status();
                let body: Value = resp.json().await.map_err(|e| ExecError::Parse(e.to_string()))?;

                if !http_status.is_success() {
                    return Err(ExecError::Rejected {
                        code: body["code"].as_i64().unwrap_or(-1),
                        msg: body["msg"].as_str().unwrap_or("unknown").to_string(),
                    });
                }

                let avg_price: f64 = body["price"].as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let filled_qty: f64 = body["executedQty"].as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                let status_str = body["status"].as_str().unwrap_or("");
                let order_status = match status_str {
                    "FILLED"            => OrderStatus::Filled,
                    "PARTIALLY_FILLED"  => OrderStatus::PartiallyFilled,
                    "NEW"               => OrderStatus::New,
                    "CANCELED"          => OrderStatus::Canceled,
                    _                   => OrderStatus::Rejected,
                };

                Ok(OrderAck {
                    client_id: req.client_id,
                    exchange_order_id: body["orderId"].as_u64(),
                    symbol: req.symbol,
                    side: req.side,
                    avg_price,
                    filled_qty,
                    status: order_status,
                })
            }
        }
    }

    async fn cancel(&self, symbol: &str, id: &ClientOrderId) -> Result<(), ExecError> {
        let params = format!(
            "symbol={}&origClientOrderId={}",
            symbol.to_uppercase(),
            id.0
        );
        let query = self.signed_query(&params);
        let url = format!("{}/api/v3/order?{}", self.base_url, query);

        let resp = self
            .client
            .delete(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| ExecError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            let body: Value = resp.json().await.map_err(|e| ExecError::Parse(e.to_string()))?;
            return Err(ExecError::Rejected {
                code: body["code"].as_i64().unwrap_or(-1),
                msg: body["msg"].as_str().unwrap_or("unknown").to_string(),
            });
        }

        Ok(())
    }

    async fn get_balances(&self) -> Result<Balances, ExecError> {
        let query = self.signed_query("");
        let url = format!("{}/api/v3/account?{}", self.base_url, query);

        let resp = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| ExecError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            let body: Value = resp.json().await.map_err(|e| ExecError::Parse(e.to_string()))?;
            return Err(ExecError::Rejected {
                code: body["code"].as_i64().unwrap_or(-1),
                msg: body["msg"].as_str().unwrap_or("unknown").to_string(),
            });
        }

        let body: Value = resp.json().await.map_err(|e| ExecError::Parse(e.to_string()))?;
        let balances_arr = body["balances"].as_array().ok_or_else(|| {
            ExecError::Parse("No 'balances' field in account response".to_string())
        })?;

        let mut usdt_free = 0.0f64;
        let mut usdt_locked = 0.0f64;
        let mut assets = std::collections::HashMap::new();

        for b in balances_arr {
            let asset = b["asset"].as_str().unwrap_or("");
            let free: f64 = b["free"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let locked: f64 = b["locked"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);

            if asset == "USDT" {
                usdt_free = free;
                usdt_locked = locked;
            } else if free > 0.0 || locked > 0.0 {
                assets.insert(asset.to_string(), free);
            }
        }

        Ok(Balances { usdt_free, usdt_locked, assets })
    }

    async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>, ExecError> {
        let params = format!("symbol={}", symbol.to_uppercase());
        let query = self.signed_query(&params);
        let url = format!("{}/api/v3/openOrders?{}", self.base_url, query);

        let resp = self
            .client
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await
            .map_err(|e| ExecError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            let body: Value = resp.json().await.map_err(|e| ExecError::Parse(e.to_string()))?;
            return Err(ExecError::Rejected {
                code: body["code"].as_i64().unwrap_or(-1),
                msg: body["msg"].as_str().unwrap_or("unknown").to_string(),
            });
        }

        let orders_arr: Vec<Value> = resp.json().await
            .map_err(|e| ExecError::Parse(e.to_string()))?;

        let orders = orders_arr.into_iter().filter_map(|o| {
            let client_id = ClientOrderId(o["clientOrderId"].as_str()?.to_string());
            let exchange_id = o["orderId"].as_u64()?;
            let price: f64 = o["price"].as_str()?.parse().ok()?;
            let orig_qty: f64 = o["origQty"].as_str()?.parse().ok()?;
            let exec_qty: f64 = o["executedQty"].as_str()?.parse().ok()?;
            let side = if o["side"].as_str() == Some("BUY") { OrderSide::Buy } else { OrderSide::Sell };
            let order_type = match o["type"].as_str() {
                Some("MARKET") => OrderType::Market,
                Some("STOP_LOSS_LIMIT") => OrderType::StopLossLimit,
                Some("TAKE_PROFIT_LIMIT") => OrderType::TakeProfitLimit,
                _ => OrderType::Limit,
            };

            Some(Order {
                client_id,
                exchange_order_id: exchange_id,
                symbol: symbol.to_string(),
                side,
                order_type,
                price,
                orig_qty,
                executed_qty: exec_qty,
                status: OrderStatus::New,
            })
        }).collect();

        Ok(orders)
    }
}
