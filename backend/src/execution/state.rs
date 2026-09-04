use std::path::Path;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{FromRow, SqlitePool};

#[derive(Debug, Clone, PartialEq, FromRow)]
pub struct PositionState {
    pub symbol: String,
    pub is_holding: bool,
    pub is_short: bool,
    pub entry_price: f64,
    pub qty: f64,
    pub initial_stop_price: f64,
    pub trailing_stop_price: f64,
    /// `client_id` of the resting exchange-side STOP_LOSS_LIMIT order that
    /// protects this position, if one has been placed. Strategies never set
    /// this (they don't know about broker-side orders) — it is filled in by
    /// `sync_broker_state` in `main.rs` after `place_protective_stop`
    /// succeeds, and consumed on the next state change to cancel/replace it.
    pub stop_order_id: Option<String>,
}

#[derive(Clone)]
pub struct StateManager {
    pool: SqlitePool,
}

impl StateManager {
    pub async fn new<P: AsRef<Path>>(db_path: P) -> Result<Self, sqlx::Error> {
        let path_str = db_path.as_ref().to_str().unwrap_or("positions.db");
        let conn_str = if path_str.starts_with("sqlite:") {
            path_str.to_string()
        } else {
            format!("sqlite:{}?mode=rwc", path_str)
        };

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&conn_str)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                is_holding BOOLEAN NOT NULL,
                is_short BOOLEAN NOT NULL,
                entry_price REAL NOT NULL,
                qty REAL NOT NULL,
                initial_stop_price REAL NOT NULL,
                trailing_stop_price REAL NOT NULL,
                stop_order_id TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )",
        )
        .execute(&pool)
        .await?;

        // Міграція для старих баз (ігноруємо помилку дублікату стовпця)
        let _ = sqlx::query("ALTER TABLE positions ADD COLUMN stop_order_id TEXT")
            .execute(&pool)
            .await;

        Ok(Self { pool })
    }

    pub async fn save_position(&self, pos: &PositionState) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO positions (symbol, is_holding, is_short, entry_price, qty, initial_stop_price, trailing_stop_price, stop_order_id, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP)
             ON CONFLICT(symbol) DO UPDATE SET
                is_holding = excluded.is_holding,
                is_short = excluded.is_short,
                entry_price = excluded.entry_price,
                qty = excluded.qty,
                initial_stop_price = excluded.initial_stop_price,
                trailing_stop_price = excluded.trailing_stop_price,
                stop_order_id = excluded.stop_order_id,
                updated_at = CURRENT_TIMESTAMP",
        )
            .bind(&pos.symbol)
            .bind(pos.is_holding)
            .bind(pos.is_short)
            .bind(pos.entry_price)
            .bind(pos.qty)
            .bind(pos.initial_stop_price)
            .bind(pos.trailing_stop_price)
            .bind(&pos.stop_order_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn load_position(&self, symbol: &str) -> Result<Option<PositionState>, sqlx::Error> {
        let pos = sqlx::query_as::<_, PositionState>(
            "SELECT symbol, is_holding, is_short, entry_price, qty, initial_stop_price, trailing_stop_price, stop_order_id
             FROM positions WHERE symbol = ?1",
        )
            .bind(symbol)
            .fetch_optional(&self.pool)
            .await?;

        Ok(pos)
    }

    pub async fn delete_position(&self, symbol: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM positions WHERE symbol = ?1")
            .bind(symbol)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
