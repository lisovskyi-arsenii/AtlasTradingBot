use rusqlite::{params, Connection, Result};
use std::sync::{Arc, Mutex};
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub struct PositionState {
    pub symbol: String,
    pub is_holding: bool,
    pub is_short: bool,
    pub entry_price: f64,
    pub qty: f64,
    pub initial_stop_price: f64,
    pub trailing_stop_price: f64,
}

#[derive(Clone)]
pub struct StateManager {
    conn: Arc<Mutex<Connection>>,
}

impl StateManager {
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        
        conn.execute(
            "CREATE TABLE IF NOT EXISTS positions (
                symbol TEXT PRIMARY KEY,
                is_holding BOOLEAN NOT NULL,
                is_short BOOLEAN NOT NULL,
                entry_price REAL NOT NULL,
                qty REAL NOT NULL,
                initial_stop_price REAL NOT NULL,
                trailing_stop_price REAL NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn save_position(&self, pos: &PositionState) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO positions (symbol, is_holding, is_short, entry_price, qty, initial_stop_price, trailing_stop_price, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, CURRENT_TIMESTAMP)
             ON CONFLICT(symbol) DO UPDATE SET
                is_holding = excluded.is_holding,
                is_short = excluded.is_short,
                entry_price = excluded.entry_price,
                qty = excluded.qty,
                initial_stop_price = excluded.initial_stop_price,
                trailing_stop_price = excluded.trailing_stop_price,
                updated_at = CURRENT_TIMESTAMP",
            params![
                pos.symbol,
                pos.is_holding,
                pos.is_short,
                pos.entry_price,
                pos.qty,
                pos.initial_stop_price,
                pos.trailing_stop_price
            ],
        )?;
        Ok(())
    }

    pub fn load_position(&self, symbol: &str) -> Result<Option<PositionState>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT symbol, is_holding, is_short, entry_price, qty, initial_stop_price, trailing_stop_price 
             FROM positions WHERE symbol = ?1"
        )?;
        
        let mut rows = stmt.query(params![symbol])?;
        
        if let Some(row) = rows.next()? {
            Ok(Some(PositionState {
                symbol: row.get(0)?,
                is_holding: row.get(1)?,
                is_short: row.get(2)?,
                entry_price: row.get(3)?,
                qty: row.get(4)?,
                initial_stop_price: row.get(5)?,
                trailing_stop_price: row.get(6)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn delete_position(&self, symbol: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM positions WHERE symbol = ?1", params![symbol])?;
        Ok(())
    }
}
