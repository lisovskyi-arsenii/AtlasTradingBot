//! Portfolio-level kill switch.
//!
//! A single `AtomicBool` acts as a shared halt flag across all async tasks.
//! When triggered (via HTTP `POST /halt`, drawdown, or daily loss limit):
//!
//! 1. New entries are blocked in the trading loop.
//! 2. A Telegram alert is sent.
//! 3. The caller is responsible for closing open positions (via the broker).
//!
//! # HTTP endpoint
//! The metrics server is extended with a `POST /halt` path that sets the flag.
//! This allows remote-halting the bot without a deployment (e.g. from a phone).
//!
//! # Thread safety
//! `KillSwitch` is cheaply cloneable (`Arc<AtomicBool>` inside) and can be
//! shared freely across `tokio::spawn` boundaries.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::alerts::telegram;

#[derive(Clone, Debug)]
pub struct KillSwitch {
    flag: Arc<AtomicBool>,
}

impl KillSwitch {
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns `true` if the bot has been halted.
    #[inline]
    pub fn is_halted(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    /// Trigger a halt, log the reason, and send a Telegram alert.
    /// Idempotent — calling multiple times is safe.
    pub async fn halt(&self, reason: &str) {
        let was_running = self.flag
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();

        if was_running {
            eprintln!("\n🛑 [KILL-SWITCH] HALT triggered: {}", reason);
            telegram::alert_critical(
                "HALT TRIGGERED",
                &format!("Reason: {}\nBot has stopped accepting new entries.", reason),
            )
            .await;
        }
    }

    /// Reset the halt flag (e.g. after manual review).
    pub fn reset(&self) {
        self.flag.store(false, Ordering::Release);
        println!("[KILL-SWITCH] Reset — bot resuming normal operation.");
    }

    /// Get a raw `Arc<AtomicBool>` for use in HTTP handlers.
    pub fn raw(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flag)
    }
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn a tiny HTTP listener on `port` for `POST /halt` and `GET /status`.
/// This extends (not replaces) the Prometheus metrics server.
pub async fn run_control_server(kill_switch: KillSwitch, port: u16) {
    use tokio::net::TcpListener;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let addr = format!("127.0.0.1:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[CONTROL] Failed to bind {}: {}", addr, e);
            return;
        }
    };
    println!("[CONTROL] HTTP control server at http://127.0.0.1:{}", port);
    println!("[CONTROL]   POST /halt   — trigger emergency halt");
    println!("[CONTROL]   POST /reset  — reset halt flag");
    println!("[CONTROL]   GET  /status — bot health");

    loop {
        let (mut stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };

        let ks = kill_switch.clone();

        tokio::spawn(async move {
            let mut reader = BufReader::new(&mut stream);
            let mut first_line = String::new();
            let _ = reader.read_line(&mut first_line).await;

            let response = if first_line.starts_with("POST /halt") {
                ks.halt(&format!("HTTP /halt from {}", peer)).await;
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nHalt triggered.\n"
            } else if first_line.starts_with("POST /reset") {
                ks.reset();
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nBot reset.\n"
            } else if first_line.starts_with("GET /status") {
                let status = if ks.is_halted() { "HALTED" } else { "RUNNING" };
                &format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nStatus: {}\n", status)
            } else {
                "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\nUnknown endpoint.\n"
            };

            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_is_not_halted() {
        let ks = KillSwitch::new();
        assert!(!ks.is_halted());
    }

    #[tokio::test]
    async fn halt_sets_flag() {
        let ks = KillSwitch::new();
        ks.halt("test halt").await;
        assert!(ks.is_halted());
    }

    #[tokio::test]
    async fn halt_is_idempotent() {
        let ks = KillSwitch::new();
        ks.halt("first").await;
        ks.halt("second").await; // should not panic
        assert!(ks.is_halted());
    }

    #[test]
    fn reset_clears_flag() {
        let ks = KillSwitch::new();
        // Set flag directly
        ks.flag.store(true, std::sync::atomic::Ordering::Release);
        ks.reset();
        assert!(!ks.is_halted());
    }
}
