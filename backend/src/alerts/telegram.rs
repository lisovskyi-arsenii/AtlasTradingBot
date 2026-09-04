//! Telegram alert integration.
//!
//! Sends a message to a Telegram bot. Credentials are read from env vars:
//! - `TELEGRAM_BOT_TOKEN` — the bot token from @BotFather
//! - `TELEGRAM_CHAT_ID`   — your personal or group chat ID
//!
//! If either var is unset, alerts silently no-op so the bot continues running.

use reqwest::Client;

/// Send a Telegram alert. No-ops gracefully if credentials are not configured.
///
/// # Example
/// ```ignore
/// // Illustrative only — needs an async context to actually await.
/// alerts::telegram::send("[ATLAS] Drawdown halt triggered! Equity: $9800").await;
/// ```
pub async fn send(text: &str) {
    let token = match std::env::var("TELEGRAM_BOT_TOKEN") {
        Ok(t) if !t.is_empty() => t,
        _ => return, // silently skip if not configured
    };
    let chat_id = match std::env::var("TELEGRAM_CHAT_ID") {
        Ok(c) if !c.is_empty() => c,
        _ => return,
    };

    let url = format!("https://api.telegram.org/bot{}/sendMessage", token);
    let client = Client::new();

    let payload = serde_json::json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    });

    match client.post(&url).json(&payload).send().await {
        Ok(resp) if resp.status().is_success() => {
            // Alert sent successfully — no noise in logs
        }
        Ok(resp) => {
            eprintln!(
                "[TELEGRAM] Alert failed: HTTP {} — {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        Err(e) => {
            eprintln!("[TELEGRAM] Alert delivery error: {}", e);
        }
    }
}

/// Format and send a critical alert (prefixes with ⚠️ and bot name).
pub async fn alert_critical(event: &str, detail: &str) {
    let msg = format!(
        "⚠️ <b>AtlasBot CRITICAL</b>\n<code>{}</code>\n{}",
        event, detail
    );
    send(&msg).await;
}

/// Format and send an informational alert.
pub async fn alert_info(event: &str, detail: &str) {
    let msg = format!(
        "ℹ️ <b>AtlasBot</b>\n<code>{}</code>\n{}",
        event, detail
    );
    send(&msg).await;
}
