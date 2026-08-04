//! Newline-delimited JSON (NDJSON) structured trade log.
//!
//! Writes one JSON object per line alongside the CSV log. JSON logs are
//! machine-readable and can be ingested by log aggregators (Loki, ELK, etc.).
//!
//! Each record includes all fields from [`CandleLogEntry`] plus a structured
//! `event_type` discriminator for easy filtering.
//!
//! # File naming
//! `{path}.jsonl` — e.g. `live_log.csv` → `live_log.jsonl`

use crate::models::candle_log_entry::CandleLogEntry;
use std::error::Error;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};

/// Derive the JSONL path from the CSV path.
pub fn jsonl_path(csv_path: &str) -> String {
    if let Some(stem) = csv_path.strip_suffix(".csv") {
        format!("{}.jsonl", stem)
    } else {
        format!("{}.jsonl", csv_path)
    }
}

/// Append `entry` as a single newline-terminated JSON object to the JSONL log.
pub async fn write_to_jsonl_file(
    entry: &CandleLogEntry,
    csv_path: &str,
) -> Result<(), Box<dyn Error>> {
    let path = jsonl_path(csv_path);

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;

    let mut writer = BufWriter::new(file);

    // Serialise to compact JSON + newline
    let line = serde_json::to_string(entry)?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonl_path_replaces_csv_extension() {
        assert_eq!(jsonl_path("trade_log.csv"), "trade_log.jsonl");
        assert_eq!(jsonl_path("logs/live_log.csv"), "logs/live_log.jsonl");
        assert_eq!(jsonl_path("no_extension"), "no_extension.jsonl");
    }
}
