use crate::models::candle_log_entry::CandleLogEntry;
use csv_async::AsyncWriterBuilder;
// Змінено імпорт
use std::error::Error;
use tokio::fs::OpenOptions;
use tokio::io::BufWriter;

pub async fn write_to_csv_file(
    candle_log_entry: &CandleLogEntry,
) -> Result<(), Box<dyn Error>> {
    let path = "trading_bot.csv";

    let should_write_header = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata.len() == 0,
        Err(_) => true,
    };

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    let writer = BufWriter::new(file);

    let mut csv_writer = AsyncWriterBuilder::new()
        .has_headers(should_write_header)
        .create_serializer(writer);

    csv_writer.serialize(candle_log_entry).await?;
    csv_writer.flush().await?;

    Ok(())
}
