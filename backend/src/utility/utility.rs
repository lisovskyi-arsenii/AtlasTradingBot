pub async fn sleep_seconds(seconds: u64) {
    tokio::time::sleep(tokio::time::Duration::from_secs(seconds)).await;
}

pub async fn sleep_milliseconds(milliseconds: u64) {
    tokio::time::sleep(tokio::time::Duration::from_millis(milliseconds)).await;
}
