use reqwest::Client;
use serde::Deserialize;
use std::error::Error;

#[derive(Debug, Deserialize)]
struct FearGreedData {
    value: String,
    value_classification: String,
}

#[derive(Debug, Deserialize)]
struct FearGreedResponse {
    data: Vec<FearGreedData>,
}

/// Fetches the latest Fear & Greed index from alternative.me API.
/// Returns a tuple of (value, classification).
pub async fn fetch_fear_greed_index(client: &Client) -> Result<(f64, String), Box<dyn Error>> {
    let url = "https://api.alternative.me/fng/?limit=1";
    let resp = client.get(url).send().await?.json::<FearGreedResponse>().await?;

    if let Some(latest) = resp.data.first() {
        let value: f64 = latest.value.parse().unwrap_or(50.0);
        return Ok((value, latest.value_classification.clone()));
    }

    Err("No data found in Fear & Greed API response".into())
}
