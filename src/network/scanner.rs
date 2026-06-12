use reqwest::Client;
use serde::Deserialize;
use std::error::Error;

#[derive(Deserialize, Debug)]
pub struct Ticker24h {
    pub symbol: String,
    #[serde(rename = "quoteVolume")]
    pub quote_volume: String,
}

pub async fn get_top_volume_pairs(client: &Client, limit: usize) -> Result<Vec<String>, Box<dyn Error>> {
    let url = "https://api.binance.com/api/v3/ticker/24hr";
    let response = client.get(url).send().await?.json::<Vec<Ticker24h>>().await?;

    // 1. Чорний список (Стейблкоїни, фіат, токени бірж, хайп/скам)
    let blacklist = [
        // Стейблкоїни та Фіат
        "USDC", "FDUSD", "TUSD", "BUSD", "USDP", "USDD", "DAI", "PYUSD", "USD1", "RLUSD", "EUR", "AEUR", "TRY",
        // Токени з левериджем
        "UP", "DOWN", "BULL", "BEAR",
        // Токсичні/Новинні/Проблемні
        "TRUMP", "MAGA", "WLD", "ZEC", "LUNA", "FTT", "BTT",
        // Низька ефективність для Z-Score або ризик делістингу
        "ADA", "ALLO", "ENA", "HMSTR", "XPL", "NEAR", "UUSDT", "STG", "TON", "GRAM"
    ];

    // 2. Мінімальний добовий об'єм у USDT (наприклад, $15 млн)
    // Це гарантує, що ми торгуємо тільки там, де є великі гравці і нормальні тренди
    let min_daily_volume_usdt = 15_000_000.0;

    let mut valid_pairs: Vec<Ticker24h> = response
        .into_iter()
        .filter(|t| {
            // Тільки USDT пари
            if !t.symbol.ends_with("USDT") {
                return false;
            }

            // Перевірка по чорному списку
            let is_blacklisted = blacklist.iter().any(|&bad| t.symbol.starts_with(bad) || t.symbol.contains(bad));
            if is_blacklisted {
                return false;
            }

            // Перевірка ліквідності
            let volume: f64 = t.quote_volume.parse().unwrap_or(0.0);
            if volume < min_daily_volume_usdt {
                return false;
            }

            true
        })
        .collect();

    // 3. Сортуємо за об'ємом від найбільшого до найменшого
    valid_pairs.sort_by(|a, b| {
        let vol_a: f64 = a.quote_volume.parse().unwrap_or(0.0);
        let vol_b: f64 = b.quote_volume.parse().unwrap_or(0.0);
        vol_b.partial_cmp(&vol_a).unwrap()
    });

    // 4. Беремо ліміт
    let top_symbols: Vec<String> = valid_pairs
        .into_iter()
        .take(limit)
        .map(|t| t.symbol)
        .collect();

    Ok(top_symbols)
}
