//! Order-book imbalance (OBI) — a microstructure signal used as a LIVE-ONLY
//! confirmation layer on top of the candle-based mean-reversion logic.
//!
//! The order book is the near future; OHLC candles are the past. When the
//! aggregated bid quantity near the top of the book dwarfs the ask quantity,
//! there is more passive demand than supply and price tends to drift up (and
//! vice versa). We collapse the top `levels` of each side into a single number
//! in `[-1.0, 1.0]`:
//!
//! ```text
//! obi = (bid_qty - ask_qty) / (bid_qty + ask_qty)
//! ```
//!
//! `+1.0` = only bids, `-1.0` = only asks, `0.0` = balanced.
//!
//! NOTE: this cannot be backtested with OHLC data (there is no historical book),
//! so it only ever runs against the live depth stream.

/// Aggregate the top `levels` of each side into an imbalance in `[-1.0, 1.0]`.
///
/// `bids` / `asks` are `(price, quantity)` pairs. Returns `None` when both sides
/// are empty (no information).
pub fn compute_imbalance(bids: &[(f64, f64)], asks: &[(f64, f64)], levels: usize) -> Option<f64> {
    let bid_qty: f64 = bids.iter().take(levels).map(|(_, q)| q).sum();
    let ask_qty: f64 = asks.iter().take(levels).map(|(_, q)| q).sum();
    let total = bid_qty + ask_qty;
    if total <= 0.0 {
        return None;
    }
    Some((bid_qty - ask_qty) / total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balanced_book_is_zero() {
        let bids = [(100.0, 5.0)];
        let asks = [(101.0, 5.0)];
        assert_eq!(compute_imbalance(&bids, &asks, 5), Some(0.0));
    }

    #[test]
    fn buy_pressure_is_positive() {
        let bids = [(100.0, 9.0)];
        let asks = [(101.0, 1.0)];
        assert_eq!(compute_imbalance(&bids, &asks, 5), Some(0.8));
    }

    #[test]
    fn empty_book_is_none() {
        assert_eq!(compute_imbalance(&[], &[], 5), None);
    }

    #[test]
    fn respects_level_cap() {
        let bids = [(100.0, 1.0), (99.0, 100.0)];
        let asks = [(101.0, 1.0), (102.0, 100.0)];
        // Only the first level on each side counts -> balanced.
        assert_eq!(compute_imbalance(&bids, &asks, 1), Some(0.0));
    }
}
