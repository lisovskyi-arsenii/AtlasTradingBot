use crate::models::data::CryptoExchange;

#[derive(Debug, Clone)]
pub struct SymbolFilters {
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: f64,
}

impl SymbolFilters {
    pub fn for_exchange(exchange: CryptoExchange) -> Self {
        match exchange {
            CryptoExchange::Binance => Self {
                tick_size: 0.01,
                step_size: 0.00001,
                min_notional: 10.0,
            },
            CryptoExchange::Bybit => Self {
                tick_size: 0.01,
                step_size: 0.00001,
                min_notional: 5.0,
            },
            CryptoExchange::Whitebit => Self {
                tick_size: 0.0001,
                step_size: 0.000001,
                min_notional: 1.0,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct Wallet {
    pub usdt_balance: f64,
    pub crypto_balance: f64,
    pub maker_fee_pct: f64,
    pub taker_fee_pct: f64,
    pub use_bnb_discount: bool,
    pub simulated_slippage_pct: f64,
    pub filters: SymbolFilters,
    pub exchange: CryptoExchange,
}

impl Wallet {
    pub fn new(start_usdt: f64, exchange: CryptoExchange) -> Self {
        let (maker, taker) = match exchange {
            CryptoExchange::Binance => (0.001, 0.001),
            CryptoExchange::Bybit => (0.001, 0.001),
            CryptoExchange::Whitebit => (0.001, 0.001),
        };

        let slippage = match exchange {
            CryptoExchange::Binance => 0.0001,  // 0.01% — high liquidity
            CryptoExchange::Bybit => 0.00015,   // 0.015%
            CryptoExchange::Whitebit => 0.0003, // 0.03% — lower liquidity
        };

        Self {
            usdt_balance: start_usdt,
            crypto_balance: 0.0,
            maker_fee_pct: maker,
            taker_fee_pct: taker,
            use_bnb_discount: false,
            simulated_slippage_pct: slippage,
            filters: SymbolFilters::for_exchange(exchange),
            exchange,
        }
    }

    pub fn new_binance_spot(start_usdt: f64) -> Self {
        Self::new(start_usdt, CryptoExchange::Binance)
    }

    fn effective_fee_pct(&self, is_maker: bool) -> f64 {
        let base = if is_maker {
            self.maker_fee_pct
        } else {
            self.taker_fee_pct
        };
        if self.use_bnb_discount && self.exchange == CryptoExchange::Binance {
            base * 0.75
        } else {
            base
        }
    }

    fn floor_to_step(value: f64, step: f64) -> f64 {
        if step <= 0.0 {
            return value;
        }
        (value / step).floor() * step
    }

    /// Effective price tick. The configured `tick_size` is a single hardcoded value
    /// (0.01) applied to every symbol, which is catastrophic for low-priced coins:
    /// e.g. for PEPE (~$0.00001) it rounds the price to 0 (a -100% fill), and for
    /// sub-dollar coins it adds several percent of artificial round-trip cost. Use
    /// the finer of the configured tick and a price-relative tick (~5 significant
    /// figures) so high-priced assets are unchanged while small-priced ones stop
    /// being distorted.
    fn effective_tick(price: f64, tick: f64) -> f64 {
        let p = price.abs();
        // Use the configured tick only while it is fine-grained relative to price
        // (<= 0.1% of price). Otherwise it is too coarse for this symbol's scale, so
        // fall back to a price-relative tick (~5 significant figures).
        if tick > 0.0 && tick <= p * 1e-3 {
            tick
        } else if p > 0.0 {
            p * 1e-5
        } else {
            tick.max(0.0)
        }
    }

    fn ceil_to_tick(value: f64, tick: f64) -> f64 {
        let tick = Self::effective_tick(value, tick);
        if tick <= 0.0 {
            return value;
        }
        (value / tick).ceil() * tick
    }

    fn floor_to_tick(value: f64, tick: f64) -> f64 {
        let tick = Self::effective_tick(value, tick);
        if tick <= 0.0 {
            return value;
        }
        (value / tick).floor() * tick
    }

    /// Simulated buy with:
    ///  - balance guard (returns None if insufficient funds)
    ///  - upward slippage (taker fills at a slightly worse price)
    ///  - tick rounding (ceil — conservative, never pay less than market)
    ///  - fee deducted from received crypto
    pub fn buy(&mut self, current_price: f64, usdt_amount: f64, is_maker: bool) -> Option<f64> {
        // FIX: guard against over-spending — was silently going negative before
        if usdt_amount > self.usdt_balance {
            return None;
        }
        if usdt_amount <= 0.0 {
            return None;
        }

        // Slippage: buyer fills at a slightly higher price
        let raw_price = current_price * (1.0 + self.simulated_slippage_pct);

        // FIX: tick rounding restored so conservative fill tests pass
        let executed_price = Self::ceil_to_tick(raw_price, self.filters.tick_size);

        let fee_pct = self.effective_fee_pct(is_maker);
        let qty_gross = usdt_amount / executed_price;
        let qty_net = Self::floor_to_step(qty_gross * (1.0 - fee_pct), self.filters.step_size);

        if qty_net * executed_price < self.filters.min_notional {
            return None;
        }

        self.usdt_balance -= usdt_amount;
        self.crypto_balance += qty_net;

        println!(
            "[WALLET] SIM-BUY: {:.6} @ ${:.4}  fee={:.4}%",
            qty_net,
            executed_price,
            fee_pct * 100.0
        );
        Some(executed_price)
    }

    /// Simulated sell-all with:
    ///  - downward slippage
    ///  - tick rounding (floor — conservative, never receive more than market)
    ///  - fee deducted from received USDT
    pub fn sell_all(&mut self, current_price: f64, is_maker: bool) -> Option<f64> {
        if self.crypto_balance <= 0.0 {
            return None;
        }

        // Slippage: seller fills at a slightly lower price
        let raw_price = current_price * (1.0 - self.simulated_slippage_pct);

        // FIX: tick rounding restored
        let executed_price = Self::floor_to_tick(raw_price, self.filters.tick_size);

        let fee_pct = self.effective_fee_pct(is_maker);
        let usdt_gross = self.crypto_balance * executed_price;
        let usdt_received = usdt_gross * (1.0 - fee_pct);

        self.usdt_balance += usdt_received;
        self.crypto_balance = 0.0;

        println!(
            "[WALLET] SIM-SELL: All @ ${:.4}  received ${:.2}  fee={:.4}%",
            executed_price,
            usdt_received,
            fee_pct * 100.0
        );
        Some(executed_price)
    }

    pub fn total_value(&self, market_price: f64) -> f64 {
        self.usdt_balance + self.crypto_balance * market_price
    }

    #[allow(dead_code)]
    fn exchange_name(&self) -> &'static str {
        match self.exchange {
            CryptoExchange::Binance => "BINANCE",
            CryptoExchange::Bybit => "BYBIT",
            CryptoExchange::Whitebit => "WHITEBIT",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buy_rounds_price_up_to_tick_for_conservative_fill() {
        let mut wallet = Wallet::new(1000.0, CryptoExchange::Binance);
        wallet.simulated_slippage_pct = 0.0;
        wallet.maker_fee_pct = 0.0;
        wallet.taker_fee_pct = 0.0;

        // 100.004 → ceil to tick 0.01 → 100.01
        let executed_price = wallet
            .buy(100.004, 100.0, false)
            .expect("buy should execute");
        assert_eq!(executed_price, 100.01);
    }

    #[test]
    fn sell_rounds_price_down_to_tick_for_conservative_fill() {
        let mut wallet = Wallet::new(1000.0, CryptoExchange::Binance);
        wallet.simulated_slippage_pct = 0.0;
        wallet.maker_fee_pct = 0.0;
        wallet.taker_fee_pct = 0.0;

        let _ = wallet
            .buy(100.00, 100.0, false)
            .expect("buy should execute");

        // 100.006 → floor to tick 0.01 → 100.00
        let executed_price = wallet
            .sell_all(100.006, false)
            .expect("sell should execute");
        assert_eq!(executed_price, 100.00);
    }

    #[test]
    fn bybit_filters_work() {
        let mut wallet = Wallet::new(500.0, CryptoExchange::Bybit);
        wallet.simulated_slippage_pct = 0.0;

        let executed = wallet.buy(50000.0, 100.0, false);
        assert!(executed.is_some(), "Bybit buy should work");
    }

    #[test]
    fn whitebit_filters_work() {
        let mut wallet = Wallet::new(500.0, CryptoExchange::Whitebit);
        wallet.simulated_slippage_pct = 0.0;

        let executed = wallet.buy(50000.0, 100.0, false);
        assert!(executed.is_some(), "Whitebit buy should work");
    }

    /// FIX: buy_not_enough_balance — was silently going negative; now returns None
    #[test]
    fn buy_not_enough_balance() {
        let mut wallet = Wallet::new(100.0, CryptoExchange::Binance);
        let result = wallet.buy(50000.0, 500.0, false);
        assert!(result.is_none(), "Should not buy with insufficient balance");
    }

    #[test]
    fn sell_without_crypto() {
        let mut wallet = Wallet::new(1000.0, CryptoExchange::Binance);
        let result = wallet.sell_all(50000.0, false);
        assert!(result.is_none(), "Should not sell without crypto");
    }

    #[test]
    fn round_trip_preserves_value_approximately() {
        // buy then immediately sell at same price — only fees eaten
        let mut wallet = Wallet::new(1000.0, CryptoExchange::Binance);
        wallet.simulated_slippage_pct = 0.0;

        wallet.buy(100.0, 500.0, false).unwrap();
        wallet.sell_all(100.0, false).unwrap();

        // 2 × 0.1% fee = ~0.2% loss
        let total = wallet.total_value(100.0);
        assert!(
            total > 998.0 && total < 1000.0,
            "Round-trip value: {}",
            total
        );
    }
}
