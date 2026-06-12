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
            CryptoExchange::Binance => (0.001, 0.001),     // 0.1% standard
            CryptoExchange::Bybit => (0.001, 0.001),       // 0.1% spot
            CryptoExchange::Whitebit => (0.001, 0.001),    // 0.1% standard
        };

        let slippage = match exchange {
            CryptoExchange::Binance => 0.0001,   // 0.01% — high liquidity
            CryptoExchange::Bybit => 0.00015,    // 0.015%
            CryptoExchange::Whitebit => 0.0003,  // 0.03% — lower liquidity
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

    fn ceil_to_tick(value: f64, tick: f64) -> f64 {
        if tick <= 0.0 {
            return value;
        }
        (value / tick).ceil() * tick
    }

    fn floor_to_tick(value: f64, tick: f64) -> f64 {
        if tick <= 0.0 {
            return value;
        }
        (value / tick).floor() * tick
    }

    /// Realistic buy simulation with exchange-specific fees and slippage
    pub fn buy(&mut self, current_price: f64, usdt_amount: f64, is_maker: bool) -> Option<f64> {
        if usdt_amount <= 0.0 || self.usdt_balance < usdt_amount {
            return None;
        }

        // Apply slippage: price moves up when buying
        let raw_price = current_price * (1.0 + self.simulated_slippage_pct);
        let executed_price = Self::ceil_to_tick(raw_price, self.filters.tick_size);

        let fee_pct = self.effective_fee_pct(is_maker);
        let gross_qty = usdt_amount / executed_price;
        let qty = Self::floor_to_step(gross_qty, self.filters.step_size);

        if qty <= 0.0 {
            return None;
        }

        let notional = qty * executed_price;
        if notional > self.usdt_balance {
            return None;
        }

        if notional < self.filters.min_notional {
            return None;
        }

        // Fee in crypto (taker typically)
        let fee_in_crypto = qty * fee_pct;
        let actual_crypto = qty - fee_in_crypto;

        if actual_crypto <= 0.0 {
            return None;
        }

        self.usdt_balance -= notional;
        self.crypto_balance += actual_crypto;

        println!(
            "[WALLET] BUY {} {:.6} @ ${:.2} (slipped from ${:.2}). Fee: {:.6} {}. Balance: ${:.2}",
            self.exchange_name(),
            actual_crypto,
            executed_price,
            current_price,
            fee_in_crypto,
            self.symbol_name(),
            self.usdt_balance,
        );

        Some(executed_price)
    }

    /// Realistic sell simulation with exchange-specific fees and slippage
    pub fn sell_all(&mut self, current_price: f64, is_maker: bool) -> Option<f64> {
        if self.crypto_balance <= 0.0 {
            return None;
        }

        // Apply slippage: price moves down when selling
        let raw_price = current_price * (1.0 - self.simulated_slippage_pct);
        let executed_price = Self::floor_to_tick(raw_price, self.filters.tick_size);

        let qty = Self::floor_to_step(self.crypto_balance, self.filters.step_size);
        if qty <= 0.0 {
            return None;
        }

        let notional = qty * executed_price;
        if notional < self.filters.min_notional {
            return None;
        }

        let fee_pct = self.effective_fee_pct(is_maker);
        let fee_in_usdt = notional * fee_pct;
        let net_usdt = notional - fee_in_usdt;

        self.crypto_balance = (self.crypto_balance - qty).max(0.0);
        self.usdt_balance += net_usdt;

        println!(
            "[WALLET] SELL {} {:.6} @ ${:.2} (slipped from ${:.2}). Fee: ${:.4}. Balance: ${:.2}",
            self.exchange_name(),
            qty,
            executed_price,
            current_price,
            fee_in_usdt,
            self.usdt_balance,
        );

        Some(executed_price)
    }

    pub fn total_value(&self, market_price: f64) -> f64 {
        self.usdt_balance + self.crypto_balance * market_price
    }

    fn exchange_name(&self) -> &'static str {
        match self.exchange {
            CryptoExchange::Binance => "BINANCE",
            CryptoExchange::Bybit => "BYBIT",
            CryptoExchange::Whitebit => "WHITEBIT",
        }
    }

    fn symbol_name(&self) -> &'static str {
        match self.exchange {
            CryptoExchange::Binance => "BTC",
            CryptoExchange::Bybit => "BTC",
            CryptoExchange::Whitebit => "BTC",
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

        let executed_price = wallet.buy(100.004, 100.0, false).expect("buy should execute");
        assert_eq!(executed_price, 100.01);
    }

    #[test]
    fn sell_rounds_price_down_to_tick_for_conservative_fill() {
        let mut wallet = Wallet::new(1000.0, CryptoExchange::Binance);
        wallet.simulated_slippage_pct = 0.0;

        let _ = wallet.buy(100.00, 100.0, false).expect("buy should execute");
        let executed_price = wallet.sell_all(100.006, false).expect("sell should execute");
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
}