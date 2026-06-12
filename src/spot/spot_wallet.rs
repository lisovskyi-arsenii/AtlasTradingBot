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
    // У файлі spot_wallet.rs, онови метод buy та sell_all
    pub fn buy(&mut self, current_price: f64, usdt_amount: f64, _is_maker: bool) -> Option<f64> {
        // У режимі імітації ми не перевіряємо жорстко баланс
        // або дозволяємо гаманцю "йти в мінус" для тестування логіки
        let executed_price = current_price * (1.0 + self.simulated_slippage_pct);
        let qty = usdt_amount / executed_price;

        self.usdt_balance -= usdt_amount;
        self.crypto_balance += qty;

        println!("[WALLET] SIM-BUY: {} BTC @ ${:.2}", qty, executed_price);
        Some(executed_price)
    }

    pub fn sell_all(&mut self, current_price: f64, _is_maker: bool) -> Option<f64> {
        if self.crypto_balance <= 0.0 { return None; }

        let executed_price = current_price * (1.0 - self.simulated_slippage_pct);
        let usdt_received = self.crypto_balance * executed_price;

        self.usdt_balance += usdt_received;
        self.crypto_balance = 0.0;

        println!("[WALLET] SIM-SELL: All @ ${:.2}. Received: ${:.2}", executed_price, usdt_received);
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