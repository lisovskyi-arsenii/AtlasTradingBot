use crate::models::data::PositionType;

pub struct FuturesWallet {
    pub margin_balance: f64, // гроші
    pub initial_margin: f64, // зафіксований при відкритті позиції
    pub position_type: PositionType, // поточний стан (long, short, none)
    pub leverage: f64, // кредитне плече
    pub entry_price: f64, // ціна, за якою відкрили позицію (0.0 за замовчуванням)
    pub position_size: f64 // об'єм позиції в USDT з урахуванням плеча (0.0 за замовчуванням)
}

impl FuturesWallet {
    pub fn new(start_margin: f64, leverage: f64) -> Self {
        Self {
            margin_balance: start_margin,
            initial_margin: 0.0,
            position_type: PositionType::None,
            leverage,
            entry_price: 0.0,
            position_size: 0.0,
        }
    }

    pub fn open_position(&mut self, position_type: PositionType, price: f64) {
        if self.position_type != PositionType::None {
            println!("Position is already open. Please close it before trying to open it again.");
            return;
        }

        self.position_type = position_type;
        self.entry_price = price;
        self.position_size = self.margin_balance * self.leverage;
        self.initial_margin = self.margin_balance;

        println!(
            "Opened {:?} | leverage: {}x | size: ${:.2} | entry: ${:.2}",
            self.position_type, self.leverage, self.position_size, self.entry_price
        );
    }

    pub fn close_position(&mut self, current_price: f64) {
        if self.position_type == PositionType::None {
            println!("Position is already close. Please open it before trying to close it.");
            return;
        }

        let price_change: f64 = (current_price - self.entry_price) / self.entry_price;
        let pnl: f64 = self.count_pnl(price_change);

        self.margin_balance += pnl;

        println!(
            "Closed {:?} at ${:.2} | PnL: ${:.2} ({:.2}%) | Balance: ${:.2}",
            self.position_type,
            current_price,
            pnl,
            (pnl / self.initial_margin) * 100.0,
            self.margin_balance
        );

        self.reset_position();
    }

    pub fn check_liquidation(&mut self, current_price: f64) -> bool {
        if self.position_type == PositionType::None {
            println!("No need for checking liquidation because position isn't opened yet.");
            return false
        }

        let price_change: f64 = (current_price - self.entry_price) / self.entry_price;
        let unrealized_pnl: f64 = self.count_pnl(price_change);

        if unrealized_pnl <= -self.initial_margin {
            println!(
                "LIQUIDATION!!! {:?} at ${:.2} | Lost: ${:.2}",
                self.position_type, current_price, self.initial_margin
            );

            self.margin_balance = 0.0;
            self.reset_position();
            return true
        }

        false
    }

    fn count_pnl(&self, price_change: f64) -> f64 {
        match self.position_type {
            PositionType::Long => self.position_size * price_change,
            PositionType::Short => -self.position_size * price_change,
            PositionType::None => 0.0
        }
    }

    fn reset_position(&mut self) {
        self.position_type = PositionType::None;
        self.entry_price = 0.0;
        self.position_size = 0.0;
        self.initial_margin = 0.0;
    }
}
