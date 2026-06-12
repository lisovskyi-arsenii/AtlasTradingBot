#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Quiet,
    Normal,
    Debug,
}

impl LogLevel {
    pub fn from_env() -> Self {
        match std::env::var("BOT_LOG")
            .unwrap_or_else(|_| "normal".to_string())
            .to_lowercase()
            .as_str()
        {
            "quiet" => Self::Quiet,
            "debug" => Self::Debug,
            _ => Self::Normal,
        }
    }

    pub fn allows_normal(self) -> bool {
        matches!(self, Self::Normal | Self::Debug)
    }

    pub fn allows_debug(self) -> bool {
        matches!(self, Self::Debug)
    }
}
