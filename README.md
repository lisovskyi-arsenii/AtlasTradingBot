# AtlasTradingBot

## Runtime configuration

Strategy parameters and runtime settings are loaded from `config.toml` at startup, so you can change values like `z_entry`, `slow_period`, `atr_multiplier`, candle timeframe, and cooldowns without recompiling.

### Files

- `config.toml` — default runtime configuration
- `MONITORING.md` — simple CSV dashboard setup

### Override config path

You can point the bot at another config file:

```bash
cargo run -- --config path/to/custom.toml
```

### Example sections

```toml
[bot]
mode = "spot"
exchange = "binance"
symbol = "BTCUSDT"
margin = 1000.0

[strategy]
z_entry = -1.2
short_z_entry = 1.3
slow_period = 40

[runtime]
live_log_path = "trading_bot.csv"
candle_timeframe_seconds = 900
```

Any CLI flags like `--mode`, `--exchange`, `--symbol`, `--margin`, and `--leverage` still override values from the TOML file.

See `MONITORING.md` for Grafana / Google Sheets dashboard ideas based on the CSV log.

### Quick CSV analysis

You can generate a local report and charts from `trading_bot.csv`:

```bash
python3 tools/analyze_trading_bot_csv.py trading_bot.csv --output-dir reports
```

If `matplotlib` is installed, the script also creates PNG charts for equity, drawdown, price vs EMA, and PnL%.

