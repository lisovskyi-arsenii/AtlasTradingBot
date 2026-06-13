# Monitoring and Dashboarding

The bot provides professional-grade monitoring with Prometheus metrics and Grafana dashboards, plus CSV logging for offline analysis.

## Prometheus + Grafana (Recommended for Live Trading)

### Quick Start

1. Start the monitoring stack:
```bash
docker-compose up -d
```

2. Run the bot with metrics enabled (default port 9100):
```bash
cargo run --release
```

3. Access dashboards:
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

### Available Metrics

The bot exports comprehensive trading metrics:

**Portfolio-level:**
- `bot_portfolio_equity_usd` - Total equity across all symbols
- `bot_portfolio_pnl_pct` - Portfolio PnL as percentage
- `bot_portfolio_pnl_usd` - Portfolio PnL in USDT
- `bot_active_symbols` - Number of active trading symbols

**Per-symbol:**
- `bot_equity_usd` - Current equity (mark-to-market)
- `bot_realized_pnl_usd` - Cumulative realized PnL
- `bot_pnl_pct` - PnL as percentage of initial capital
- `bot_drawdown_pct` - Current drawdown from peak
- `bot_rsi` - Latest RSI value
- `bot_z_score` - Latest Z-score
- `bot_atr` - Latest ATR value
- `bot_atr_pct` - ATR as percentage of price
- `bot_trade_count` - Number of completed trades
- `bot_unrealized_pnl_usd` - Unrealized PnL of open position
- `bot_position_side` - Position: 0=FLAT, 1=LONG, -1=SHORT
- `bot_wallet_usdt` - Free USDT balance
- `bot_last_price` - Last candle close price
- `bot_peak_equity_usd` - Peak equity high-water mark
- `bot_candle_count` - Candles processed
- `bot_drawdown_stop_active` - 1 if max-drawdown stop is active

### Dashboard Panels

The pre-configured Grafana dashboard includes:

1. **Portfolio PnL %** - Gauge showing overall performance
2. **Portfolio Equity** - Time series of total equity
3. **Drawdown by Symbol** - Per-symbol drawdown tracking
4. **Equity by Symbol** - Individual symbol performance
5. **Trade Count** - Total trades per symbol
6. **Position Status** - Current position (FLAT/LONG/SHORT)
7. **RSI by Symbol** - RSI indicator values

### Configuration

In `config.toml`:

```toml
[runtime]
metrics_port = 9100  # Set to 0 to disable
```

## CSV Logging (Offline Analysis)

The bot also writes live performance data to CSV for offline analysis.

### What is logged

Each row contains:

- candle OHLC values
- indicator values
- entry/exit signals
- current equity
- unrealized/realized PnL metrics
- peak equity and drawdown
- trade count

### Local analysis script

For a fast offline review, run:

```bash
python3 tools/analyze_trading_bot_csv.py trading_bot.csv --output-dir reports
```

This generates a Markdown summary and, if `matplotlib` is available, PNG charts for equity, drawdown, price vs EMA, and PnL%.

## Practical workflow

- Run a backtest and record the expected result.
- Run the bot live with Prometheus/Grafana monitoring.
- Compare live metrics against backtest expectations.
- If live PnL drifts materially, inspect slippage, fees, and signal quality.

## Config knobs

In `config.toml`:

```toml
[runtime]
live_log_path = "trading_bot.csv"
candle_timeframe_seconds = 900
poll_interval_seconds = 3
metrics_port = 9100  # Prometheus metrics endpoint
```

