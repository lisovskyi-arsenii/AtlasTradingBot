# Monitoring and Dashboarding

The bot writes live performance data to a CSV file defined in `config.toml` under `[runtime].live_log_path`.

## What is logged

Each row contains:

- candle OHLC values
- indicator values
- entry/exit signals
- current equity
- unrealized/realized PnL metrics
- peak equity and drawdown
- trade count

That is enough to build a simple real-time dashboard.

## Grafana option

1. Export the CSV into a location Grafana can read.
2. Use a CSV datasource plugin or ingest the file into a database such as PostgreSQL or SQLite.
3. Plot:
   - `equity`
   - `pnl_pct`
   - `drawdown_pct`
   - `trade_count`
   - `close`

Recommended panels:

- Equity curve vs time
- Drawdown % vs time
- Price vs SMA/Z-score
- Trade count / action markers

## Google Sheets option

1. Open Google Sheets.
2. Import the live CSV or sync it via an Apps Script / CSV import.
3. Create charts for:
   - `equity`
   - `pnl_pct`
   - `drawdown_pct`
   - `close`

      ## Local analysis script

      For a fast offline review, run:

      ```bash
      python3 tools/analyze_trading_bot_csv.py trading_bot.csv --output-dir reports
      ```

      This generates a Markdown summary and, if `matplotlib` is available, PNG charts for equity, drawdown, price vs EMA, and PnL%.

## Practical workflow

- Run a backtest and record the expected result.
- Run the bot live and compare `pnl_pct` / `equity` in the dashboard.
- If live PnL drifts materially from backtest expectations, inspect slippage, fees, and signal quality.

## Config knobs

In `config.toml`:

```toml
[runtime]
live_log_path = "trading_bot.csv"
candle_timeframe_seconds = 900
poll_interval_seconds = 3
```

You can point Grafana or Sheets at that file and refresh it continuously.

