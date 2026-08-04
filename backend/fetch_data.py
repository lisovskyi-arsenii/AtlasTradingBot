#!/usr/bin/env python3
"""
Binance Historical Data Downloader for RustBot Backtesting.

Usage:
    python fetch_data.py --symbol BTCUSDT --interval 15m --start 2026-01-01 --end 2026-05-31
    python fetch_data.py --symbol ETHUSDT --interval 15m --start 2026-01-01 --end 2026-05-31
    python fetch_data.py --symbol SOLUSDT --interval 15m --start 2026-01-01 --end 2026-05-31

    # Download ALL major pairs for full backtest suite:
    python fetch_data.py --all
"""

import argparse
import csv
import os
import time
from datetime import datetime, timedelta
from urllib.request import urlopen
import json

BINANCE_BASE = "https://api.binance.com/api/v3"

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "ADAUSDT"]
INTERVALS = ["15m", "1h", "4h"]
INTERVAL_MAP = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "8h": 480,
    "12h": 720, "1d": 1440, "3d": 4320, "1w": 10080, "1mo": 43200
}


def ms_to_dt(ms):
    return datetime.fromtimestamp(ms / 1000.0)


def dt_to_ms(dt):
    return int(dt.timestamp() * 1000)


def fetch_klines(symbol, interval, start_ms, end_ms, limit=1000):
    """Fetch klines from Binance REST API."""
    url = f"{BINANCE_BASE}/klines?symbol={symbol}&interval={interval}&startTime={start_ms}&endTime={end_ms}&limit={limit}"
    try:
        with urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        return data
    except Exception as e:
        print(f"  [ERROR] {e}")
        return []


def download_symbol_interval(symbol, interval, start_date, end_date, output_dir="data"):
    """Download historical data for one symbol+interval into a CSV file."""
    os.makedirs(output_dir, exist_ok=True)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Create filename: SYMBOL-INTERVAL-START-END.csv
    filename = f"{symbol}-{interval}-{start_date}-{end_date}.csv"
    filepath = os.path.join(output_dir, filename)

    print(f"\n{'='*60}")
    print(f"Downloading {symbol} {interval} ({start_date} -> {end_date})")
    print(f"{'='*60}")

    start_ms = dt_to_ms(start_dt)
    end_ms = dt_to_ms(end_dt)

    all_candles = []
    current_ms = start_ms
    request_count = 0

    while current_ms < end_ms:
        klines = fetch_klines(symbol, interval, current_ms, end_ms)
        request_count += 1

        if not klines:
            print(f"  No data returned. Stopping.")
            break

        for k in klines:
            ts = k[0]
            if ts > end_ms:
                break
            if ts >= start_ms:
                all_candles.append(k)
            current_ms = max(current_ms, ts + 1)

        # Check if we got less than limit (means end of data)
        if len(klines) < 1000:
            break

        # Binance rate limit: ~1200 requests per minute
        if request_count % 100 == 0:
            print(f"  Progress: {ms_to_dt(current_ms)} ({len(all_candles)} candles)...")
            time.sleep(0.5)
        else:
            time.sleep(0.1)

    if not all_candles:
        print(f"  [WARN] No candles downloaded for {symbol} {interval}")
        return

    # Write CSV (format: timestamp,open,high,low,close,volume)
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        for c in all_candles:
            ts_ms = c[0]
            open_p = c[1]
            high = c[2]
            low = c[3]
            close = c[4]
            volume = c[5]
            writer.writerow([ts_ms, open_p, high, low, close, volume])

    print(f"\n  ✅ Saved {len(all_candles)} candles to {filepath}")
    print(f"     Range: {ms_to_dt(all_candles[0][0])} -> {ms_to_dt(all_candles[-1][0])}")


def main():
    parser = argparse.ArgumentParser(description="Binance Historical Data Downloader for RustBot")
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair (e.g. BTCUSDT, ETHUSDT)")
    parser.add_argument("--interval", default="15m", help="Timeframe (1m, 5m, 15m, 1h, 4h, 1d)")
    parser.add_argument("--start", default="2026-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-05-31", help="End date (YYYY-MM-DD)")
    parser.add_argument("--all", action="store_true", help="Download all major pairs")
    parser.add_argument("--output", default=".", help="Output directory")

    args = parser.parse_args()

    if args.all:
        print("╔════════════════════════════════════════════════╗")
        print("║  Binance Multi-Symbol Data Downloader         ║")
        print("╚════════════════════════════════════════════════╝")
        print(f"\nDownloading {len(SYMBOLS)} symbols × {len(INTERVALS)} intervals")
        print(f"Date range: {args.start} -> {args.end}")
        print()

        total_est = len(SYMBOLS) * len(INTERVALS)
        count = 0
        for symbol in SYMBOLS:
            for interval in INTERVALS:
                count += 1
                print(f"\n[{count}/{total_est}] Downloading...")
                download_symbol_interval(symbol, interval, args.start, args.end, args.output)
                time.sleep(0.5)  # Be nice to Binance API

        print(f"\n{'='*60}")
        print(f"✅ ALL DOWNLOADS COMPLETE!")
        print(f"   Files saved to: {os.path.abspath(args.output)}/")
        print(f"   To run backtest: RUN_ALL_CSV=true cargo run -- --margin 10000 --symbol BTCUSDT")
    else:
        download_symbol_interval(args.symbol, args.interval, args.start, args.end, args.output)

    # Print summary of what's now available
    print(f"\n{'='*60}")
    print(f"Files in '{args.output}/':")
    for f in sorted(os.listdir(args.output)):
        if f.endswith(".csv"):
            size_mb = os.path.getsize(os.path.join(args.output, f)) / (1024 * 1024)
            print(f"  {f:<50} {size_mb:.1f} MB")


if __name__ == "__main__":
    main()