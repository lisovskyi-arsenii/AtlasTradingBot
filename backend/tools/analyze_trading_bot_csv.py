#!/usr/bin/env python3
"""Analyze trading_bot.csv and optionally generate simple charts.

Usage:
    python3 tools/analyze_trading_bot_csv.py trading_bot.csv
    python3 tools/analyze_trading_bot_csv.py trading_bot.csv --output-dir reports
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class Summary:
    row_count: int
    action_counts: Counter
    phase_counts: Counter
    no_signal_reasons: Counter
    first_timestamp: str
    last_timestamp: str
    final_equity: float
    initial_equity: float
    max_equity: float
    min_equity: float
    max_drawdown_pct: float
    final_pnl_pct: float
    realized_pnl_usdt: float
    trade_count: int
    long_rows: int
    short_rows: int
    flat_rows: int


def _to_float(row: Dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        value = row.get(key, "")
        return float(value) if value not in (None, "") else default
    except ValueError:
        return default


def _to_int(row: Dict[str, str], key: str, default: int = 0) -> int:
    try:
        value = row.get(key, "")
        return int(float(value)) if value not in (None, "") else default
    except ValueError:
        return default


def load_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def compute_summary(rows: List[Dict[str, str]]) -> Summary:
    if not rows:
        raise ValueError("CSV is empty")

    action_counts = Counter(row.get("action", "") for row in rows)
    phase_counts = Counter(row.get("phase", "") for row in rows)
    no_signal_reasons = Counter(row.get("no_signal_reason", "") for row in rows if row.get("action", "") == "NO_SIGNAL")

    equities = [_to_float(row, "equity") for row in rows]
    pnls_pct = [_to_float(row, "pnl_pct") for row in rows]
    realized_pnl_usdt = _to_float(rows[-1], "realized_pnl_usdt")

    peak = equities[0]
    max_drawdown_pct = 0.0
    for equity in equities:
        if equity > peak:
            peak = equity
        if peak > 0.0:
            drawdown_pct = (peak - equity) / peak * 100.0
            if drawdown_pct > max_drawdown_pct:
                max_drawdown_pct = drawdown_pct

    long_rows = sum(1 for row in rows if row.get("position_type", "") == "LONG")
    short_rows = sum(1 for row in rows if row.get("position_type", "") == "SHORT")
    flat_rows = sum(1 for row in rows if row.get("position_type", "") == "NONE")

    return Summary(
        row_count=len(rows),
        action_counts=action_counts,
        phase_counts=phase_counts,
        no_signal_reasons=no_signal_reasons,
        first_timestamp=rows[0].get("timestamp", ""),
        last_timestamp=rows[-1].get("timestamp", ""),
        final_equity=equities[-1],
        initial_equity=equities[0],
        max_equity=max(equities),
        min_equity=min(equities),
        max_drawdown_pct=max_drawdown_pct,
        final_pnl_pct=pnls_pct[-1],
        realized_pnl_usdt=realized_pnl_usdt,
        trade_count=_to_int(rows[-1], "trade_count"),
        long_rows=long_rows,
        short_rows=short_rows,
        flat_rows=flat_rows,
    )


def write_markdown_report(output_path: Path, csv_path: Path, summary: Summary, rows: List[Dict[str, str]]) -> None:
    last = rows[-1]
    top_reasons = summary.no_signal_reasons.most_common(10)
    top_actions = summary.action_counts.most_common()

    md = [
        f"# Trading Bot CSV Report\n",
        f"Source file: `{csv_path}`\n",
        f"Rows: **{summary.row_count}**\n",
        f"First timestamp: `{summary.first_timestamp}`\n",
        f"Last timestamp: `{summary.last_timestamp}`\n",
        "## Core performance\n",
        f"- Initial equity: `${summary.initial_equity:.2f}`\n",
        f"- Final equity: `${summary.final_equity:.2f}`\n",
        f"- Final PnL: **{summary.final_pnl_pct:+.2f}%**\n",
        f"- Realized PnL: `${summary.realized_pnl_usdt:.2f}`\n",
        f"- Max equity: `${summary.max_equity:.2f}`\n",
        f"- Min equity: `${summary.min_equity:.2f}`\n",
        f"- Max drawdown: **{summary.max_drawdown_pct:.2f}%**\n",
        f"- Total trades recorded: **{summary.trade_count}**\n",
        "\n## Position distribution\n",
        f"- Long rows: **{summary.long_rows}**\n",
        f"- Short rows: **{summary.short_rows}**\n",
        f"- Flat rows: **{summary.flat_rows}**\n",
        "\n## Actions\n",
    ]
    for action, count in top_actions:
        md.append(f"- {action or 'EMPTY'}: {count}\n")

    md.append("\n## Top no-signal reasons\n")
    for reason, count in top_reasons:
        md.append(f"- {reason or 'EMPTY'}: {count}\n")

    md.extend([
        "\n## Last snapshot\n",
        f"- Timestamp: `{last.get('timestamp', '')}`\n",
        f"- Action: `{last.get('action', '')}`\n",
        f"- Position: `{last.get('position_type', '')}`\n",
        f"- Equity: `${_to_float(last, 'equity'):.2f}`\n",
        f"- Drawdown: `{_to_float(last, 'drawdown_pct'):.2f}%`\n",
        f"- Close: `${_to_float(last, 'close'):.2f}`\n",
        f"- Z-score: `{_to_float(last, 'z_score'):.4f}`\n",
        f"- EMA: `${_to_float(last, 'ema_value'):.2f}`\n",
        f"- ATR%: `{_to_float(last, 'atr_pct'):.4f}%`\n",
    ])

    output_path.write_text("".join(md), encoding="utf-8")


def maybe_write_charts(output_dir: Path, rows: List[Dict[str, str]]) -> Optional[List[Path]]:
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)

    indices = [_to_int(row, "bar_index", idx) for idx, row in enumerate(rows)]
    equity = [_to_float(row, "equity") for row in rows]
    drawdown = [_to_float(row, "drawdown_pct") for row in rows]
    close = [_to_float(row, "close") for row in rows]
    ema = [_to_float(row, "ema_value") for row in rows]
    pnl_pct = [_to_float(row, "pnl_pct") for row in rows]

    written: List[Path] = []

    def save_plot(filename: str, title: str, series: List[tuple[str, List[float]]], ylabel: str) -> None:
        plt.figure(figsize=(12, 6))
        for label, values in series:
            plt.plot(indices, values, label=label, linewidth=1.4)
        plt.title(title)
        plt.xlabel("bar_index")
        plt.ylabel(ylabel)
        plt.grid(True, alpha=0.25)
        plt.legend()
        path = output_dir / filename
        plt.tight_layout()
        plt.savefig(path, dpi=150)
        plt.close()
        written.append(path)

    save_plot("equity_curve.png", "Equity Curve", [("equity", equity)], "USDT")
    save_plot("drawdown_curve.png", "Drawdown", [("drawdown_pct", drawdown)], "%")
    save_plot("price_vs_ema.png", "Price vs EMA", [("close", close), ("ema_value", ema)], "Price")
    save_plot("pnl_pct.png", "PnL %", [("pnl_pct", pnl_pct)], "%")

    return written


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze trading_bot.csv")
    parser.add_argument("csv_file", type=Path, help="Path to trading_bot.csv")
    parser.add_argument("--output-dir", type=Path, default=Path("reports"), help="Where to write the report and charts")
    args = parser.parse_args()

    rows = load_rows(args.csv_file)
    summary = compute_summary(rows)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    report_path = args.output_dir / "trading_bot_report.md"
    write_markdown_report(report_path, args.csv_file, summary, rows)

    charts = maybe_write_charts(args.output_dir, rows)

    print(f"Report written to: {report_path}")
    if charts:
        for chart in charts:
            print(f"Chart written to: {chart}")
    else:
        print("matplotlib not available; charts were skipped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

