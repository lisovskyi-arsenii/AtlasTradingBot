# AtlasTradingBot — Production Readiness Audit

**Date:** 2026-08-03 (Updated 2026-08-03 — Deep Code Analysis Pass)
**Role:** Senior Algorithmic Trading Engineer / System Architect / Security Auditor
**Verdict:** This is a research/backtest engine with a live price feed — **not** a production trading system. There is no signed order API, no exchange state reconciliation, and "LIVE" mode still only mutates an in-memory simulated wallet.

---

## System Profile (Inferred from Repo)

| Aspect | Reality |
|---|---|
| Exchanges | Binance primary; Bybit/WhiteBit price stubs only |
| Stack | Rust + Tokio, REST + WS, `ta`, CSV logs, Prometheus/Grafana |
| Strategy | Z-score mean-reversion (longs + simulated shorts), ATR/panic stops, optional filters |
| Live mode | Paper trading on Binance ticker/depth — **no real orders** |
| Futures | Struct exists but `final_equity()` / `total_trades()` are `todo!()` → process abort |

---

## 1. Critical Vulnerabilities (Money / Fatal)

### C1 — No real execution layer; "LIVE" is simulated
`Wallet::buy` / `sell_all` only adjust local balances and print `SIM-BUY` / `SIM-SELL`. There is no HMAC signing, no `/order` endpoint, no client order IDs, no fill confirmation. Flip `use_testnet = false` and you still trade air.

### C2 — Futures mode will crash in live
`FuturesTradingStrategy::final_equity` / `total_trades` are `todo!()`. Live loop calls `strategy.final_equity(...)` every candle close → process abort mid-session.

### C3 — Spot shorts are fake accounting, enabled by default
`enable_short = true` in `config.toml` while comments admit Binance Spot cannot short. Shorts reserve USDT in memory (`wallet.usdt_balance -= margin`). Wiring real spot orders on top of this logic will send invalid short sells or mis-account PnL.

### C4 — Backtest vs live timeframe / volume mismatch
- Batch download/warmup uses **1h** Binance klines.
- Live builds synthetic candles at `candle_timeframe_seconds = 900` (**15m**).
- Live candle `volume` is always **0** → volume filter / `vol_sma` are meaningless live.
Parameters tuned on 1h OHLC will not behave the same on 15m tick-aggregated bars.

### C5 — Auto-download "1 year" is capped at 1000 bars
`download_history_to_csv(..., 8760)` hits Binance once; API max is **1000**. Batch results claiming ~1y are often ~41 days of 1h data unless CSVs came from `fetch_data.py`.

### C6 — Optimistic stop fills in backtests
Stops exit at stop price when `low`/`high` pierces the level. Gaps, thin books, and adverse selection are not modeled. Live will underperform backtests systematically.

### C7 — Soft risk controls don't flatten risk
`max_strategy_drawdown_pct` only **blocks new entries**; open positions keep bleeding. No portfolio kill switch, no exchange-side stop orders.

### C8 — Daily loss tracking incomplete / ineffective
`daily_pnl` updates on **long** exits only — short exits skip it. Adaptive risk / daily limit partially blind. Daily reset uses wall-clock even in backtest context.

### C9 — OBI filter fails open
`latest_obi == None` → `order_book_confirms` returns **true**. Depth WS drop = silent disable of the filter. Stale books after reconnect are never invalidated.

### C10 — Metrics + Grafana exposed / weak defaults
- Metrics bind `0.0.0.0:9100` with no auth → position/equity leakage.
- Docker Grafana `admin`/`admin`.
- `.gitignore` only ignores `/target` and `*.csv` — secrets would be easy to commit once added.

### C11 — Exchange / testnet config ignored
`config.crypto_exchange` and `use_testnet` are parsed but live path always uses Binance mainnet WS + REST. Operator believes they are on testnet; they are not.

### C12 — Strategy asymmetry will churn shorts
`z_entry = -2.0` vs `short_z_entry = 0.3` → shorts fire far more often. Combined with simulated fees/slippage, this can dominate losses once real fees apply.

### C13 — Adaptive risk sizes up after winning streaks
`update_adaptive_risk` multiplies risk after high win-rate — classic path to ruin after a hot streak into a regime change.

### C14 (NEW) — Busy-polling main loop wastes CPU and adds latency jitter
The live main loop calls `try_recv()` in a tight 5ms sleep loop (`sleep_milliseconds(5)`). This is ~200 polls/sec of CPU waste and adds up to 5ms of candle-close latency jitter. Should use `tokio::select!` on the channels + interval.

### C15 (NEW) — CSV audit log errors are silently swallowed
`let _ = write_to_csv_file(...).await;` in the log task discards all write errors. A full disk or permission change will silently lose the entire trade audit trail without any alarm.

### C16 (NEW) — `SymbolFilters` are exchange-wide constants, not per-symbol
`SymbolFilters::for_exchange()` returns one hardcoded set for all symbols on an exchange (e.g., `tick_size: 0.01`, `step_size: 0.00001`). This is catastrophic for low-priced tokens (PEPE, SHIB): the code already has a workaround comment for `effective_tick`, but `step_size` is still hardcoded. Correctly, these must be loaded from `GET /exchangeInfo` per symbol.

### C17 (NEW) — WS reconnect has no heartbeat / sequence validation
`run_binance_websocket_client` reconnects with a fixed 5s delay on any disconnect. There is no ping/pong heartbeat (`ping` frame every 20s required by Binance), no sequence-number check, and no staleness detection. Silent stale feeds are more dangerous than a disconnected one.

### C18 (NEW) — `short_z_entry = 0.3` is near-zero threshold
A z-score of 0.3 means "slightly above the mean" — this fires shorts constantly in any non-flat market, generating excessive churn, fees, and stop-outs. This single config value likely accounts for the majority of simulated losses.

### C19 (NEW) — Rate-limit (429) handling is absent
REST calls in `fetch_historical_candles`, the scanner, and warmup loops have zero backoff on 429 or 5xx responses. A burst of failures during startup (e.g. batch scan + warmup for 5 symbols) will silently skip symbols and continue with under-warmed strategies.

### C20 (NEW) — Phantom "use_testnet" flag has no runtime effect
`BotConfig::use_testnet` is parsed from config and stored, but **never read** in the trading or WebSocket path. The live loop unconditionally connects to `stream.binance.com` and `api.binance.com` (mainnet). This is a ticking time bomb when real API keys are added.

---

## 2. Missing Features (Production Baseline)

| Area | Missing |
|---|---|
| Execution | Signed REST/WS trading API, market/limit/IOC/FOK, cancel/replace, exchange-native SL/TP |
| Paper vs live gate | Explicit paper broker vs live broker; hard refuse live without testnet proof |
| State | Persist positions, orders, equity peak, cooldowns; crash recovery |
| Reconciliation | On boot: sync balances, open orders, positions vs local state |
| Resilience | Rate-limit (429) handling, exponential backoff + jitter, idempotent retries |
| Risk | Portfolio heat, correlation caps, daily/weekly loss halt that **closes** positions, remote kill switch |
| Alerts | Telegram/Discord/PagerDuty for fills, rejects, WS disconnect, drawdown halt |
| Testing | Strategy unit tests, integration tests against testnet, walk-forward / OOS harness |
| Data integrity | Exchange `exchangeInfo` filters (tick/step/minNotional), not hardcoded BTC-like filters |
| Observability | Structured logs (JSON), correlation IDs per order, alert on CSV write failure |
| Ops | Health endpoint, graceful shutdown (flatten or leave exchange stops), process supervisor |
| Security | Secrets via env/KMS, IP-bound API keys, withdraw disabled, metrics auth / localhost bind |

### Phantom features (look real, not wired)
- Fear & Greed Index *(struct field exists, `fear_greed_last_update == 0` always → passes through)*
- Spread gate *(`latest_spread_pct == 0.0` always live → passes through)*
- Time-of-day filter *(`preferred_hours_mask = 0xFFFFFFFF` → always passes)*
- Order splitting *(computes splits then still one sim fill)*
- BTC circuit breaker in live *(no BTC price feed in live loop)*
- `use_testnet` flag *(parsed but never used in network paths)*

---

## 3. Architecture Review

### Current coupling (problem)

```
main.rs
  ├─ WS ticks ──► strategy.on_tick / CandleBuilder
  ├─ strategy ──► Wallet (sim fills) ──► CSV log
  └─ Prometheus
```

Signal generation, sizing, "execution", accounting, and I/O live in one monolith (`SpotStrategy` ~2k lines). Network clients only fetch public market data.

### Target production architecture

```
MarketData ──► SignalEngine ──► RiskGate ──► ExecutionBroker ──► Exchange
     │              │               │              │
     └──────────────┴───────────────┴── StateStore / Reconciler
                                       │
                                  Alerts / Metrics
```

### Concrete structural failures

1. **No broker abstraction** — cannot paper-trade and live-trade behind one interface.
2. **Exchange polymorphism incomplete** — trait has price/history only; live hardcodes Binance.
3. **Candle clock is global + tick-gated** — candle closes only when a tick arrives after the timer; multi-symbol sync is wall-clock based, not exchange candle open time.
4. **Busy loop** (`try_recv` + 5ms sleep) — prefer `tokio::select!` on channels + interval.
5. **No backpressure / reconnect state machine** — WS reconnects with fixed 5s delay; no heartbeat, no sequence check on depth.
6. **Logging errors swallowed** — `let _ = write_to_csv_file(...)` can lose the audit trail silently.
7. **Capital model** — per-symbol wallets from a split of `margin`; no shared portfolio risk or rebalancing after PnL.
8. **(NEW) SpotStrategy is a 2049-line God Object** — signal computation, risk gating, position management, PnL accounting, logging, and metrics snapshot generation are all in one struct. It should be decomposed: `SignalEngine`, `RiskManager`, `PositionManager`, `Reporter`.
9. **(NEW) `StrategyConfig` duplication** — `StrategyConfig` and `StrategyFileConfig` are maintained in parallel with 30+ field copies. An `Option<T>` → `T` pattern with a derive macro or builder would halve this.

---

## 4. Prioritized Action Plan

### P0 — Do not lose money / do not crash (this week)

1. **Hard-gate live trading**
   Until a real broker exists, rename mode to `paper` and refuse `mode=futures` / any "live money" flag.

2. **Disable spot shorts in spot config**
   ```toml
   enable_short = false
   ```
   Keep shorts only behind futures + real margin API.

3. **Fix Futures stubs or remove from CLI**
   Replace `todo!()` with fail-at-startup, or hide `Mode::Futures`.

4. **Align timeframe**
   Backtest and live must use the same interval (either both 15m exchange klines, or both 1h). Prefer exchange kline WS (`@kline_15m`) over homemade tick aggregation.

5. **Fix batch history download**
   Paginate like `fetch_data.py` (startTime cursor, 1000/page) or always use the Python downloader.

6. **Bind metrics to `127.0.0.1`** and change Grafana password; expand `.gitignore` for `.env`, keys, `config.local.toml`.

7. **(NEW) Fix `use_testnet` to actually route to testnet** — `testnet.binance.vision` WS + REST when `use_testnet = true`.

8. **(NEW) Propagate CSV write errors** — log errors to stderr + Prometheus counter instead of `let _ = ...`.

9. **(NEW) Fix WS heartbeat** — send Binance-required ping frame every 20s; reconnect if pong not received within 5s.

### P1 — Execution & safety (before any testnet capital)

10. **Introduce `ExecutionBroker` trait**
    ```rust
    #[async_trait]
    pub trait ExecutionBroker: Send + Sync {
        async fn place_order(&self, req: OrderRequest) -> Result<OrderAck, ExecError>;
        async fn cancel(&self, id: ClientOrderId) -> Result<(), ExecError>;
        async fn get_balances(&self) -> Result<Balances, ExecError>;
        async fn get_open_orders(&self, symbol: &str) -> Result<Vec<Order>, ExecError>;
    }

    pub struct PaperBroker { /* current Wallet */ }
    pub struct BinanceSpotBroker { /* signed REST */ }
    ```

11. **Exchange-native protective orders**
    After entry: place STOP_LOSS_LIMIT / OCO. Software stops alone die with the process.

12. **Reconciliation on boot**
    ```
    load local state → fetch balances/orders →
    if mismatch: halt new entries, alert, optionally flatten
    ```

13. **Idempotent orders** — deterministic `newClientOrderId`, retry only on network ambiguity after `GET /order`.

14. **Rate-limit middleware** — respect `Retry-After` / Binance weights; exponential backoff with jitter on 429/5xx.

15. **Kill switch** — file/env/HTTP `POST /halt` that cancels opens, blocks entries, optionally market-closes.

16. **(NEW) Load `exchangeInfo` per symbol on boot** — replace `SymbolFilters::for_exchange()` with real tick/step/minNotional values from the API. Cache with TTL.

17. **(NEW) Replace busy-poll loop with `tokio::select!`** — eliminates CPU waste and removes latency jitter from candle-close events.

### P2 — Risk & realism

18. **Portfolio risk gate** — max concurrent correlated exposure (e.g. total alt beta to BTC); max portfolio heat = sum of risk_per_trade.
19. **Drawdown halt must flatten** (or at least cancel and reduce), not only block entries.
20. **Track daily PnL on shorts**; reset at exchange UTC day boundary consistently.
21. **Fail-closed filters** — OBI/spread: if feed stale > N seconds → no new entries.
22. **Load `exchangeInfo` filters** per symbol; delete hardcoded tick/step for all pairs.
23. **Cap adaptive risk ≤ 1.0** (de-risk after losses only; never size up automatically).
24. **Rebalance `short_z_entry`** to match long stringency (mirror: set to `+2.0`), or disable shorts until edge proven OOS.
25. **(NEW) Stale OBI invalidation** — track last OBI update timestamp; if > 30s stale → treat as missing (fail-closed).
26. **(NEW) Volume feed in live mode** — subscribe to `@aggTrade` or `@kline` stream to get real volume; currently live `Candle.volume = 0.0`.
27. **(NEW) Daily PnL must track short exits** — currently `daily_pnl` only updates on long trade exits (missing `short_unrealized_pnl_usdt` settlement).

### P3 — Observability & process

28. Alerts on: fill, reject, WS disconnect > 30s, reconciliation mismatch, drawdown halt.
29. Structured logging with `order_id`, `symbol`, `intent`, `fill_price`, `slippage_bps`.
30. Strategy unit tests for exit priority, panic stop, cooldown, short accounting; property tests for wallet invariants.
31. Walk-forward / purged CV; never tune on the same window you report.
32. Graceful shutdown: drain channels, persist state, leave exchange stops active.
33. **(NEW) Criterion benchmarks** — profile `on_candle_close` hot path; per `ideas.txt`, the main perf sink is likely `Vec<Trade>` / `VecDeque` allocation on each candle, not arithmetic.
34. **(NEW) Flamegraph integration** — add `cargo-flamegraph` target to Makefile so perf regressions are detectable.
35. **(NEW) Frontend dashboard** — real-time WebSocket-fed browser UI (per `ideas.txt`) showing live equity, open positions, signal state, and last 50 candles per symbol.

### Design pattern for the critical cutover

```rust
// RiskGate sits between signal and broker
async fn on_entry_signal(sig: Signal, risk: &RiskState, broker: &dyn ExecutionBroker) {
    if risk.halted || risk.portfolio_heat() > MAX_HEAT { return; }
    if !risk.filters_ok(&sig) { return; } // fail-closed

    let size = risk.size_position(&sig);
    let ack = broker.place_order(OrderRequest {
        client_id: ClientOrderId::new(&sig), // idempotent
        side: sig.side,
        qty: size,
        typ: OrderType::Market, // or limit+timeout
    }).await?;

    broker.place_protective_stops(ack.symbol, ack.avg_price, sig.stop, sig.tp).await?;
    risk.record_open(ack);
}
```

---

## 5. Pillar Scores

| Pillar | Status |
|---|---|
| Architecture & performance | Research monolith; WS used for prices only; no durable state |
| Risk & execution | Soft sim stops; no exchange stops; incomplete daily limits; no portfolio heat |
| Error handling & resilience | Fixed 5s WS retry; no 429 handling; no crash reconciliation |
| Security | No API secrets yet (good accident); metrics/Grafana exposed; weak ignore rules |
| Monitoring / testing / paper | CSV + Prometheus exist; paper≈live sim; almost no strategy tests; no alerts |

---

## 6. Quick-Win Fixes (< 1 day each, high safety impact)

| # | Fix | File | Impact |
|---|---|---|---|
| QW1 | Set `enable_short = false` in config | `config.toml` | Stops fake-short churn |
| QW2 | Replace `let _ = write_to_csv_file` | `main.rs` | No silent audit loss |
| QW3 | Bind metrics to `127.0.0.1` | `metrics/mod.rs` | Stops equity leakage |
| QW4 | Add `todo!()` guard at startup for Futures mode | `main.rs` | No mid-session abort |
| QW5 | Fix `short_z_entry` → `2.0` | `config.toml` | Eliminates short churn |
| QW6 | Add WS ping/pong heartbeat (20s) | `binance_websocket_client.rs` | Fixes stale feed risk |
| QW7 | Implement `use_testnet` routing | `binance_client.rs` + WS | Testnet flag works |
| QW8 | Add `.env` / `config.local.toml` to `.gitignore` | `.gitignore` | Prevents secret leak |

---

## Bottom Line

Treat current "live" as **paper trading only**. Highest ROI work is:

1. Explicit paper/live broker split
2. Timeframe/data integrity
3. Disable fake spot shorts / futures panics
4. Exchange-native stops + reconciliation + kill switch
5. Then testnet with tiny size

**Do not** deploy real capital until P0 and P1 are done.
