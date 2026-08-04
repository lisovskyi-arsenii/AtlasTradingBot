use prometheus::{
    Encoder, GaugeVec, IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

/// All Prometheus metrics for the trading bot.
///
/// Clone is cheap (all fields are `Arc`-backed internally by the prometheus crate).
#[derive(Clone)]
pub struct BotMetrics {
    pub registry: Registry,

    // ── Per-symbol gauges ────────────────────────────────────────────────────
    /// Current equity (USDT) including unrealised PnL.
    pub equity_usd: GaugeVec,
    /// Total realised PnL (USDT) since start.
    pub realized_pnl_usd: GaugeVec,
    /// Total PnL as a percentage of initial capital.
    pub pnl_pct: GaugeVec,
    /// Current drawdown from peak equity (%).
    pub drawdown_pct: GaugeVec,
    /// Latest RSI value.
    pub rsi: GaugeVec,
    /// Latest Z-Score value.
    pub z_score: GaugeVec,
    /// Latest ATR value (absolute).
    pub atr: GaugeVec,
    /// ATR as a percentage of price.
    pub atr_pct: GaugeVec,
    /// Number of completed trades.
    pub trade_count: IntGaugeVec,
    /// Unrealised PnL (USDT) of the open position.
    pub unrealized_pnl_usd: GaugeVec,
    /// Current position: 0 = FLAT, 1 = LONG, -1 = SHORT.
    pub position_side: IntGaugeVec,
    /// Wallet USDT balance (free cash).
    pub wallet_usdt: GaugeVec,
    /// Last candle close price.
    pub last_price: GaugeVec,
    /// Peak equity reached (high-water mark).
    pub peak_equity: GaugeVec,
    /// Number of candles processed per symbol.
    pub candle_count: IntGaugeVec,
    /// Whether the drawdown stop is currently active: 0 = no, 1 = yes.
    pub drawdown_stop_active: IntGaugeVec,

    // ── Portfolio-level gauges ───────────────────────────────────────────────
    /// Sum of equity across all symbols.
    pub portfolio_equity_usd: prometheus::Gauge,
    /// Portfolio PnL as a percentage of total initial capital.
    pub portfolio_pnl_pct: prometheus::Gauge,
    /// Portfolio total PnL in USDT.
    pub portfolio_pnl_usd: prometheus::Gauge,
    /// Count of symbols currently being traded.
    pub active_symbols: prometheus::IntGauge,
}

impl BotMetrics {
    /// Build and register all metrics. Panics on duplicate registration.
    pub fn new() -> Self {
        let registry = Registry::new();

        let sym_labels = &["symbol"];

        let equity_usd = GaugeVec::new(
            Opts::new("bot_equity_usd", "Current equity in USDT (mark-to-market)"),
            sym_labels,
        ).unwrap();

        let realized_pnl_usd = GaugeVec::new(
            Opts::new("bot_realized_pnl_usd", "Cumulative realised PnL in USDT"),
            sym_labels,
        ).unwrap();

        let pnl_pct = GaugeVec::new(
            Opts::new("bot_pnl_pct", "Total PnL as % of initial capital"),
            sym_labels,
        ).unwrap();

        let drawdown_pct = GaugeVec::new(
            Opts::new("bot_drawdown_pct", "Current drawdown from peak equity (%)"),
            sym_labels,
        ).unwrap();

        let rsi = GaugeVec::new(
            Opts::new("bot_rsi", "Latest RSI indicator value"),
            sym_labels,
        ).unwrap();

        let z_score = GaugeVec::new(
            Opts::new("bot_z_score", "Latest Z-Score value"),
            sym_labels,
        ).unwrap();

        let atr = GaugeVec::new(
            Opts::new("bot_atr", "Latest ATR value (absolute)"),
            sym_labels,
        ).unwrap();

        let atr_pct = GaugeVec::new(
            Opts::new("bot_atr_pct", "ATR as % of current price"),
            sym_labels,
        ).unwrap();

        let trade_count = IntGaugeVec::new(
            Opts::new("bot_trade_count", "Number of completed trades"),
            sym_labels,
        ).unwrap();

        let unrealized_pnl_usd = GaugeVec::new(
            Opts::new("bot_unrealized_pnl_usd", "Unrealised PnL of open position (USDT)"),
            sym_labels,
        ).unwrap();

        let position_side = IntGaugeVec::new(
            Opts::new("bot_position_side", "Position: 0=FLAT, 1=LONG, -1=SHORT"),
            sym_labels,
        ).unwrap();

        let wallet_usdt = GaugeVec::new(
            Opts::new("bot_wallet_usdt", "Free USDT balance in wallet"),
            sym_labels,
        ).unwrap();

        let last_price = GaugeVec::new(
            Opts::new("bot_last_price", "Last candle close price"),
            sym_labels,
        ).unwrap();

        let peak_equity = GaugeVec::new(
            Opts::new("bot_peak_equity_usd", "Peak equity high-water mark (USDT)"),
            sym_labels,
        ).unwrap();

        let candle_count = IntGaugeVec::new(
            Opts::new("bot_candle_count", "Candles processed since start"),
            sym_labels,
        ).unwrap();

        let drawdown_stop_active = IntGaugeVec::new(
            Opts::new("bot_drawdown_stop_active", "1 if max-drawdown stop is active"),
            sym_labels,
        ).unwrap();

        let portfolio_equity_usd = prometheus::Gauge::new(
            "bot_portfolio_equity_usd",
            "Total portfolio equity across all symbols (USDT)",
        ).unwrap();

        let portfolio_pnl_pct = prometheus::Gauge::new(
            "bot_portfolio_pnl_pct",
            "Portfolio PnL as % of total initial capital",
        ).unwrap();

        let portfolio_pnl_usd = prometheus::Gauge::new(
            "bot_portfolio_pnl_usd",
            "Portfolio total PnL in USDT",
        ).unwrap();

        let active_symbols = prometheus::IntGauge::new(
            "bot_active_symbols",
            "Number of symbols currently being traded",
        ).unwrap();

        // Register all metrics
        registry.register(Box::new(equity_usd.clone())).unwrap();
        registry.register(Box::new(realized_pnl_usd.clone())).unwrap();
        registry.register(Box::new(pnl_pct.clone())).unwrap();
        registry.register(Box::new(drawdown_pct.clone())).unwrap();
        registry.register(Box::new(rsi.clone())).unwrap();
        registry.register(Box::new(z_score.clone())).unwrap();
        registry.register(Box::new(atr.clone())).unwrap();
        registry.register(Box::new(atr_pct.clone())).unwrap();
        registry.register(Box::new(trade_count.clone())).unwrap();
        registry.register(Box::new(unrealized_pnl_usd.clone())).unwrap();
        registry.register(Box::new(position_side.clone())).unwrap();
        registry.register(Box::new(wallet_usdt.clone())).unwrap();
        registry.register(Box::new(last_price.clone())).unwrap();
        registry.register(Box::new(peak_equity.clone())).unwrap();
        registry.register(Box::new(candle_count.clone())).unwrap();
        registry.register(Box::new(drawdown_stop_active.clone())).unwrap();
        registry.register(Box::new(portfolio_equity_usd.clone())).unwrap();
        registry.register(Box::new(portfolio_pnl_pct.clone())).unwrap();
        registry.register(Box::new(portfolio_pnl_usd.clone())).unwrap();
        registry.register(Box::new(active_symbols.clone())).unwrap();

        Self {
            registry,
            equity_usd,
            realized_pnl_usd,
            pnl_pct,
            drawdown_pct,
            rsi,
            z_score,
            atr,
            atr_pct,
            trade_count,
            unrealized_pnl_usd,
            position_side,
            wallet_usdt,
            last_price,
            peak_equity,
            candle_count,
            drawdown_stop_active,
            portfolio_equity_usd,
            portfolio_pnl_pct,
            portfolio_pnl_usd,
            active_symbols,
        }
    }

    /// Encode all metrics as Prometheus text format.
    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&metric_families, &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }
}

/// Snapshot of one symbol's state used to update Prometheus gauges.
pub struct SymbolSnapshot {
    pub symbol: String,
    pub equity: f64,
    pub realized_pnl_usdt: f64,
    pub pnl_pct: f64,
    pub drawdown_pct: f64,
    pub rsi: f64,
    pub z_score: f64,
    pub atr: f64,
    pub atr_pct: f64,
    pub trade_count: i64,
    pub unrealized_pnl_usdt: f64,
    /// 0 = flat, 1 = long, -1 = short
    pub position_side: i64,
    pub wallet_usdt: f64,
    pub last_price: f64,
    pub peak_equity: f64,
    pub candle_count: i64,
    pub drawdown_stop_active: bool,
}

/// Push a snapshot of all symbols into the Prometheus gauges.
pub fn update_metrics(metrics: &BotMetrics, snapshots: &[SymbolSnapshot], initial_capital: f64) {
    let mut total_equity = 0.0;

    for s in snapshots {
        let labels = &[s.symbol.as_str()];
        metrics.equity_usd.with_label_values(labels).set(s.equity);
        metrics.realized_pnl_usd.with_label_values(labels).set(s.realized_pnl_usdt);
        metrics.pnl_pct.with_label_values(labels).set(s.pnl_pct);
        metrics.drawdown_pct.with_label_values(labels).set(s.drawdown_pct);
        metrics.rsi.with_label_values(labels).set(s.rsi);
        metrics.z_score.with_label_values(labels).set(s.z_score);
        metrics.atr.with_label_values(labels).set(s.atr);
        metrics.atr_pct.with_label_values(labels).set(s.atr_pct);
        metrics.trade_count.with_label_values(labels).set(s.trade_count);
        metrics.unrealized_pnl_usd.with_label_values(labels).set(s.unrealized_pnl_usdt);
        metrics.position_side.with_label_values(labels).set(s.position_side);
        metrics.wallet_usdt.with_label_values(labels).set(s.wallet_usdt);
        metrics.last_price.with_label_values(labels).set(s.last_price);
        metrics.peak_equity.with_label_values(labels).set(s.peak_equity);
        metrics.candle_count.with_label_values(labels).set(s.candle_count);
        metrics.drawdown_stop_active.with_label_values(labels).set(if s.drawdown_stop_active { 1 } else { 0 });

        total_equity += s.equity;
    }

    metrics.portfolio_equity_usd.set(total_equity);
    metrics.active_symbols.set(snapshots.len() as i64);

    let total_pnl_usd = total_equity - initial_capital;
    let total_pnl_pct = if initial_capital > 0.0 {
        (total_pnl_usd / initial_capital) * 100.0
    } else {
        0.0
    };
    metrics.portfolio_pnl_usd.set(total_pnl_usd);
    metrics.portfolio_pnl_pct.set(total_pnl_pct);
}

/// Start a lightweight HTTP server that serves `/metrics` in Prometheus format.
///
/// Binds to `127.0.0.1` only — never exposed to the network.
/// Runs forever; should be spawned as a `tokio::spawn` task.
pub async fn run_metrics_server(metrics: Arc<BotMetrics>, port: u16) {
    // Bind to loopback only: position/equity data must not be world-readable.
    let addr = format!("127.0.0.1:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[METRICS] Failed to bind {}: {}. Metrics disabled.", addr, e);
            return;
        }
    };
    println!("[METRICS] Prometheus endpoint live at http://127.0.0.1:{}/metrics (loopback only)", port);

    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let metrics = Arc::clone(&metrics);

        tokio::spawn(async move {
            // Read the HTTP request (we don't care about the content, just
            // need to drain it so the response is sent cleanly).
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;

            let body = metrics.encode();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );

            use tokio::io::AsyncWriteExt;
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.flush().await;
        });
    }
}
