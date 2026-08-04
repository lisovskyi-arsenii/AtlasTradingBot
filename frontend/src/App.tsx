import { useEffect, useState } from 'react';
import { Activity, TrendingUp, LineChart, Target, Zap, List, AlertTriangle } from 'lucide-react';
import './index.css';

interface PositionInfo {
  symbol: string;
  qty: number;
  entry_price: number;
  current_price: number;
  unrealized_pnl: number;
  unrealized_pnl_pct: number;
  side: string;
}

interface DashboardState {
  total_equity: number;
  initial_margin: number;
  pnl_pct: number;
  pnl_usdt: number;
  daily_pnl_usdt: number;
  drawdown_pct: number;
  total_trades: number;
  win_rate: number;
  fear_greed_index: number;
  is_kill_switch_active: boolean;
  open_positions: PositionInfo[];
}

function App() {
  const [state, setState] = useState<DashboardState | null>(null);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    // Connect to Axum WebSocket
    const ws = new WebSocket('ws://localhost:8080/ws');

    ws.onopen = () => {
      console.log('Connected to WebSocket');
      setConnected(true);
    };

    ws.onmessage = (event) => {
      try {
        const data: DashboardState = JSON.parse(event.data);
        setState(data);
      } catch (err) {
        console.error('Failed to parse WS message:', err);
      }
    };

    ws.onclose = () => {
      console.log('Disconnected from WebSocket');
      setConnected(false);
    };

    return () => {
      ws.close();
    };
  }, []);

  const formatCurrency = (val: number) => 
    new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(val);

  const formatPct = (val: number) => 
    new Intl.NumberFormat('en-US', { style: 'percent', minimumFractionDigits: 2 }).format(val / 100);

  const getColorClass = (val: number) => {
    if (val > 0) return 'text-green';
    if (val < 0) return 'text-red';
    return '';
  };

  return (
    <div className="app-container">
      <header className="header">
        <h1>Atlas Terminal</h1>
        <div className="status-badge">
          <div className={`status-indicator ${connected ? 'connected' : 'disconnected'}`} />
          <span className="text-muted">{connected ? 'Live' : 'Offline'}</span>
        </div>
      </header>

      {state?.is_kill_switch_active && (
        <div className="kill-switch-alert" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '0.5rem' }}>
          <AlertTriangle size={24} />
          SYSTEM HALTED: Kill switch engaged
        </div>
      )}

      <main>
        <section className="metrics-grid">
          <div className="metric-card">
            <span className="metric-label" style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
              <Activity size={14} /> Total Equity
            </span>
            <span className="metric-value mono">
              {state ? formatCurrency(state.total_equity) : '---'}
            </span>
            <span className="metric-sub text-muted mono">
              Margin: {state ? formatCurrency(state.initial_margin) : '---'}
            </span>
          </div>

          <div className="metric-card">
            <span className="metric-label" style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
              <TrendingUp size={14} /> Total PnL
            </span>
            <span className={`metric-value mono ${state ? getColorClass(state.pnl_usdt) : ''}`}>
              {state ? (state.pnl_usdt > 0 ? '+' : '') + formatCurrency(state.pnl_usdt) : '---'}
            </span>
            <span className={`metric-sub mono ${state ? getColorClass(state.pnl_pct) : ''}`}>
              {state ? (state.pnl_pct > 0 ? '+' : '') + formatPct(state.pnl_pct) : '---'}
            </span>
          </div>

          <div className="metric-card">
            <span className="metric-label" style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
              <LineChart size={14} /> Daily PnL
            </span>
            <span className={`metric-value mono ${state ? getColorClass(state.daily_pnl_usdt) : ''}`}>
              {state ? (state.daily_pnl_usdt > 0 ? '+' : '') + formatCurrency(state.daily_pnl_usdt) : '---'}
            </span>
          </div>

          <div className="metric-card">
            <span className="metric-label" style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
              <Activity size={14} /> Drawdown
            </span>
            <span className={`metric-value mono ${state && state.drawdown_pct > 5 ? 'text-red' : ''}`}>
              {state ? formatPct(state.drawdown_pct) : '---'}
            </span>
          </div>

          <div className="metric-card">
            <span className="metric-label" style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
              <Target size={14} /> Win Rate (Trades)
            </span>
            <span className="metric-value mono">
              {state ? formatPct(state.win_rate) : '---'}
            </span>
            <span className="metric-sub text-muted mono">
              {state ? state.total_trades : 0} trades closed
            </span>
          </div>

          <div className="metric-card">
            <span className="metric-label" style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
              <Zap size={14} /> Fear & Greed Index
            </span>
            <span className="metric-value mono">
              {state ? state.fear_greed_index.toFixed(0) : '---'}
            </span>
            <span className="metric-sub text-muted">
              {state ? (state.fear_greed_index < 30 ? 'Fear' : state.fear_greed_index > 70 ? 'Greed' : 'Neutral') : '---'}
            </span>
          </div>
        </section>

        <section>
          <div className="header" style={{ marginTop: '2rem', marginBottom: '1rem', borderBottom: 'none' }}>
            <h2 style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '1rem', textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-secondary)' }}>
              <List size={18} /> Open Positions
            </h2>
          </div>
          
          <div className="table-container">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Side</th>
                  <th>Size</th>
                  <th>Entry Price</th>
                  <th>Current Price</th>
                  <th>Unrealized PnL</th>
                </tr>
              </thead>
              <tbody>
                {!state || state.open_positions.length === 0 ? (
                  <tr>
                    <td colSpan={6} style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-secondary)' }}>
                      No active positions
                    </td>
                  </tr>
                ) : (
                  state.open_positions.map((pos, idx) => (
                    <tr key={idx}>
                      <td className="mono" style={{ fontWeight: 500 }}>{pos.symbol}</td>
                      <td className={`mono ${pos.side === 'Long' ? 'text-green' : 'text-red'}`}>
                        {pos.side}
                      </td>
                      <td className="mono">{pos.qty}</td>
                      <td className="mono">{formatCurrency(pos.entry_price)}</td>
                      <td className="mono">{formatCurrency(pos.current_price)}</td>
                      <td className={`mono ${getColorClass(pos.unrealized_pnl)}`}>
                        {pos.unrealized_pnl > 0 ? '+' : ''}{formatCurrency(pos.unrealized_pnl)} ({pos.unrealized_pnl > 0 ? '+' : ''}{formatPct(pos.unrealized_pnl_pct)})
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  );
}

export default App;
