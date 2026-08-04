use axum::{
    routing::get,
    Router,
    extract::ws::{WebSocketUpgrade, WebSocket, Message},
    response::IntoResponse,
};
use tokio::sync::broadcast;
use std::net::SocketAddr;
use axum::extract::State;
use serde::Serialize;

/// 1. Структура для даних, які ми хочемо відправляти на фронтенд.
/// Вам потрібно буде описати тут поля: поточний баланс, відкриті позиції, PnL тощо.
#[derive(Debug, Clone, Serialize)]
pub struct PositionInfo {
    pub symbol: String,
    pub qty: f64,
    pub entry_price: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
    pub unrealized_pnl_pct: f64,
    pub side: String, // "Long" або "Short"
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardState {
    // ── Базові показники портфеля ──
    pub total_equity: f64,
    pub initial_margin: f64,
    pub pnl_pct: f64,
    pub pnl_usdt: f64,
    pub daily_pnl_usdt: f64,
    pub drawdown_pct: f64,

    // ── Показники стратегії та ризику ──
    pub total_trades: u32,
    pub win_rate: f64,
    pub fear_greed_index: f64,
    pub is_kill_switch_active: bool,
    
    // ── Відкриті позиції ──
    pub open_positions: Vec<PositionInfo>,
}

/// 2. Функція для запуску HTTP/WebSocket сервера.
/// `state_rx` - це канал, через який сервер буде отримувати нові дані від головного циклу бота (з main.rs).
pub async fn run_dashboard_server(
    port: u16, 
    state_rx: broadcast::Sender<DashboardState>
) {
    // 3. Тут вам потрібно створити Axum Router.
    // Він повинен мати щонайменше один маршрут (наприклад, `/ws`), який оброблятиме WebSocket з'єднання.
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state_rx);
        // Якщо фронтенд зібраний у статику (dist/), ви можете також додати сюди ServeDir, 
        // щоб бекенд сам роздавав файли фронтенду, але під час розробки простіше використовувати Vite dev server (порт 5173).

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("[DASHBOARD] Запуск сервера на http://{}", addr);

    // 4. Запуск сервера. Використайте `axum::Server` або `tokio::net::TcpListener` (залежно від версії axum).
    // ... напишіть код запуску тут ...

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// 5. Обробник для маршруту `/ws`.
/// Викликається кожного разу, коли клієнт (браузер) підключається по WebSocket.
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(tx): State<broadcast::Sender<DashboardState>>,
) -> impl IntoResponse {
    let rx = tx.subscribe();

    ws.on_upgrade(|socket| handle_socket(socket, rx))
}

/// 6. Основна логіка WebSocket з'єднання.
/// Саме тут ви будете читати дані з вашого каналу `state_rx` (який треба сюди передати або клонувати) 
/// і відправляти їх клієнту через `socket.send(...)`.
async fn handle_socket(mut socket: WebSocket, mut rx: broadcast::Receiver<DashboardState>) {
    // Нескінченний цикл, у якому:
    // 1. Чекаємо нових повідомлень з `state_rx`.
    // 2. Перетворюємо структуру DashboardState у JSON (за допомогою serde_json::to_string).
    // 3. Відправляємо JSON клієнту: socket.send(Message::Text(json)).await;
    // 4. Обробляємо помилки відключення клієнта.
    
    // ... напишіть логіку тут ...
    loop {
        let state = match rx.recv().await {
            Ok(state) => state,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        };

        let json = match serde_json::to_string(&state) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("Помилка серіалізації: {}", e);
                continue;
            }
        };

        if socket.send(Message::Text(json.into())).await.is_err() {
            break;
        }
    }
}
