use crate::execution::{ExecutionBroker, state::StateManager, state::PositionState};
use crate::models::bot_config::BotConfig;
use std::sync::Arc;

pub async fn reconcile_startup(
    symbol: &str,
    broker: &Arc<dyn ExecutionBroker>,
    state_manager: &StateManager,
    _config: &BotConfig,
) -> Result<Option<PositionState>, String> {
    println!("[RECONCILIATION] Starting reconciliation for symbol {}", symbol);

    // 1. Fetch real balances
    let balances = broker
        .get_balances()
        .await
        .map_err(|e| format!("Failed to get balances: {}", e))?;
    println!(
        "[RECONCILIATION] Real Balances: USDT={:.2} free, {:.2} locked",
        balances.usdt_free, balances.usdt_locked
    );

    // 2. Fetch open orders
    let open_orders = broker
        .get_open_orders(symbol)
        .await
        .map_err(|e| format!("Failed to get open orders: {}", e))?;
    println!(
        "[RECONCILIATION] Found {} open orders for {}",
        open_orders.len(),
        symbol
    );
    let has_resting_stop_on_exchange = open_orders.iter().any(|o| {
        matches!(o.order_type, crate::execution::OrderType::StopLossLimit)
    });

    // 3. Check local state
    let local_state = state_manager
        .load_position(symbol)
        .await
        .map_err(|e| format!("SQLite error: {}", e))?;

    if let Some(ref state) = local_state {
        println!(
            "[RECONCILIATION] SQLite says we have a position: holding={}, short={}, qty={}",
            state.is_holding, state.is_short, state.qty
        );

        // Verify with exchange balances
        let base_asset = symbol.replace("USDT", "");
        let real_base_qty = balances.assets.get(&base_asset).copied().unwrap_or(0.0);

        if state.is_holding && real_base_qty < state.qty * 0.99 {
            println!("[RECONCILIATION] WARNING: SQLite expects {} {} but exchange has only {}. Fixing state.",
                     state.qty, base_asset, real_base_qty);

            return if real_base_qty > 0.0 {
                // We have a partial position somehow
                let mut new_state = state.clone();
                new_state.qty = real_base_qty;
                state_manager
                    .save_position(&new_state)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(Some(new_state))
            } else {
                // Position is gone. If a protective stop was resting on the
                // exchange, it's what most likely closed the position while
                // we were offline — but cancel it anyway (best-effort) in
                // case it's still open for some other reason (e.g. manual
                // close on the exchange), so we don't leave an orphaned
                // order behind.
                if let Some(ref stop_id) = state.stop_order_id {
                    let id = crate::execution::ClientOrderId(stop_id.clone());
                    if let Err(e) = broker.cancel(symbol, &id).await {
                        println!("[RECONCILIATION] Orphaned stop {} cancel failed (likely already gone): {}", stop_id, e);
                    }
                }
                state_manager
                    .delete_position(symbol)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(None)
            }
        }

        // Balances confirm the position is real. If it has no resting
        // protective stop — either it never got one (pre-Phase-0 state row,
        // or the process died between market entry and placing the stop),
        // or `stop_order_id` is stale and the order is actually gone from
        // the exchange (`has_resting_stop_on_exchange` catches that case
        // too) — a confirmed position must never sit unprotected. The
        // caller decides how to place it since that needs strategy-specific
        // sizing/price logic; signal it by clearing `stop_order_id` here so
        // downstream code can tell "confirmed position, needs a stop" apart
        // from "confirmed position, already protected".
        if state.is_holding && state.stop_order_id.is_some() && !has_resting_stop_on_exchange {
            println!(
                "[RECONCILIATION] WARNING: recorded stop {} for {} not found among open orders — treating as unprotected.",
                state.stop_order_id.as_deref().unwrap_or(""), symbol
            );
            let mut fixed = state.clone();
            fixed.stop_order_id = None;
            state_manager.save_position(&fixed).await.map_err(|e| e.to_string())?;
            return Ok(Some(fixed));
        }
    } else {
        println!("[RECONCILIATION] SQLite reports NO active position.");
    }

    Ok(local_state)
}
