use crate::execution::{ExecutionBroker, state::StateManager, state::PositionState};
use crate::models::bot_config::BotConfig;
use std::sync::Arc;

pub async fn reconcile_startup(
    broker: &Arc<dyn ExecutionBroker>,
    state_manager: &StateManager,
    config: &BotConfig,
) -> Result<Option<PositionState>, String> {
    println!("[RECONCILIATION] Starting reconciliation for symbol {}", config.symbol);
    
    // 1. Fetch real balances
    let balances = broker.get_balances().await.map_err(|e| format!("Failed to get balances: {}", e))?;
    println!("[RECONCILIATION] Real Balances: USDT={:.2} free, {:.2} locked", balances.usdt_free, balances.usdt_locked);
    
    // 2. Fetch open orders
    let open_orders = broker.get_open_orders(&config.symbol).await.map_err(|e| format!("Failed to get open orders: {}", e))?;
    println!("[RECONCILIATION] Found {} open orders for {}", open_orders.len(), config.symbol);
    
    // 3. Check local state
    let local_state = state_manager.load_position(&config.symbol).map_err(|e| format!("SQLite error: {}", e))?;
    
    if let Some(ref state) = local_state {
        println!("[RECONCILIATION] SQLite says we have a position: holding={}, short={}, qty={}", 
                 state.is_holding, state.is_short, state.qty);
                 
        // Verify with exchange balances
        let base_asset = config.symbol.replace("USDT", "");
        let real_base_qty = balances.assets.get(&base_asset).copied().unwrap_or(0.0);
        
        if state.is_holding && real_base_qty < state.qty * 0.99 {
            println!("[RECONCILIATION] WARNING: SQLite expects {} {} but exchange has only {}. Fixing state.", 
                     state.qty, base_asset, real_base_qty);
                     
            if real_base_qty > 0.0 {
                // We have a partial position somehow
                let mut new_state = state.clone();
                new_state.qty = real_base_qty;
                state_manager.save_position(&new_state).map_err(|e| e.to_string())?;
                return Ok(Some(new_state));
            } else {
                // Position is gone
                state_manager.delete_position(&config.symbol).map_err(|e| e.to_string())?;
                return Ok(None);
            }
        }
    } else {
        println!("[RECONCILIATION] SQLite reports NO active position.");
    }
    
    Ok(local_state)
}
