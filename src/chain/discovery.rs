use ethers::prelude::*;
use std::sync::Arc;

use crate::broadcast::broadcast_cluster_snapshot;
use crate::state::{AppState, NodeState, NodeStatus};

use super::{ChainConfig, ChainNodeEvent};

pub async fn initial_chain_sync(state: &AppState, chain: &ChainConfig) -> Result<u64, String> {
    let current_block = chain.current_block().await?;

    let scan_from = chain.from_block;
    tracing::info!(
        "Chain sync: scanning blocks {scan_from}..{current_block} ({} blocks)",
        current_block.saturating_sub(scan_from)
    );

    let registered_addrs = chain
        .replay_to_current_set(scan_from, current_block)
        .await?;

    tracing::info!(
        "Chain sync: found {} registered addresses after replay",
        registered_addrs.len()
    );

    let mut count = 0;
    for addr in &registered_addrs {
        match chain.fetch_node_info(*addr).await {
            Ok(Some(info)) => {
                let mut node = NodeState::from_chain_info(&info);
                node.apply_geo(&state.geo);
                tracing::info!(
                    "Discovered node {} (port={}, lat={}, lon={})",
                    node.address,
                    node.admin_port,
                    node.latitude,
                    node.longitude
                );

                if let Err(e) = state.db.upsert_node(&node).await {
                    tracing::warn!("Failed to persist node {}: {e}", node.address);
                    continue;
                }
                state.nodes.write().insert(node.address.clone(), node);
                count += 1;
            }
            Ok(None) => {
                tracing::debug!("Address {addr:?} no longer registered, skipping");
            }
            Err(e) => {
                tracing::warn!("Failed to fetch info for {addr:?}: {e}");
            }
        }
    }

    if let Err(e) = state.db.set_last_chain_block(current_block).await {
        tracing::warn!("Failed to persist last_chain_block: {e}");
    }

    broadcast_cluster_snapshot(state);
    tracing::info!("Chain sync complete: {count} nodes discovered up to block {current_block}");

    Ok(current_block)
}

pub async fn chain_event_loop(state: AppState, chain: Arc<ChainConfig>, mut last_block: u64) {
    let mut interval = tokio::time::interval(chain.poll_interval);

    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => {
                tracing::info!("chain_event_loop: shutting down, persisting last_block={last_block}");
                if let Err(e) = state.db.set_last_chain_block(last_block).await {
                    tracing::error!("Failed to persist last_chain_block on shutdown: {e}");
                }
                return;
            }
            _ = interval.tick() => {}
        }

        let current_block = match chain.current_block().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!("Chain event loop: {e}");
                continue;
            }
        };

        if current_block <= last_block {
            continue;
        }

        let filter = Filter::new()
            .address(chain.registry_address)
            .from_block(last_block + 1)
            .to_block(current_block);

        match chain.provider.get_logs(&filter).await {
            Ok(logs) => {
                for log in logs {
                    if let Some(event) = chain.decode_log(&log) {
                        process_chain_event(&state, &chain, event).await;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to fetch logs {}..{current_block}: {e}",
                    last_block + 1
                );
                continue;
            }
        }

        last_block = current_block;

        if let Err(e) = state.db.set_last_chain_block(current_block).await {
            tracing::warn!("Failed to persist last_chain_block: {e}");
        }
    }
}

async fn process_chain_event(state: &AppState, chain: &ChainConfig, event: ChainNodeEvent) {
    match event {
        ChainNodeEvent::Registered { address } => {
            let addr_str = format!("{address:?}");
            tracing::info!("Chain: new registration for {addr_str}");

            match chain.fetch_node_info(address).await {
                Ok(Some(info)) => {
                    let mut node = NodeState::from_chain_info(&info);
                    node.apply_geo(&state.geo);

                    if let Err(e) = state.db.upsert_node(&node).await {
                        tracing::warn!("Failed to persist node {}: {e}", node.address);
                        return;
                    }
                    state.nodes.write().insert(node.address.clone(), node);
                    broadcast_cluster_snapshot(state);
                }
                Ok(None) => {
                    tracing::warn!("Chain: {addr_str} registered but contract says not registered");
                }
                Err(e) => {
                    tracing::warn!("Chain: failed to fetch info for {addr_str}: {e}");
                }
            }
        }
        ChainNodeEvent::Removed { address } => {
            let addr_str = format!("{address:?}");
            tracing::info!("Chain: node deregistered {addr_str}");

            let previous_status = {
                let nodes = state.nodes.read();
                nodes.get(&addr_str).map(|n| n.status.clone())
            };

            if let Some(node) = state.nodes.write().get_mut(&addr_str) {
                node.status = NodeStatus::Deregistered;
            }

            if let Err(e) = state.db.deregister_node(&addr_str).await {
                tracing::warn!("Failed to deregister node {addr_str} in DB: {e}");
                if let Some(prev) = previous_status {
                    if let Some(node) = state.nodes.write().get_mut(&addr_str) {
                        tracing::warn!("Rolling back in-memory status for {addr_str} to {prev}");
                        node.status = prev;
                    }
                }
                return;
            }

            state.nodes.write().remove(&addr_str);
            state.metrics.write().remove(&addr_str);
            broadcast_cluster_snapshot(state);
        }
        ChainNodeEvent::Unstaked { address, amount } => {
            let addr_str = format!("{address:?}");
            tracing::info!("Chain: node {addr_str} unstaked {amount}");
        }
    }
}
