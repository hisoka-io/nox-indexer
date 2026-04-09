use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use ethers::utils::{hex, keccak256};
use serde_json::json;
use tokio::sync::broadcast::error::RecvError;

use crate::broadcast::cluster_snapshot_json;
use crate::state::{AppState, NodeState, NodeStatus};

pub async fn handle_healthz(State(state): State<AppState>) -> impl IntoResponse {
    match state.db.ping().await {
        Ok(()) => (
            axum::http::StatusCode::OK,
            axum::Json(json!({ "status": "ok" })),
        )
            .into_response(),
        Err(e) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({ "status": "degraded", "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn handle_get_reputation(State(state): State<AppState>) -> impl IntoResponse {
    match state.db.get_reputation_ranking().await {
        Ok(ranking) => axum::Json(json!({ "ranking": ranking })).into_response(),
        Err(e) => {
            tracing::error!("Failed to query reputation: {e}");
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({ "error": "Failed to query reputation" })),
            )
                .into_response()
        }
    }
}

pub async fn handle_get_state(State(state): State<AppState>) -> impl IntoResponse {
    let mut nodes: Vec<NodeState> = state.nodes.read().values().cloned().collect();

    if nodes.is_empty() {
        if let Ok(db_nodes) = state.db.load_all_nodes().await {
            if !db_nodes.is_empty() {
                let mut mem = state.nodes.write();
                for (addr, ns) in &db_nodes {
                    mem.entry(addr.clone()).or_insert_with(|| ns.clone());
                }
                nodes = mem.values().cloned().collect();
            }
        }
    }

    let node_addrs: std::collections::HashSet<&str> =
        nodes.iter().map(|n| n.address.as_str()).collect();

    let metrics: std::collections::HashMap<String, _> = state
        .metrics
        .read()
        .iter()
        .filter(|(addr, _)| node_addrs.contains(addr.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let events: Vec<serde_json::Value> = state.recent_events.read().iter().cloned().collect();

    let reputation = state.db.get_reputation_ranking().await.unwrap_or_default();

    axum::Json(json!({
        "nodes": nodes,
        "metrics": metrics,
        "recent_events": events,
        "reputation": reputation
    }))
}

pub async fn handle_ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_ws_connection(socket, state))
}

/// Serve the network topology for SDK seed discovery.
///
/// Returns the same JSON format as a nox node's GET /topology endpoint,
/// filtered to only include online nodes with valid sphinx keys.
/// The fingerprint is XOR(keccak256(address)) matching NoxRegistry.sol.
pub async fn handle_seed_topology(State(state): State<AppState>) -> impl IntoResponse {
    let nodes = state.nodes.read();

    let online_nodes: Vec<serde_json::Value> = nodes
        .values()
        .filter(|n| n.status == NodeStatus::Online && !n.sphinx_key.is_empty())
        .map(|n| {
            let ingress_url = if n.ingress_url.is_empty() {
                derive_ingress_url(&n.p2p_addr, n.ingress_port)
            } else {
                n.ingress_url.clone()
            };
            json!({
                "address": n.address,
                "sphinx_key": n.sphinx_key,
                "url": n.p2p_addr,
                "stake": "0",
                "last_seen": 0,
                "is_privileged": true,
                "layer": n.layer,
                "role": n.role,
                "ingress_url": ingress_url,
                "metadata_url": n.metadata_url
            })
        })
        .collect();

    let mut fingerprint = [0u8; 32];
    for n in nodes.values() {
        if n.status == NodeStatus::Online && !n.sphinx_key.is_empty() {
            let addr_clean = n.address.strip_prefix("0x").unwrap_or(&n.address);
            if let Ok(addr_bytes) = hex::decode(addr_clean) {
                let hash = keccak256(&addr_bytes);
                for i in 0..32 {
                    fingerprint[i] ^= hash[i];
                }
            }
        }
    }

    let fp_hex = hex::encode(fingerprint); // lowercase hex, 64 chars

    axum::Json(json!({
        "nodes": online_nodes,
        "fingerprint": fp_hex
    }))
}

fn derive_ingress_url(p2p_addr: &str, ingress_port: u16) -> String {
    let ip = p2p_addr
        .split('/')
        .nth(2)
        .unwrap_or("0.0.0.0");
    format!("http://{}:{}", ip, ingress_port)
}

async fn handle_ws_connection(mut socket: WebSocket, state: AppState) {
    let mut rx = state.tx.subscribe();

    let nodes: Vec<NodeState> = state.nodes.read().values().cloned().collect();
    if socket.send(Message::Text(cluster_snapshot_json(&nodes))).await.is_err() {
        return;
    }

    loop {
        match rx.recv().await {
            Ok(msg) => {
                if socket.send(Message::Text(msg)).await.is_err() {
                    break; // client disconnected
                }
            }
            Err(RecvError::Lagged(n)) => {
                tracing::warn!("WebSocket client lagged by {n} messages, re-sending snapshot");
                let nodes: Vec<NodeState> = state.nodes.read().values().cloned().collect();
                if socket.send(Message::Text(cluster_snapshot_json(&nodes))).await.is_err() {
                    break;
                }
            }
            Err(RecvError::Closed) => {
                break; // sender dropped, server is shutting down
            }
        }
    }
}
