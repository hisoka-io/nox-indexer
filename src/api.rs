use axum::http::StatusCode;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use serde::Serialize;
use serde_json::json;
use tokio::sync::broadcast::error::RecvError;

use crate::broadcast::cluster_snapshot_json;
use crate::state::{AppState, NodeState, NodeStatus};

#[derive(Serialize)]
struct SeedTopologyNode {
    address: String,
    sphinx_key: String,
    url: String,
    stake: String,
    last_seen: u64,
    is_privileged: bool,
    layer: u8,
    role: u8,
    ingress_url: String,
    metadata_url: String,
}

#[derive(Serialize)]
struct SeedLiveness {
    address: String,
    status: NodeStatus,
    observed_at_unix: u64,
}

#[derive(Serialize)]
struct SeedTopologySnapshot {
    schema_version: u8,
    nodes: Vec<SeedTopologyNode>,
    fingerprint: String,
    timestamp: u64,
    block_number: u64,
    pow_difficulty: u32,
    liveness: Vec<SeedLiveness>,
}

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

pub async fn handle_seed_topology(State(state): State<AppState>) -> impl IntoResponse {
    match build_seed_topology(&state).await {
        Ok(snapshot) => (StatusCode::OK, axum::Json(json!(snapshot))).into_response(),
        Err(error) => {
            tracing::error!("Seed topology unavailable: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(json!({ "error": "chain-verified topology unavailable" })),
            )
                .into_response()
        }
    }
}

async fn build_seed_topology(state: &AppState) -> Result<SeedTopologySnapshot, String> {
    let block_number = state
        .db
        .get_last_chain_block()
        .await
        .map_err(|error| format!("load processed chain block: {error}"))?
        .filter(|block| *block > 0)
        .ok_or_else(|| "no processed chain block".to_string())?;
    let observed_at: std::collections::HashMap<String, u64> = state
        .db
        .load_liveness_observed_at()
        .await
        .map_err(|error| format!("load liveness observations: {error}"))?
        .into_iter()
        .map(|(address, observed)| (address.to_lowercase(), observed))
        .collect();
    let statuses: std::collections::BTreeMap<String, NodeStatus> = state
        .nodes
        .read()
        .values()
        .filter(|node| node.status != NodeStatus::Deregistered)
        .map(|node| (node.address.to_lowercase(), node.status.clone()))
        .collect();
    if statuses.is_empty() {
        return Err("no replayed registered members are available".to_string());
    }
    let member_addresses: Vec<String> = statuses.keys().cloned().collect();
    let (members, fingerprint) = state
        .chain
        .pinned_topology_members(&member_addresses, block_number)
        .await?;
    if members.len() != statuses.len() {
        return Err("pinned member count differs from replayed member set".to_string());
    }

    let now_unix = u64::try_from(chrono::Utc::now().timestamp())
        .map_err(|_| "system time is before Unix epoch".to_string())?;
    assemble_seed_topology(
        members,
        fingerprint,
        statuses,
        observed_at,
        block_number,
        now_unix,
    )
}

fn assemble_seed_topology(
    members: Vec<crate::chain::PinnedTopologyNode>,
    fingerprint: String,
    statuses: std::collections::BTreeMap<String, NodeStatus>,
    observed_at: std::collections::HashMap<String, u64>,
    block_number: u64,
    now_unix: u64,
) -> Result<SeedTopologySnapshot, String> {
    if members.len() != statuses.len() {
        return Err("pinned member count differs from replayed member set".to_string());
    }
    if members
        .windows(2)
        .any(|pair| pair[0].address >= pair[1].address)
    {
        return Err("pinned members are not in canonical address order".to_string());
    }
    let mut liveness = Vec::with_capacity(members.len());
    let mut nodes = Vec::with_capacity(members.len());
    for member in members {
        let status = statuses.get(&member.address).cloned().ok_or_else(|| {
            format!(
                "pinned member {} is absent from replay state",
                member.address
            )
        })?;
        let observation = observed_at.get(&member.address).copied().unwrap_or(0);
        liveness.push(SeedLiveness {
            address: member.address.clone(),
            status,
            observed_at_unix: observation,
        });
        nodes.push(SeedTopologyNode {
            address: member.address,
            sphinx_key: member.sphinx_key,
            url: member.url,
            stake: member.stake,
            last_seen: observation,
            is_privileged: member.is_privileged,
            layer: member.layer,
            role: member.role,
            ingress_url: member.ingress_url,
            metadata_url: member.metadata_url,
        });
    }
    Ok(SeedTopologySnapshot {
        schema_version: 2,
        nodes,
        fingerprint,
        timestamp: now_unix,
        block_number,
        pow_difficulty: 0,
        liveness,
    })
}

async fn handle_ws_connection(mut socket: WebSocket, state: AppState) {
    let mut rx = state.tx.subscribe();

    let nodes: Vec<NodeState> = state.nodes.read().values().cloned().collect();
    if socket
        .send(Message::Text(cluster_snapshot_json(&nodes)))
        .await
        .is_err()
    {
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
                if socket
                    .send(Message::Text(cluster_snapshot_json(&nodes)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Err(RecvError::Closed) => {
                break; // sender dropped, server is shutting down
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::PinnedTopologyNode;

    fn member(address: &str) -> PinnedTopologyNode {
        PinnedTopologyNode {
            address: address.to_string(),
            sphinx_key: "11".repeat(32),
            url: "/ip4/127.0.0.1/tcp/9000".to_string(),
            stake: "0".to_string(),
            is_privileged: true,
            layer: 0,
            role: 1,
            ingress_url: "http://127.0.0.1:9002".to_string(),
            metadata_url: String::new(),
        }
    }

    #[test]
    fn seed_snapshot_retains_offline_registered_members_as_liveness_only() {
        let online = "0x1111111111111111111111111111111111111111";
        let offline = "0x2222222222222222222222222222222222222222";
        let statuses = std::collections::BTreeMap::from([
            (online.to_string(), NodeStatus::Online),
            (offline.to_string(), NodeStatus::Offline),
        ]);
        let observed = std::collections::HashMap::from([
            (online.to_string(), 100_u64),
            (offline.to_string(), 90_u64),
        ]);

        let snapshot = assemble_seed_topology(
            vec![member(online), member(offline)],
            "00".repeat(32),
            statuses,
            observed,
            7,
            101,
        )
        .expect("complete replay state must produce a seed snapshot");

        assert_eq!(snapshot.nodes.len(), 2);
        assert_eq!(snapshot.liveness[1].status, NodeStatus::Offline);
    }
}
