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
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;

use crate::broadcast::cluster_snapshot_json;
use crate::node::offsets::network_totals;
use crate::state::{AppState, CachedSeed, NodeState, NodeStatus, SyncPhase};

/// A pinned seed snapshot is reused for this long unless membership changes.
const SEED_CACHE_TTL: Duration = Duration::from_secs(30);
/// When a rebuild fails, a cached snapshot this young is still served. Its block
/// stays recent enough for non-archive RPCs to verify against.
const SEED_STALE_MAX: Duration = Duration::from_secs(600);

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
    /// Frozen members stay in `nodes` because the registry's count and
    /// fingerprint include them, but their liveness is always `offline` so
    /// clients never route through them.
    frozen: bool,
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

/// 200 whenever Postgres answers, so a slow or retrying chain sync never fails
/// the platform healthcheck. Chain progress is reported in the body.
pub async fn handle_healthz(State(state): State<AppState>) -> impl IntoResponse {
    let chain = state.sync.read().clone();
    match state.db.ping().await {
        Ok(()) => (
            axum::http::StatusCode::OK,
            axum::Json(json!({ "status": "ok", "chain": chain })),
        )
            .into_response(),
        Err(e) => {
            // The driver error can name internal hosts; keep it in the logs only.
            tracing::warn!("Healthcheck database ping failed: {e}");
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(
                    json!({ "status": "degraded", "error": "database unavailable", "chain": chain }),
                ),
            )
                .into_response()
        }
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
        if let Ok(db_nodes) = state
            .db
            .load_registry_nodes(&state.chain.registry_hex)
            .await
        {
            if !db_nodes.is_empty() {
                let mut mem = state.nodes.write();
                for (addr, ns) in &db_nodes {
                    mem.entry(addr.clone()).or_insert_with(|| ns.clone());
                }
                nodes = mem.values().cloned().collect();
            }
        }
    }
    nodes.sort_by(|left, right| left.address.cmp(&right.address));

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

    // Registered nodes only: deregistered rows would drag the average down forever.
    let reputation = state.db.get_reputation_ranking().await.unwrap_or_default();
    let reputation_avg = (!reputation.is_empty())
        .then(|| reputation.iter().map(|r| r.score).sum::<f64>() / reputation.len() as f64);

    let (totals, totals_nodes) = {
        let offsets = state.metric_offsets.read();
        (network_totals(offsets.values()), offsets.len())
    };
    let mut network_totals_json = totals.to_camel_case_json();
    // Nodes whose counters feed the totals (every node ever scraped). This is
    // not the registered node count; that is `nodes.length`.
    network_totals_json.insert("scrapedNodeCount".to_string(), json!(totals_nodes));

    let indexer = state.sync.read().clone();
    let genesis = *state.network_genesis_ms.read();

    axum::Json(json!({
        "nodes": nodes,
        "metrics": metrics,
        "recent_events": events,
        "reputation": reputation,
        "network_totals": network_totals_json,
        "network_genesis_ms": genesis,
        "network_reputation_avg": reputation_avg,
        "indexer": indexer,
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

/// Chain-pinned members at the last processed block, cached per topology version.
async fn pinned_seed_members(state: &AppState) -> Result<CachedSeed, String> {
    let block_number = {
        let sync = state.sync.read();
        match (sync.phase, sync.last_block) {
            (SyncPhase::Starting, _) | (_, None) | (_, Some(0)) => {
                return Err("chain sync has not completed yet".to_string())
            }
            (_, Some(block)) => block,
        }
    };
    let version = state.topology_version();

    let mut cache = state.seed_cache.lock().await;
    if let Some(cached) = cache.as_ref() {
        if cached.topology_version == version && cached.built_at.elapsed() < SEED_CACHE_TTL {
            return Ok(cached.clone());
        }
    }

    let mut member_addresses: Vec<String> = state
        .nodes
        .read()
        .values()
        .filter(|node| node.status != NodeStatus::Deregistered)
        .map(|node| node.address.to_lowercase())
        .collect();
    member_addresses.sort();
    if member_addresses.is_empty() {
        return Err("no replayed registered members are available".to_string());
    }

    match state
        .chain
        .pinned_topology_members(&member_addresses, block_number)
        .await
    {
        Ok((members, fingerprint)) => {
            let fresh = CachedSeed {
                block: block_number,
                topology_version: version,
                built_at: Instant::now(),
                members,
                fingerprint,
            };
            *cache = Some(fresh.clone());
            Ok(fresh)
        }
        Err(error) => match cache.as_ref() {
            Some(cached) if cached.built_at.elapsed() < SEED_STALE_MAX => {
                tracing::warn!(
                    "Seed rebuild at block {block_number} failed, serving block {}: {error}",
                    cached.block
                );
                Ok(cached.clone())
            }
            _ => Err(error),
        },
    }
}

async fn build_seed_topology(state: &AppState) -> Result<SeedTopologySnapshot, String> {
    let pinned = pinned_seed_members(state).await?;
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

    let now_unix = u64::try_from(chrono::Utc::now().timestamp())
        .map_err(|_| "system time is before Unix epoch".to_string())?;
    assemble_seed_topology(
        pinned.members,
        pinned.fingerprint,
        &statuses,
        &observed_at,
        pinned.block,
        now_unix,
    )
}

/// Build the wire snapshot. Membership comes only from the chain-pinned set;
/// indexer state contributes liveness, and a member it has no status for (or
/// one that is frozen) is reported offline.
fn assemble_seed_topology(
    members: Vec<crate::chain::PinnedTopologyNode>,
    fingerprint: String,
    statuses: &std::collections::BTreeMap<String, NodeStatus>,
    observed_at: &std::collections::HashMap<String, u64>,
    block_number: u64,
    now_unix: u64,
) -> Result<SeedTopologySnapshot, String> {
    if members.is_empty() {
        return Err("pinned topology has no members".to_string());
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
        let status = match statuses.get(&member.address) {
            Some(NodeStatus::Online) if !member.frozen => NodeStatus::Online,
            _ => NodeStatus::Offline,
        };
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
            frozen: member.frozen,
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
            frozen: false,
        }
    }

    const ONLINE: &str = "0x1111111111111111111111111111111111111111";
    const OFFLINE: &str = "0x2222222222222222222222222222222222222222";
    const FROZEN: &str = "0x3333333333333333333333333333333333333333";

    #[test]
    fn seed_snapshot_retains_offline_registered_members_as_liveness_only() {
        let statuses = std::collections::BTreeMap::from([
            (ONLINE.to_string(), NodeStatus::Online),
            (OFFLINE.to_string(), NodeStatus::Offline),
        ]);
        let observed = std::collections::HashMap::from([
            (ONLINE.to_string(), 100_u64),
            (OFFLINE.to_string(), 90_u64),
        ]);

        let snapshot = assemble_seed_topology(
            vec![member(ONLINE), member(OFFLINE)],
            "00".repeat(32),
            &statuses,
            &observed,
            7,
            101,
        )
        .expect("complete replay state must produce a seed snapshot");

        assert_eq!(snapshot.nodes.len(), 2);
        assert_eq!(snapshot.liveness[1].status, NodeStatus::Offline);
    }

    #[test]
    fn frozen_members_stay_in_the_fingerprinted_set_but_are_never_live() {
        let statuses = std::collections::BTreeMap::from([
            (ONLINE.to_string(), NodeStatus::Online),
            (FROZEN.to_string(), NodeStatus::Online),
        ]);
        let mut frozen = member(FROZEN);
        frozen.frozen = true;

        let snapshot = assemble_seed_topology(
            vec![member(ONLINE), frozen],
            "00".repeat(32),
            &statuses,
            &std::collections::HashMap::new(),
            7,
            101,
        )
        .unwrap();

        assert_eq!(
            snapshot.nodes.len(),
            2,
            "count/fingerprint verification needs every member"
        );
        assert!(snapshot.nodes[1].frozen);
        assert_eq!(snapshot.liveness[0].status, NodeStatus::Online);
        assert_eq!(snapshot.liveness[1].status, NodeStatus::Offline);
    }

    #[test]
    fn members_unknown_to_the_indexer_are_reported_offline() {
        let snapshot = assemble_seed_topology(
            vec![member(ONLINE)],
            "00".repeat(32),
            &std::collections::BTreeMap::new(),
            &std::collections::HashMap::new(),
            7,
            101,
        )
        .unwrap();
        assert_eq!(snapshot.liveness[0].status, NodeStatus::Offline);
    }

    #[test]
    fn unordered_members_are_rejected() {
        assert!(assemble_seed_topology(
            vec![member(OFFLINE), member(ONLINE)],
            "00".repeat(32),
            &std::collections::BTreeMap::new(),
            &std::collections::HashMap::new(),
            7,
            101,
        )
        .is_err());
    }
}
