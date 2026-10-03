use axum::http::StatusCode;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
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
    /// What the node reports about itself in `/metrics/json`. Omitted when the
    /// node has no report, and for every node while any online exit lacks one
    /// (see `assemble_seed_topology`).
    #[serde(skip_serializing_if = "Option::is_none")]
    capabilities: Option<Vec<String>>,
    /// Informational; omitted when the node has not reported a version.
    #[serde(skip_serializing_if = "Option::is_none")]
    build_version: Option<String>,
}

/// What the indexer last read from one node's own endpoints.
#[derive(Default, Clone, Debug)]
struct NodeReport {
    capabilities: Option<Vec<String>>,
    build_version: Option<String>,
    pow_difficulty: Option<u32>,
}

/// The `role` values that can be chosen as exits (2 = exit, 3 = full).
fn is_exit_role(role: u8) -> bool {
    matches!(role, 2 | 3)
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
    let sync_age_secs = chain.sync_age_secs(chrono::Utc::now().timestamp_millis());
    match state.db.ping().await {
        Ok(()) => (
            axum::http::StatusCode::OK,
            axum::Json(json!({ "status": "ok", "sync_age_secs": sync_age_secs, "chain": chain })),
        )
            .into_response(),
        Err(e) => {
            // The driver error can name internal hosts; keep it in the logs only.
            tracing::warn!("Healthcheck database ping failed: {e}");
            (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(json!({
                    "status": "degraded",
                    "error": "database unavailable",
                    "sync_age_secs": sync_age_secs,
                    "chain": chain,
                })),
            )
                .into_response()
        }
    }
}

/// For uptime monitors: 200 only when Postgres answers and the chain sync is
/// live, verified and caught up within `HEALTH_MAX_SYNC_AGE_SECS`. Not used as
/// the platform healthcheck, because a long replay after a deploy would fail it.
pub async fn handle_healthz_sync(State(state): State<AppState>) -> impl IntoResponse {
    let chain = state.sync.read().clone();
    let now_ms = chrono::Utc::now().timestamp_millis();
    let sync_age_secs = chain.sync_age_secs(now_ms);
    let max_sync_age_secs = state.max_sync_age_secs;

    let problem = match state.db.ping().await {
        Err(e) => {
            tracing::warn!("Sync healthcheck database ping failed: {e}");
            Some("database unavailable")
        }
        Ok(()) => chain
            .check_health(now_ms, max_sync_age_secs)
            .err()
            .map(|reason| reason.as_str()),
    };

    let (code, status) = match problem {
        None => (StatusCode::OK, "ok"),
        Some(_) => (StatusCode::SERVICE_UNAVAILABLE, "unhealthy"),
    };
    (
        code,
        axum::Json(json!({
            "status": status,
            "error": problem,
            "sync_age_secs": sync_age_secs,
            "max_sync_age_secs": max_sync_age_secs,
            "chain": chain,
        })),
    )
        .into_response()
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
    // Node versions before cumulativeMaximumCostUsd existed banked 0 for it, so
    // the total is only a lower bound and can fall below cumulativeCostUsd.
    network_totals_json.insert(
        "cumulativeMaximumCostUsdIsLowerBound".to_string(),
        json!(true),
    );

    let indexer = state.sync.read().clone();
    let genesis = *state.network_genesis_ms.read();
    let settlements = state.settlements.read().clone();

    axum::Json(json!({
        "nodes": nodes,
        "metrics": metrics,
        "recent_events": events,
        "reputation": reputation,
        "network_totals": network_totals_json,
        "network_genesis_ms": genesis,
        "network_reputation_avg": reputation_avg,
        "indexer": indexer,
        "settlements": settlements,
    }))
}

#[derive(Deserialize)]
pub struct SettlementQuery {
    limit: Option<u32>,
}

const SETTLEMENT_LIMIT_DEFAULT: u32 = 50;
const SETTLEMENT_LIMIT_MAX: u32 = 500;

/// Settlement totals plus the most recent settlements and exit credit claims.
pub async fn handle_get_settlements(
    State(state): State<AppState>,
    Query(query): Query<SettlementQuery>,
) -> impl IntoResponse {
    let status = state.settlements.read().clone();
    let chain_id = state.sync.read().chain_id;
    let limit = i64::from(
        query
            .limit
            .unwrap_or(SETTLEMENT_LIMIT_DEFAULT)
            .clamp(1, SETTLEMENT_LIMIT_MAX),
    );

    let mut recent = Vec::new();
    let mut recent_claims = Vec::new();
    if let (Some(chain_id), Some(entry_point)) = (chain_id, status.entry_point.as_deref()) {
        let settlements = state
            .db
            .recent_paid_settlements(chain_id, entry_point, limit)
            .await;
        let claims = match status.reward_pool.as_deref() {
            Some(pool) => {
                state
                    .db
                    .recent_exit_credit_claims(chain_id, pool, limit)
                    .await
            }
            None => Ok(Vec::new()),
        };
        match (settlements, claims) {
            (Ok(settlements), Ok(claims)) => {
                recent = settlements;
                recent_claims = claims;
            }
            (Err(e), _) | (_, Err(e)) => {
                tracing::error!("Failed to query settlements: {e}");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({ "error": "Failed to query settlements" })),
                )
                    .into_response();
            }
        }
    }

    let mut body = serde_json::to_value(&status).unwrap_or_else(|_| json!({}));
    if let Some(object) = body.as_object_mut() {
        object.insert("recent".to_string(), json!(recent));
        object.insert("recent_claims".to_string(), json!(recent_claims));
    }
    axum::Json(body).into_response()
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

    let mut reports: std::collections::HashMap<String, NodeReport> = state
        .metrics
        .read()
        .iter()
        .map(|(address, metrics)| {
            (
                address.to_lowercase(),
                NodeReport {
                    capabilities: metrics.capabilities.clone(),
                    build_version: Some(metrics.build_version.clone())
                        .filter(|version| !version.is_empty()),
                    pow_difficulty: None,
                },
            )
        })
        .collect();
    for (address, difficulty) in state.pow_difficulties.read().iter() {
        reports
            .entry(address.to_lowercase())
            .or_default()
            .pow_difficulty = Some(*difficulty);
    }

    let now_unix = u64::try_from(chrono::Utc::now().timestamp())
        .map_err(|_| "system time is before Unix epoch".to_string())?;
    assemble_seed_topology(
        pinned.members,
        pinned.fingerprint,
        &statuses,
        &observed_at,
        &reports,
        state.seed_max_pow_difficulty,
        pinned.block,
        now_unix,
    )
}

/// Build the wire snapshot. Membership comes only from the chain-pinned set;
/// indexer state contributes liveness, and a member it has no status for (or
/// one that is frozen) is reported offline.
///
/// Capabilities are all or nothing. SDK 0.3.0 and later switch to capability
/// mode as soon as any node carries a `capabilities` list, and then pick only
/// exits that list `paid_v2` for paid execution. So capabilities are published
/// only while every online exit has its own well-formed report; otherwise they
/// are omitted for every node and clients keep their earlier behaviour. Each
/// list is the node's own report, never inferred from role or version.
///
/// `pow_difficulty` is the highest difficulty an online member reports, capped
/// at `max_pow_difficulty`: a packet that meets it is accepted by every entry.
#[allow(clippy::too_many_arguments)]
fn assemble_seed_topology(
    members: Vec<crate::chain::PinnedTopologyNode>,
    fingerprint: String,
    statuses: &std::collections::BTreeMap<String, NodeStatus>,
    observed_at: &std::collections::HashMap<String, u64>,
    reports: &std::collections::HashMap<String, NodeReport>,
    max_pow_difficulty: u32,
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
    let is_online = |member: &crate::chain::PinnedTopologyNode| {
        !member.frozen && matches!(statuses.get(&member.address), Some(NodeStatus::Online))
    };
    let report_of = |address: &str| reports.get(address);
    let publish_capabilities = members
        .iter()
        .filter(|member| is_online(member) && is_exit_role(member.role))
        .all(|member| report_of(&member.address).is_some_and(|r| r.capabilities.is_some()));
    if !publish_capabilities {
        tracing::debug!("Seed capabilities withheld: an online exit has no capability report");
    }
    let pow_difficulty = members
        .iter()
        .filter(|member| is_online(member))
        .filter_map(|member| report_of(&member.address).and_then(|r| r.pow_difficulty))
        .max()
        .unwrap_or(0)
        .min(max_pow_difficulty);

    let mut liveness = Vec::with_capacity(members.len());
    let mut nodes = Vec::with_capacity(members.len());
    for member in members {
        let status = if is_online(&member) {
            NodeStatus::Online
        } else {
            NodeStatus::Offline
        };
        let observation = observed_at.get(&member.address).copied().unwrap_or(0);
        let report = report_of(&member.address);
        liveness.push(SeedLiveness {
            address: member.address.clone(),
            status,
            observed_at_unix: observation,
            capabilities: report
                .and_then(|r| r.capabilities.clone())
                .filter(|_| publish_capabilities),
            build_version: report.and_then(|r| r.build_version.clone()),
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
        pow_difficulty,
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

    fn no_reports() -> std::collections::HashMap<String, NodeReport> {
        std::collections::HashMap::new()
    }

    fn exit(address: &str) -> PinnedTopologyNode {
        PinnedTopologyNode {
            role: 2,
            layer: 2,
            ..member(address)
        }
    }

    fn report(capabilities: Option<&[&str]>, pow: Option<u32>) -> NodeReport {
        NodeReport {
            capabilities: capabilities
                .map(|names| names.iter().map(|name| name.to_string()).collect()),
            build_version: Some("0.4.0-rc.3".to_string()),
            pow_difficulty: pow,
        }
    }

    fn online(addresses: &[&str]) -> std::collections::BTreeMap<String, NodeStatus> {
        addresses
            .iter()
            .map(|address| (address.to_string(), NodeStatus::Online))
            .collect()
    }

    fn seed_json(
        members: Vec<PinnedTopologyNode>,
        statuses: &std::collections::BTreeMap<String, NodeStatus>,
        reports: &std::collections::HashMap<String, NodeReport>,
    ) -> serde_json::Value {
        let snapshot = assemble_seed_topology(
            members,
            "00".repeat(32),
            statuses,
            &std::collections::HashMap::new(),
            reports,
            16,
            7,
            101,
        )
        .expect("ordered members must produce a seed snapshot");
        serde_json::to_value(snapshot).expect("seed snapshot serializes")
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
            &no_reports(),
            16,
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
            &no_reports(),
            16,
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
            &no_reports(),
            16,
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
            &no_reports(),
            16,
            7,
            101,
        )
        .is_err());
    }

    const RELAY: &str = "0x4444444444444444444444444444444444444444";
    const EXIT_A: &str = "0x5555555555555555555555555555555555555555";
    const EXIT_B: &str = "0x6666666666666666666666666666666666666666";

    #[test]
    fn capabilities_and_versions_are_published_when_every_online_exit_reports() {
        let reports = std::collections::HashMap::from([
            (RELAY.to_string(), report(Some(&["surb_v2"]), Some(1))),
            (
                EXIT_A.to_string(),
                report(Some(&["paid_v2", "surb_v2"]), Some(1)),
            ),
            (EXIT_B.to_string(), report(Some(&["surb_v2"]), Some(1))),
        ]);
        let json = seed_json(
            vec![member(RELAY), exit(EXIT_A), exit(EXIT_B)],
            &online(&[RELAY, EXIT_A, EXIT_B]),
            &reports,
        );

        assert_eq!(
            json["liveness"][0],
            serde_json::json!({
                "address": RELAY,
                "status": "online",
                "observed_at_unix": 0,
                "capabilities": ["surb_v2"],
                "build_version": "0.4.0-rc.3",
            })
        );
        assert_eq!(
            json["liveness"][1]["capabilities"],
            serde_json::json!(["paid_v2", "surb_v2"])
        );
        // An exit without a chain executor reports no paid_v2, and none is added.
        assert_eq!(
            json["liveness"][2]["capabilities"],
            serde_json::json!(["surb_v2"])
        );
        assert_eq!(json["pow_difficulty"], 1);
    }

    #[test]
    fn capabilities_are_withheld_for_all_nodes_while_an_online_exit_lacks_a_report() {
        for missing in [Some(report(None, Some(1))), None] {
            let mut reports = std::collections::HashMap::from([
                (RELAY.to_string(), report(Some(&["surb_v2"]), Some(1))),
                (
                    EXIT_A.to_string(),
                    report(Some(&["paid_v2", "surb_v2"]), Some(1)),
                ),
            ]);
            if let Some(legacy) = missing.clone() {
                reports.insert(EXIT_B.to_string(), legacy);
            }
            let json = seed_json(
                vec![member(RELAY), exit(EXIT_A), exit(EXIT_B)],
                &online(&[RELAY, EXIT_A, EXIT_B]),
                &reports,
            );
            for entry in json["liveness"].as_array().unwrap() {
                assert!(entry.get("capabilities").is_none(), "{entry}");
            }
            assert_eq!(json["liveness"][0]["build_version"], "0.4.0-rc.3");
            assert_eq!(json["liveness"][1]["build_version"], "0.4.0-rc.3");
        }
    }

    #[test]
    fn offline_or_frozen_exits_without_reports_do_not_block_capabilities() {
        let reports = std::collections::HashMap::from([(
            EXIT_A.to_string(),
            report(Some(&["paid_v2", "surb_v2"]), Some(1)),
        )]);
        let mut frozen = exit(EXIT_B);
        frozen.frozen = true;
        let json = seed_json(
            vec![member(RELAY), exit(EXIT_A), frozen],
            &online(&[EXIT_A, EXIT_B]),
            &reports,
        );
        assert_eq!(
            json["liveness"][1]["capabilities"],
            serde_json::json!(["paid_v2", "surb_v2"])
        );
        // A relay without a report is simply not v2-capable; it does not block.
        assert!(json["liveness"][0].get("capabilities").is_none());
        assert!(json["liveness"][0].get("build_version").is_none());
        assert!(json["liveness"][2].get("capabilities").is_none());
    }

    #[test]
    fn pow_difficulty_is_the_highest_online_report_capped() {
        let reports = std::collections::HashMap::from([
            (RELAY.to_string(), report(Some(&[]), Some(2))),
            (EXIT_A.to_string(), report(Some(&["paid_v2"]), Some(5))),
            (EXIT_B.to_string(), report(Some(&["paid_v2"]), Some(9))),
        ]);
        let json = seed_json(
            vec![member(RELAY), exit(EXIT_A), exit(EXIT_B)],
            &online(&[RELAY, EXIT_A]),
            &reports,
        );
        assert_eq!(json["pow_difficulty"], 5, "offline nodes do not count");

        let capped = assemble_seed_topology(
            vec![member(RELAY), exit(EXIT_A)],
            "00".repeat(32),
            &online(&[RELAY, EXIT_A]),
            &std::collections::HashMap::new(),
            &std::collections::HashMap::from([(EXIT_A.to_string(), report(None, Some(40)))]),
            16,
            7,
            101,
        )
        .unwrap();
        assert_eq!(capped.pow_difficulty, 16);

        let json = seed_json(vec![member(RELAY)], &online(&[RELAY]), &no_reports());
        assert_eq!(
            json["pow_difficulty"], 0,
            "no reports keeps the SDK default"
        );
    }
}
