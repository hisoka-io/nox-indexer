use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, Notify};
use tokio_util::sync::CancellationToken;

/// Assign initial layer based on role, matching NOX TopologyManager logic.
/// Role 1 (Relay): layer = SHA256(address)[0] % 2 (0 or 1)
/// Role 2 (Exit): layer = 2 (always)
/// Role 3 (Full): layer = SHA256(address)[0] % 3 (0, 1, or 2)
pub fn primary_layer_for_role(role: u8, address: &str) -> u8 {
    match role {
        2 => 2,
        _ => {
            let hash = Sha256::digest(address.to_lowercase().as_bytes());
            match role {
                1 => hash[0] % 2,
                _ => hash[0] % 3,
            }
        }
    }
}

use crate::chain::ChainConfig;
use crate::chain::{self, OnChainNode, PinnedTopologyNode};
use crate::db::{Db, NodeRow};
use crate::node::metrics::StructuredMetrics;

pub const MAX_RECENT_EVENTS: usize = 200;

pub struct TopoDedup {
    seen: HashSet<String>,
    last_clear: Instant,
}

impl TopoDedup {
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
            last_clear: Instant::now(),
        }
    }

    pub fn check(&mut self, key: &str) -> bool {
        if self.last_clear.elapsed().as_secs() > 30 {
            self.seen.clear();
            self.last_clear = Instant::now();
        }
        self.seen.insert(key.to_string())
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    Online,
    Offline,
    Deregistered,
}

impl NodeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeStatus::Online => "online",
            NodeStatus::Offline => "offline",
            NodeStatus::Deregistered => "deregistered",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "online" => NodeStatus::Online,
            "offline" => NodeStatus::Offline,
            "deregistered" => NodeStatus::Deregistered,
            other => {
                tracing::warn!("Unknown node status '{other}', defaulting to offline");
                NodeStatus::Offline
            }
        }
    }
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct NodeState {
    pub id: String,
    pub address: String,
    pub admin_port: u16,
    pub ingress_port: u16,
    pub p2p_addr: String,
    pub sphinx_key: String,
    pub admin_url: String,
    pub ingress_url: String,
    pub metadata_url: String,
    pub status: NodeStatus,
    pub role: u8,
    pub layer: u8,
    pub latitude: f64,
    pub longitude: f64,
    /// Frozen by the registry's slasher: still a member (counted in the
    /// fingerprint) but excluded from routing and seed liveness.
    pub frozen: bool,
    #[serde(skip)]
    pub registry_address: String,
    #[serde(skip)]
    pub chain_id: u64,
}

impl From<NodeRow> for NodeState {
    fn from(row: NodeRow) -> Self {
        let admin_url = if row.admin_url.is_empty() {
            chain::derive_admin_url(&row.p2p_addr)
        } else {
            row.admin_url
        };
        Self {
            address: row.address,
            id: row.id,
            admin_port: row.admin_port as u16,
            ingress_port: row.ingress_port as u16,
            p2p_addr: row.p2p_addr,
            sphinx_key: row.sphinx_key,
            admin_url,
            ingress_url: row.ingress_url,
            metadata_url: row.metadata_url,
            status: NodeStatus::from_str(&row.status),
            role: row.role as u8,
            layer: row.layer as u8,
            latitude: row.latitude,
            longitude: row.longitude,
            frozen: row.frozen,
            registry_address: row.registry_address,
            chain_id: u64::try_from(row.chain_id).unwrap_or_default(),
        }
    }
}

impl NodeState {
    pub fn from_chain_info(info: &OnChainNode, chain_id: u64, registry_address: &str) -> Self {
        let admin_url = chain::derive_admin_url(&info.url);
        let parsed = chain::parse_multiaddr(&info.url);
        let admin_port = parsed
            .as_ref()
            .and_then(|(_, p)| p.checked_add(1))
            .unwrap_or(0);
        let ingress_port = parsed
            .as_ref()
            .and_then(|(_, p)| p.checked_add(2))
            .unwrap_or(0);

        let id = if info.address.len() >= 10 {
            format!("nox-{}", &info.address[2..10])
        } else {
            format!("nox-{}", info.address)
        };

        Self {
            id,
            address: info.address.to_lowercase(),
            admin_port,
            ingress_port,
            p2p_addr: info.url.clone(),
            sphinx_key: info.sphinx_key.clone(),
            admin_url,
            ingress_url: info.ingress_url.clone(),
            metadata_url: info.metadata_url.clone(),
            status: NodeStatus::Offline,
            role: info.role,
            layer: primary_layer_for_role(info.role, &info.address),
            latitude: 0.0,
            longitude: 0.0,
            frozen: info.frozen,
            registry_address: registry_address.to_lowercase(),
            chain_id,
        }
    }

    pub fn apply_geo(&mut self, geo: &Option<crate::geo::GeoIp>) {
        if let Some(geo) = geo {
            if let Some((ip, _port)) = chain::parse_multiaddr(&self.p2p_addr) {
                if let Some((lat, lon)) = geo.lookup(&ip) {
                    self.latitude = lat;
                    self.longitude = lon;
                }
            }
        }
    }
}

#[derive(Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncPhase {
    /// Serving the database only; the first chain sync has not finished.
    #[default]
    Starting,
    /// Replaying or resuming registry events.
    Syncing,
    /// A sync attempt failed and is being retried with backoff.
    Retrying,
    /// Initial sync done; following new blocks.
    Live,
}

/// Chain discovery progress, exposed on `/healthz` and `/v1/state`.
#[derive(Serialize, Clone, Debug, Default)]
pub struct SyncStatus {
    pub phase: SyncPhase,
    pub chain_id: Option<u64>,
    pub registry_address: String,
    /// Last block whose registry events are fully applied (the checkpoint).
    pub last_block: Option<u64>,
    /// Replay progress while syncing.
    pub scan_block: Option<u64>,
    /// Whether the member set last matched the registry's count and fingerprint.
    pub verified: Option<bool>,
    pub attempts: u32,
    /// Full text for logs; serialized as a coarse category (see
    /// [`public_error_summary`]) because `/healthz` and `/v1/state` are public
    /// and provider errors can carry keyed hostnames or internal addresses.
    #[serde(serialize_with = "serialize_public_error")]
    pub last_error: Option<String>,
    /// When (Unix ms) every registry event up to the safe head was last
    /// applied. Advances on every live poll, so its age measures sync lag.
    pub last_synced_at_ms: Option<i64>,
    /// Safe head (latest block minus confirmations) seen by the last poll.
    pub head_block: Option<u64>,
}

/// Why the chain sync is not healthy, for monitors. Static text only: the
/// response is public.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncUnhealthy {
    NotLive,
    NeverSynced,
    Stale,
    Unverified,
}

impl SyncUnhealthy {
    pub fn as_str(self) -> &'static str {
        match self {
            SyncUnhealthy::NotLive => "chain sync is not live",
            SyncUnhealthy::NeverSynced => "chain sync has not completed",
            SyncUnhealthy::Stale => "chain sync is stale",
            SyncUnhealthy::Unverified => "membership disagrees with the registry",
        }
    }
}

impl SyncStatus {
    /// Seconds since every event up to the safe head was last applied.
    pub fn sync_age_secs(&self, now_ms: i64) -> Option<u64> {
        self.last_synced_at_ms
            .map(|at| u64::try_from(now_ms.saturating_sub(at)).unwrap_or(0) / 1_000)
    }

    /// Whether the indexer is following the chain closely enough to serve
    /// fresh seed snapshots.
    pub fn check_health(&self, now_ms: i64, max_age_secs: u64) -> Result<(), SyncUnhealthy> {
        if self.phase != SyncPhase::Live {
            return Err(SyncUnhealthy::NotLive);
        }
        let age = self
            .sync_age_secs(now_ms)
            .ok_or(SyncUnhealthy::NeverSynced)?;
        if age > max_age_secs {
            return Err(SyncUnhealthy::Stale);
        }
        if self.verified == Some(false) {
            return Err(SyncUnhealthy::Unverified);
        }
        Ok(())
    }
}

/// A public, secret-free description of a chain sync error. The full error is
/// always logged where it happens.
pub fn public_error_summary(error: &str) -> &'static str {
    use crate::chain::retry::{classify_rpc_error, RpcErrorKind};
    let lower = error.to_lowercase();
    if lower.contains("expected_chain_id") {
        return "rpc chain id differs from EXPECTED_CHAIN_ID";
    }
    if lower.contains("disagrees with registry") {
        return "replayed membership disagrees with the registry";
    }
    if [
        "load checkpoint",
        "load registry members",
        "load settlement",
        "persist ",
        "deregister stale",
    ]
    .iter()
    .any(|marker| lower.contains(marker))
    {
        return "database error";
    }
    if lower.contains("unknown layout") {
        return "registry layout not supported by this indexer version";
    }
    if lower.contains("shutting down") {
        return "shutting down";
    }
    match classify_rpc_error(error) {
        RpcErrorKind::RateLimited => "rpc rate limited",
        RpcErrorKind::RangeTooLarge { .. } => "rpc block range limit",
        RpcErrorKind::Transient => "rpc or sync error (see indexer logs)",
    }
}

pub(crate) fn serialize_public_error<S: serde::Serializer>(
    error: &Option<String>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match error {
        Some(error) => serializer.serialize_some(public_error_summary(error)),
        None => serializer.serialize_none(),
    }
}

/// Chain-pinned topology members, rebuilt lazily by `/seed/topology`.
#[derive(Clone)]
pub struct CachedSeed {
    pub block: u64,
    pub topology_version: u64,
    pub built_at: Instant,
    pub members: Vec<PinnedTopologyNode>,
    pub fingerprint: String,
}

#[derive(Clone)]
pub struct AppState {
    pub chain: Arc<ChainConfig>,
    pub nodes: Arc<RwLock<HashMap<String, NodeState>>>,
    pub metrics: Arc<RwLock<HashMap<String, StructuredMetrics>>>,
    pub recent_events: Arc<RwLock<VecDeque<Value>>>,
    pub topo_dedup: Arc<Mutex<TopoDedup>>,
    pub tx: broadcast::Sender<String>,
    pub db: Db,
    pub geo: Option<crate::geo::GeoIp>,
    pub shutdown: CancellationToken,
    /// Per-node banked lifetime totals, so container restarts do not reset the
    /// cumulative figures shown on the dashboard.
    pub metric_offsets: Arc<RwLock<HashMap<String, crate::node::offsets::NodeOffset>>>,
    pub sync: Arc<RwLock<SyncStatus>>,
    /// Bumped whenever registry membership or a profile changes, invalidating the seed cache.
    pub topology_version: Arc<AtomicU64>,
    pub seed_cache: Arc<tokio::sync::Mutex<Option<CachedSeed>>>,
    /// Wakes the uptime loop so new members are probed without waiting a full interval.
    pub probe_now: Arc<Notify>,
    /// Earliest evidence of the network (first uptime check), in Unix ms.
    pub network_genesis_ms: Arc<RwLock<Option<i64>>>,
    /// Sync age above which `/healthz/sync` reports the indexer as stale.
    pub max_sync_age_secs: u64,
    /// Paid execution settlements and exit credit claims (off unless configured).
    pub settlements: Arc<RwLock<crate::chain::settlement::SettlementStatus>>,
}

impl AppState {
    pub fn bump_topology_version(&self) {
        self.topology_version.fetch_add(1, Ordering::SeqCst);
    }

    pub fn topology_version(&self) -> u64 {
        self.topology_version.load(Ordering::SeqCst)
    }

    pub fn mark_synced(&self, block: u64, verified: bool) {
        let mut sync = self.sync.write();
        sync.phase = SyncPhase::Live;
        sync.last_block = Some(block);
        sync.scan_block = Some(block);
        sync.verified = Some(verified);
        sync.attempts = 0;
        sync.last_error = None;
        sync.last_synced_at_ms = Some(chrono::Utc::now().timestamp_millis());
        sync.head_block = Some(block);
    }

    /// The live loop has applied every registry event up to the safe head.
    pub fn mark_caught_up(&self, last_block: u64, safe_head: u64) {
        let mut sync = self.sync.write();
        sync.last_block = Some(last_block);
        sync.scan_block = Some(last_block);
        sync.head_block = Some(safe_head);
        sync.last_synced_at_ms = Some(chrono::Utc::now().timestamp_millis());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_errors_are_published_as_categories_only() {
        let status = SyncStatus {
            last_error: Some(
                "all 2 RPC endpoint(s) failed for eth_getLogs: [https://arb-sepolia.g.alchemy.com: \
                 error sending request for url (http://postgres.railway.internal:5432)] \
                 [https://x.example: 429 Too Many Requests]"
                    .to_string(),
            ),
            ..SyncStatus::default()
        };
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["last_error"], "rpc rate limited");
        let text = json.to_string();
        assert!(
            !text.contains("alchemy") && !text.contains("railway.internal"),
            "{text}"
        );

        assert_eq!(
            public_error_summary("persist node 0xabc: error returned from database: ..."),
            "database error"
        );
        assert_eq!(
            public_error_summary("full replay disagrees with registry: count 12 != 13"),
            "replayed membership disagrees with the registry"
        );
        assert_eq!(
            public_error_summary("RPC reports chain id 1 but EXPECTED_CHAIN_ID is 421614"),
            "rpc chain id differs from EXPECTED_CHAIN_ID"
        );
        assert_eq!(
            public_error_summary(
                "relayer profile failed after 5 attempts: relayers(0xabc): relayers() returned \
                 an unknown layout (first string offset 0x140)"
            ),
            "registry layout not supported by this indexer version"
        );
        let none = serde_json::to_value(SyncStatus::default()).unwrap();
        assert!(none["last_error"].is_null());
    }

    fn live_status(synced_at_ms: i64) -> SyncStatus {
        SyncStatus {
            phase: SyncPhase::Live,
            last_block: Some(10),
            verified: Some(true),
            last_synced_at_ms: Some(synced_at_ms),
            ..SyncStatus::default()
        }
    }

    #[test]
    fn sync_health_tracks_the_age_of_the_last_caught_up_poll() {
        let now = 1_000_000_000;
        assert_eq!(live_status(now - 30_000).check_health(now, 300), Ok(()));
        assert_eq!(live_status(now - 30_000).sync_age_secs(now), Some(30));
        assert_eq!(
            live_status(now - 301_000).check_health(now, 300),
            Err(SyncUnhealthy::Stale)
        );
        let mut retrying = live_status(now);
        retrying.phase = SyncPhase::Retrying;
        assert_eq!(retrying.check_health(now, 300), Err(SyncUnhealthy::NotLive));
        let mut mismatched = live_status(now);
        mismatched.verified = Some(false);
        assert_eq!(
            mismatched.check_health(now, 300),
            Err(SyncUnhealthy::Unverified)
        );
        let mut fresh = live_status(now);
        fresh.last_synced_at_ms = None;
        assert_eq!(
            fresh.check_health(now, 300),
            Err(SyncUnhealthy::NeverSynced)
        );
        assert_eq!(
            SyncStatus::default().check_health(now, 300),
            Err(SyncUnhealthy::NotLive)
        );
    }

    #[test]
    fn chain_discovery_requires_a_liveness_probe_before_marking_a_member_online() {
        let node = NodeState::from_chain_info(
            &OnChainNode {
                address: "0x1111111111111111111111111111111111111111".to_string(),
                url: "/ip4/127.0.0.1/tcp/9000".to_string(),
                ingress_url: "http://127.0.0.1:9002".to_string(),
                metadata_url: String::new(),
                sphinx_key: "11".repeat(32),
                role: 1,
                frozen: false,
            },
            421_614,
            "0xABC",
        );

        assert_eq!(node.status, NodeStatus::Offline);
        assert_eq!(node.registry_address, "0xabc");
    }
}
