use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Assign initial layer based on role, matching NOX TopologyManager logic.
/// Role 1 (Relay): layer = SHA256(address)[0] % 2 (0 or 1)
/// Role 2 (Exit): layer = 2 (always)
/// Role 3 (Full): layer = SHA256(address)[0] % 3 (0, 1, or 2)
fn initial_layer_for_role(role: u8, address: &str) -> u8 {
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

use crate::chain::{self, OnChainNode};
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
        }
    }
}

impl NodeState {
    pub fn from_chain_info(info: &OnChainNode) -> Self {
        let admin_url = chain::derive_admin_url(&info.url);
        let parsed = chain::parse_multiaddr(&info.url);
        let admin_port = parsed.as_ref().map_or(0, |(_, p)| p + 1);
        let ingress_port = parsed.as_ref().map_or(0, |(_, p)| p + 2);

        let id = if info.address.len() >= 10 {
            format!("nox-{}", &info.address[2..10])
        } else {
            format!("nox-{}", &info.address)
        };

        Self {
            id,
            address: info.address.clone(),
            admin_port,
            ingress_port,
            p2p_addr: info.url.clone(),
            sphinx_key: info.sphinx_key.clone(),
            admin_url,
            ingress_url: info.ingress_url.clone(),
            metadata_url: info.metadata_url.clone(),
            status: NodeStatus::Online,
            role: info.role,
            layer: initial_layer_for_role(info.role, &info.address),
            latitude: 0.0,
            longitude: 0.0,
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

#[derive(Clone)]
pub struct AppState {
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
}
