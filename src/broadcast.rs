use serde_json::json;

use crate::node::metrics::StructuredMetrics;
use crate::state::{AppState, NodeState};

pub fn cluster_snapshot_json(nodes: &[NodeState]) -> String {
    json!({
        "type": "CLUSTER",
        "payload": {
            "node_count": nodes.len(),
            "nodes": nodes
        }
    })
    .to_string()
}

pub fn broadcast_cluster_snapshot(state: &AppState) {
    let nodes: Vec<NodeState> = state.nodes.read().values().cloned().collect();
    if let Err(e) = state.tx.send(cluster_snapshot_json(&nodes)) {
        tracing::debug!("No active WebSocket subscribers: {e}");
    }
}

pub fn broadcast_event(state: &AppState, event: &serde_json::Value) {
    if let Err(e) = state.tx.send(
        json!({
            "type": "EVENT",
            "payload": event
        })
        .to_string(),
    ) {
        tracing::debug!("No active WebSocket subscribers: {e}");
    }
}

pub fn broadcast_metrics(state: &AppState, node_address: &str, metrics: &StructuredMetrics) {
    if let Err(e) = state.tx.send(
        json!({
            "type": "METRICS",
            "node_address": node_address,
            "payload": metrics
        })
        .to_string(),
    ) {
        tracing::debug!("No active WebSocket subscribers: {e}");
    }
}
