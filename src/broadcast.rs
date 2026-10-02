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
    send(state, cluster_snapshot_json(&nodes));
}

/// Fan a message out to WebSocket clients. A send error only means no client
/// is connected right now, which is the normal idle state, so it is not logged.
fn send(state: &AppState, message: String) {
    let _ = state.tx.send(message);
}

pub fn broadcast_event(state: &AppState, event: &serde_json::Value) {
    send(
        state,
        json!({
            "type": "EVENT",
            "payload": event
        })
        .to_string(),
    );
}

pub fn broadcast_metrics(state: &AppState, node_address: &str, metrics: &StructuredMetrics) {
    send(
        state,
        json!({
            "type": "METRICS",
            "node_address": node_address,
            "payload": metrics
        })
        .to_string(),
    );
}
