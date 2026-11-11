use serde_json::json;

use crate::node::metrics::StructuredMetrics;
use crate::state::{public_nodes, AppState, NodeState};

/// The CLUSTER message: every known node in its public projection.
pub fn cluster_snapshot_json(state: &AppState) -> String {
    let nodes: Vec<NodeState> = state.nodes.read().values().cloned().collect();
    let metrics = state.metrics.read();
    json!({
        "type": "CLUSTER",
        "payload": {
            "node_count": nodes.len(),
            "nodes": public_nodes(&nodes, &metrics)
        }
    })
    .to_string()
}

pub fn broadcast_cluster_snapshot(state: &AppState) {
    send(state, cluster_snapshot_json(state));
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
