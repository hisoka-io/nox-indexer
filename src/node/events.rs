use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IngestEvent {
    NodeStarted {
        timestamp: u64,
        node_id: String,
    },
    PeerConnected {
        peer_id: String,
        node_id: String,
    },
    PeerDisconnected {
        peer_id: String,
        node_id: String,
    },
    TopologyAdd {
        address: String,
        role: u8,
        stake: String,
        node_id: String,
    },
    TopologyRemove {
        address: String,
        node_id: String,
    },
    PacketProcessed {
        duration_ms: u64,
        node_id: String,
    },
}
