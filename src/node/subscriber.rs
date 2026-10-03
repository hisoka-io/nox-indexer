use std::collections::HashMap;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use reqwest_eventsource::{Event, EventSource};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::broadcast::{broadcast_cluster_snapshot, broadcast_event, broadcast_metrics};
use crate::node::events::IngestEvent;
use crate::node::metrics::StructuredMetrics;
use crate::node::offsets::NodeOffset;
use crate::state::{AppState, NodeStatus, MAX_RECENT_EVENTS};

const UPTIME_CONCURRENCY: usize = 20;

/// Whether a node event may be kept in `recent_events` and sent to `/v1/live`.
///
/// Per-packet events are node-internal telemetry and are not republished.
/// Traffic volume stays visible through the metric counters.
pub fn is_republishable(event: &IngestEvent) -> bool {
    !matches!(event, IngestEvent::PacketProcessed { .. })
}

/// Check a membership change reported by a node against the registry state
/// synced from chain. Returns the role to publish for an addition, `Some(None)`
/// for a removal, or `None` when the address is not a known member (an addition
/// must also still be registered).
fn chain_view_of_topology_event(state: &AppState, event: &IngestEvent) -> Option<Option<u8>> {
    let nodes = state.nodes.read();
    match event {
        IngestEvent::TopologyAdd { address, .. } => nodes
            .get(&address.to_lowercase())
            .filter(|node| node.status != NodeStatus::Deregistered)
            .map(|node| Some(node.role)),
        IngestEvent::TopologyRemove { address, .. } => {
            nodes.get(&address.to_lowercase()).map(|_| None)
        }
        _ => Some(None),
    }
}

pub fn process_event(state: &AppState, node_address: &str, event: &IngestEvent) {
    if !is_republishable(event) {
        return;
    }

    // Membership changes are published only for registry members, with the
    // role as synced from chain.
    let Some(chain_role) = chain_view_of_topology_event(state, event) else {
        return;
    };

    let dedup_key = match event {
        IngestEvent::TopologyAdd { address, .. } => Some(format!("topo_add:{address}")),
        IngestEvent::TopologyRemove { address, .. } => Some(format!("topo_remove:{address}")),
        _ => None,
    };
    if let Some(key) = dedup_key {
        if !state.topo_dedup.lock().check(&key) {
            return;
        }
    }

    if let Ok(mut raw) = serde_json::to_value(event) {
        if let Some(obj) = raw.as_object_mut() {
            obj.insert(
                "node_address".into(),
                serde_json::Value::String(node_address.to_string()),
            );
            if let Some(role) = chain_role {
                obj.insert("role".into(), serde_json::Value::from(role));
            }
        }
        {
            let mut buffer = state.recent_events.write();
            buffer.push_back(raw.clone());
            while buffer.len() > MAX_RECENT_EVENTS {
                buffer.pop_front();
            }
        }
        broadcast_event(state, &raw);
    }
}

async fn subscribe_node_events(
    state: AppState,
    node_address: String,
    base_url: String,
    cancel: CancellationToken,
) {
    let url = format!("{base_url}/events");

    loop {
        if cancel.is_cancelled() {
            break;
        }

        tracing::info!("SSE connecting to {node_address} at {url}");
        let mut es = EventSource::get(&url);

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    es.close();
                    tracing::debug!("SSE cancelled for {node_address}");
                    return;
                }
                next = es.next() => {
                    match next {
                        Some(Ok(Event::Open)) => {
                            tracing::info!("SSE connected to {node_address}");
                        }
                        Some(Ok(Event::Message(msg))) => {
                            match serde_json::from_str::<IngestEvent>(&msg.data) {
                                Ok(event) => process_event(&state, &node_address, &event),
                                Err(e) => {
                                    tracing::warn!(
                                        "SSE parse error for {node_address}: {e} — data: {}",
                                        &msg.data[..msg.data.len().min(200)]
                                    );
                                }
                            }
                        }
                        Some(Err(err)) => {
                            tracing::warn!("SSE error for {node_address}: {err}");
                            es.close();
                            break;
                        }
                        None => {
                            tracing::warn!("SSE stream ended for {node_address}");
                            break;
                        }
                    }
                }
            }
        }

        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(Duration::from_secs(3)) => {}
        }
    }
}

async fn scrape_node_metrics(
    state: AppState,
    node_address: String,
    base_url: String,
    cancel: CancellationToken,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();
    let metrics_url = format!("{base_url}/metrics/json");
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                tracing::debug!("Metrics scraper cancelled for {node_address}");
                return;
            }
            _ = interval.tick() => {
                match client.get(&metrics_url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.json::<StructuredMetrics>().await {
                            // The scraper can outlive a removal by one tick;
                            // only current members feed the totals.
                            Ok(_) if !is_registered_member(&state, &node_address) => {
                                tracing::debug!(
                                    "Ignoring metrics from {node_address}: not a registered member"
                                );
                            }
                            Ok(mut parsed) => {
                                apply_lifetime_offsets(&state, &node_address, &mut parsed).await;
                                state.metrics.write().insert(node_address.clone(), parsed.clone());
                                broadcast_metrics(&state, &node_address, &parsed);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to parse metrics JSON for {node_address}: {e}"
                                );
                            }
                        }
                    }
                    Ok(resp) => {
                        tracing::warn!(
                            "Metrics scrape for {node_address} returned {}",
                            resp.status()
                        );
                    }
                    Err(e) => {
                        tracing::warn!("Metrics scrape failed for {node_address}: {e}");
                    }
                }
            }
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn is_registered_member(state: &AppState, address: &str) -> bool {
    state
        .nodes
        .read()
        .get(address)
        .is_some_and(|node| node.status != NodeStatus::Deregistered)
}

/// Fold banked lifetime totals into a freshly scraped reading.
///
/// Detects a node restart, banks the previous incarnation's final counters, then
/// replaces the counters in `parsed` with the node's accepted lifetime totals so
/// the dashboard sees a continuous figure. The write lock is released before
/// any DB call.
async fn apply_lifetime_offsets(state: &AppState, address: &str, parsed: &mut StructuredMetrics) {
    let banked_snapshot = {
        let mut map = state.metric_offsets.write();
        let offset = map.entry(address.to_string()).or_default();
        // Observe the raw reading first; applying before observing would fold
        // previously banked totals back into last_raw and double-count them.
        let seen = offset.observe(parsed, std::time::Instant::now());
        offset.apply(parsed);
        if seen.adjusted && !offset.adjusting {
            tracing::warn!(
                "Node {address} reported counters that went backwards or grew faster \
                 than the accepted rate; holding or capping them"
            );
        }
        offset.adjusting = seen.adjusted;
        if seen.restarted {
            Some(offset.clone())
        } else {
            None
        }
    };

    let Some(offset) = banked_snapshot else {
        return;
    };

    tracing::info!(
        "Node {address} restarted (incarnation {}); banked lifetime totals \
         (packets_received={:.0}, uptime_seconds={:.0})",
        offset.last_node_start_time,
        offset.banked.packets_received,
        offset.banked.uptime_seconds,
    );

    if let Err(e) = state
        .db
        .save_metric_offset(address, &offset, now_ms())
        .await
    {
        tracing::warn!("Failed to persist metric offsets for {address}: {e}");
    } else if let Some(entry) = state.metric_offsets.write().get_mut(address) {
        entry.dirty = false;
    }
}

/// Periodically flush banked totals so an indexer restart loses at most one
/// interval of in-flight counts rather than the whole current incarnation.
pub async fn metric_offset_flush_loop(state: AppState, interval_secs: u64) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.tick().await; // skip the immediate first tick

    loop {
        tokio::select! {
            () = state.shutdown.cancelled() => {
                flush_dirty_offsets(&state).await;
                tracing::info!("metric_offset_flush_loop: shutting down");
                return;
            }
            _ = interval.tick() => {}
        }

        flush_dirty_offsets(&state).await;
    }
}

async fn flush_dirty_offsets(state: &AppState) {
    let pending: Vec<(String, NodeOffset)> = {
        let map = state.metric_offsets.read();
        map.iter()
            .filter(|(_, o)| o.dirty || !o.last_raw.is_zero())
            .map(|(a, o)| (a.clone(), o.clone()))
            .collect()
    };

    let ts = now_ms();
    for (address, offset) in pending {
        if let Err(e) = state.db.save_metric_offset(&address, &offset, ts).await {
            tracing::warn!("Failed to flush metric offsets for {address}: {e}");
            continue;
        }
        if let Some(entry) = state.metric_offsets.write().get_mut(&address) {
            entry.dirty = false;
        }
    }
}

struct ActiveNodeSub {
    sse_handle: JoinHandle<()>,
    metrics_handle: JoinHandle<()>,
    cancel: CancellationToken,
}

fn resolve_base_url(admin_url: &str, admin_port: u16) -> Option<String> {
    if !admin_url.is_empty() {
        Some(admin_url.to_string())
    } else if admin_port > 0 {
        Some(format!("http://127.0.0.1:{admin_port}"))
    } else {
        None
    }
}

pub async fn manage_subscriptions(state: AppState) {
    let mut active: HashMap<String, ActiveNodeSub> = HashMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => {
                tracing::info!("manage_subscriptions: shutting down, cancelling all node subscriptions");
                for (addr, sub) in active.drain() {
                    tracing::debug!("Cancelling subscription for {addr}");
                    sub.cancel.cancel();
                }
                return;
            }
            _ = interval.tick() => {}
        }

        let current_nodes: HashMap<String, String> = state
            .nodes
            .read()
            .iter()
            .filter_map(|(addr, ns)| {
                resolve_base_url(&ns.admin_url, ns.admin_port).map(|url| (addr.clone(), url))
            })
            .collect();

        for (addr, base_url) in &current_nodes {
            if active.contains_key(addr) {
                continue;
            }

            tracing::info!("Subscribing to node {addr} at {base_url}");
            let cancel = CancellationToken::new();

            let sse_handle = tokio::spawn(subscribe_node_events(
                state.clone(),
                addr.clone(),
                base_url.clone(),
                cancel.clone(),
            ));

            let metrics_handle = tokio::spawn(scrape_node_metrics(
                state.clone(),
                addr.clone(),
                base_url.clone(),
                cancel.clone(),
            ));

            active.insert(
                addr.clone(),
                ActiveNodeSub {
                    sse_handle,
                    metrics_handle,
                    cancel,
                },
            );
        }

        let removed: Vec<String> = active
            .keys()
            .filter(|addr| !current_nodes.contains_key(*addr))
            .cloned()
            .collect();

        for addr in removed {
            if let Some(sub) = active.remove(&addr) {
                tracing::info!("Unsubscribing from removed node {addr}");
                sub.cancel.cancel();
                sub.sse_handle.abort();
                sub.metrics_handle.abort();
                state.metrics.write().remove(&addr);
            }
        }
    }
}

pub async fn uptime_check_loop(state: AppState, interval_secs: u64) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => {
                tracing::info!("uptime_check_loop: shutting down");
                return;
            }
            _ = interval.tick() => {}
            // Chain sync registered new members: probe now so they do not sit
            // "offline" for a whole interval after a registry change.
            () = state.probe_now.notified() => {}
        }

        let targets = match state.db.load_all_uptime_targets().await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("Failed to load uptime targets: {e}");
                continue;
            }
        };

        let probe_results: Vec<(String, bool)> = stream::iter(targets.iter().cloned())
            .map(|(address, admin_url)| {
                let client = client.clone();
                async move {
                    let metrics_url = format!("{admin_url}/metrics/json");
                    let reachable = client
                        .get(&metrics_url)
                        .send()
                        .await
                        .map(|r| r.status().is_success())
                        .unwrap_or(false);
                    (address, reachable)
                }
            })
            .buffer_unordered(UPTIME_CONCURRENCY)
            .collect()
            .await;

        let mut status_changed = false;

        for (address, reachable) in &probe_results {
            if let Err(e) = state.db.upsert_reputation(address, *reachable).await {
                tracing::warn!("Failed to update reputation for {address}: {e}");
            }

            if apply_status_change(&state, address, *reachable).await {
                status_changed = true;
            }
        }

        if status_changed {
            broadcast_cluster_snapshot(&state);
        }

        if !targets.is_empty() {
            tracing::debug!("Uptime check complete: {} nodes probed", targets.len());
        }

        if let Err(e) = state.db.decay_stale_scores(6).await {
            tracing::warn!("Failed to decay stale scores: {e}");
        }

        if state.network_genesis_ms.read().is_none() && !probe_results.is_empty() {
            let interval_ms =
                i64::try_from(interval_secs.saturating_mul(1_000)).unwrap_or(i64::MAX);
            match state.db.ensure_network_genesis(interval_ms).await {
                Ok(genesis) => *state.network_genesis_ms.write() = genesis,
                Err(e) => tracing::warn!("Failed to record network genesis: {e}"),
            }
        }
    }
}

async fn apply_status_change(state: &AppState, address: &str, reachable: bool) -> bool {
    let new_status = if reachable {
        NodeStatus::Online
    } else {
        NodeStatus::Offline
    };

    let needs_update = {
        let nodes = state.nodes.read();
        matches!(
            nodes.get(address),
            Some(node) if node.status != NodeStatus::Deregistered && node.status != new_status
        )
    };

    if !needs_update {
        return false;
    }

    if let Err(e) = state.db.update_node_status(address, &new_status).await {
        tracing::warn!("Failed to persist status for {address}: {e}");
        return false;
    }

    let mut nodes = state.nodes.write();
    if let Some(node) = nodes.get_mut(address) {
        if node.status != NodeStatus::Deregistered && node.status != new_status {
            tracing::info!(
                "Node {} ({}) status: {} -> {}",
                node.id,
                node.address,
                node.status,
                new_status
            );
            node.status = new_status;
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::NodeState;
    use tokio::sync::broadcast::error::TryRecvError;

    const NODE: &str = "0x1111111111111111111111111111111111111111";

    fn member(address: &str, status: NodeStatus) -> NodeState {
        let mut node = NodeState::from_chain_info(
            &crate::chain::OnChainNode {
                address: address.to_string(),
                url: "/ip4/127.0.0.1/tcp/9000".to_string(),
                ingress_url: String::new(),
                metadata_url: String::new(),
                sphinx_key: "11".repeat(32),
                role: 1,
                frozen: false,
            },
            421_614,
            "0xabc",
        );
        node.status = status;
        node
    }

    #[tokio::test]
    async fn per_packet_events_are_neither_stored_nor_republished() {
        let state = AppState::for_tests();
        let mut live = state.tx.subscribe();

        process_event(
            &state,
            NODE,
            &IngestEvent::PacketProcessed {
                duration_ms: 42,
                node_id: "nox-11111111".to_string(),
            },
        );
        assert!(state.recent_events.read().is_empty());
        assert!(matches!(live.try_recv(), Err(TryRecvError::Empty)));

        process_event(
            &state,
            NODE,
            &IngestEvent::PeerConnected {
                peer_id: "peer".to_string(),
                node_id: "nox-11111111".to_string(),
            },
        );
        let recent = state.recent_events.read().clone();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0]["kind"], "peer_connected");
        assert_eq!(recent[0]["node_address"], NODE);
        let message: serde_json::Value =
            serde_json::from_str(&live.try_recv().expect("event is published")).unwrap();
        assert_eq!(message["type"], "EVENT");
        assert_eq!(message["payload"]["kind"], "peer_connected");
    }

    #[tokio::test]
    async fn membership_events_are_published_only_for_registry_members() {
        let state = AppState::for_tests();
        let departed = "0x2222222222222222222222222222222222222222";
        let unknown = "0x3333333333333333333333333333333333333333";
        {
            let mut nodes = state.nodes.write();
            nodes.insert(NODE.to_string(), member(NODE, NodeStatus::Online));
            nodes.insert(
                departed.to_string(),
                member(departed, NodeStatus::Deregistered),
            );
        }
        let add = |address: &str, role: u8| IngestEvent::TopologyAdd {
            address: address.to_string(),
            role,
            stake: "0".to_string(),
            node_id: "nox-reporter".to_string(),
        };
        let remove = |address: &str| IngestEvent::TopologyRemove {
            address: address.to_string(),
            node_id: "nox-reporter".to_string(),
        };

        process_event(&state, NODE, &add(unknown, 2));
        process_event(&state, NODE, &add(departed, 2));
        process_event(&state, NODE, &remove(unknown));
        assert!(state.recent_events.read().is_empty());

        // A member's addition carries the registry role, whatever was reported.
        process_event(
            &state,
            NODE,
            &add(&NODE.to_uppercase().replace("0X", "0x"), 2),
        );
        process_event(&state, NODE, &remove(departed));
        let recent = state.recent_events.read().clone();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0]["kind"], "topology_add");
        assert_eq!(recent[0]["role"], 1);
        assert_eq!(recent[1]["kind"], "topology_remove");
        assert_eq!(recent[1]["address"], departed);
    }

    #[test]
    fn per_packet_events_still_parse() {
        // Nodes keep sending them; they must be dropped quietly, not logged as
        // parse errors on every packet.
        let event: IngestEvent = serde_json::from_str(
            r#"{"kind":"packet_processed","duration_ms":12,"node_id":"nox-1"}"#,
        )
        .unwrap();
        assert!(!is_republishable(&event));
    }

    #[tokio::test]
    async fn only_registered_members_feed_metrics() {
        let state = AppState::for_tests();
        let departed = "0x2222222222222222222222222222222222222222";
        {
            let mut nodes = state.nodes.write();
            nodes.insert(NODE.to_string(), member(NODE, NodeStatus::Offline));
            nodes.insert(
                departed.to_string(),
                member(departed, NodeStatus::Deregistered),
            );
        }

        assert!(is_registered_member(&state, NODE));
        assert!(!is_registered_member(&state, departed));
        assert!(!is_registered_member(
            &state,
            "0x3333333333333333333333333333333333333333"
        ));
    }

    #[tokio::test]
    async fn scraped_counters_are_bounded_before_they_reach_the_totals() {
        let state = AppState::for_tests();
        let mut first = StructuredMetrics {
            node_start_time: 1_000.0,
            packets_received: 10.0,
            ..Default::default()
        };
        apply_lifetime_offsets(&state, NODE, &mut first).await;
        assert_eq!(first.packets_received, 10.0);

        let mut inflated = StructuredMetrics {
            node_start_time: 1_000.0,
            packets_received: 1.0e15,
            ..Default::default()
        };
        apply_lifetime_offsets(&state, NODE, &mut inflated).await;
        assert!(
            inflated.packets_received < 1.0e5,
            "{}",
            inflated.packets_received
        );
        assert!(state.metric_offsets.read()[NODE].adjusting);

        let totals = crate::node::offsets::network_totals(state.metric_offsets.read().values());
        assert_eq!(totals.packets_received, inflated.packets_received);
    }
}
