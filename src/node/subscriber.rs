use std::collections::HashMap;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use reqwest_eventsource::{Event, EventSource};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use serde::Deserialize;

use crate::broadcast::{broadcast_cluster_snapshot, broadcast_event, broadcast_metrics};
use crate::node::events::IngestEvent;
use crate::node::metrics::{StructuredMetrics, NODE_VERSION_HEADER};
use crate::node::offsets::NodeOffset;
use crate::node::targets::{node_http_client, url_allowed};
use crate::state::{AppState, NodeStatus, MAX_RECENT_EVENTS};

const UPTIME_CONCURRENCY: usize = 20;

#[derive(Deserialize)]
struct TopoNode {
    address: String,
    #[serde(default)]
    layer: u8,
    #[serde(default = "default_role")]
    role: u8,
}

fn default_role() -> u8 {
    1
}

#[derive(Deserialize)]
struct TopoSnapshot {
    nodes: Vec<TopoNode>,
}

async fn sync_topology_from_node(state: &AppState, base_url: &str) {
    let client = node_http_client(Some(Duration::from_secs(5)));

    let url = format!("{base_url}/topology");
    match client.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.json::<TopoSnapshot>().await {
            Ok(snapshot) => {
                let updated_nodes = {
                    let mut nodes = state.nodes.write();
                    let mut changed = Vec::new();
                    for topo_node in &snapshot.nodes {
                        if let Some(ns) = nodes.get_mut(&topo_node.address) {
                            if ns.layer != topo_node.layer || ns.role != topo_node.role {
                                tracing::info!(
                                    "Topology: {} layer {} -> {}, role {} -> {}",
                                    ns.address,
                                    ns.layer,
                                    topo_node.layer,
                                    ns.role,
                                    topo_node.role
                                );
                                ns.layer = topo_node.layer;
                                ns.role = topo_node.role;
                                changed.push(ns.clone());
                            }
                        }
                    }
                    changed
                };

                if !updated_nodes.is_empty() {
                    for node in &updated_nodes {
                        if let Err(e) = state.db.upsert_node(node).await {
                            tracing::warn!("Failed to persist node {}: {e}", node.address);
                        }
                    }
                    broadcast_cluster_snapshot(state);
                }
                tracing::info!(
                    "Topology sync: {} nodes from {base_url}",
                    snapshot.nodes.len()
                );
            }
            Err(e) => {
                tracing::warn!("Failed to parse topology from {base_url}: {e}");
            }
        },
        Ok(resp) => {
            tracing::debug!("Topology fetch from {base_url} returned {}", resp.status());
        }
        Err(e) => {
            tracing::debug!("Topology fetch from {base_url} failed: {e}");
        }
    }
}

pub fn process_event(state: &AppState, node_address: &str, event: &IngestEvent) {
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
    let client = node_http_client(None);

    loop {
        if cancel.is_cancelled() {
            break;
        }

        tracing::info!("SSE connecting to {node_address} at {url}");
        let mut es = match EventSource::new(client.get(&url)) {
            Ok(es) => es,
            Err(error) => {
                tracing::warn!("SSE request for {node_address} could not be built: {error}");
                return;
            }
        };

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
    let client = node_http_client(Some(Duration::from_secs(5)));
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
                        let header_version = resp
                            .headers()
                            .get(NODE_VERSION_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned);
                        match resp.json::<StructuredMetrics>().await {
                            Ok(mut parsed) => {
                                parsed.fill_build_version(header_version.as_deref());
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

/// Fold banked lifetime totals into a freshly scraped reading.
///
/// Detects a node restart, banks the previous incarnation's final counters, then
/// adds the running total onto `parsed` so the dashboard sees a continuous
/// lifetime figure. The write lock is released before any DB call.
async fn apply_lifetime_offsets(state: &AppState, address: &str, parsed: &mut StructuredMetrics) {
    let banked_snapshot = {
        let mut map = state.metric_offsets.write();
        let offset = map.entry(address.to_string()).or_default();
        // Observe the raw reading first; applying before observing would fold
        // previously banked totals back into last_raw and double-count them.
        let restarted = offset.observe(parsed);
        offset.apply(parsed);
        if restarted {
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
    /// The admin URL the tasks were spawned with. A different current URL
    /// (after `updateUrl`) restarts them.
    base_url: String,
    sse_handle: JoinHandle<()>,
    metrics_handle: JoinHandle<()>,
    cancel: CancellationToken,
}

impl ActiveNodeSub {
    fn stop(self) {
        self.cancel.cancel();
        self.sse_handle.abort();
        self.metrics_handle.abort();
    }
}

/// Addresses whose subscription must stop (removed, or URL changed) and
/// addresses that need a new subscription (new, or URL changed).
fn plan_subscription_changes(
    active: &HashMap<String, String>,
    current: &HashMap<String, String>,
) -> (Vec<String>, Vec<String>) {
    let mut stop: Vec<String> = active
        .iter()
        .filter(|(addr, url)| current.get(*addr) != Some(*url))
        .map(|(addr, _)| addr.clone())
        .collect();
    let mut start: Vec<String> = current
        .iter()
        .filter(|(addr, url)| active.get(*addr) != Some(*url))
        .map(|(addr, _)| addr.clone())
        .collect();
    stop.sort();
    start.sort();
    (stop, start)
}

fn resolve_base_url(admin_url: &str, admin_port: u16) -> Option<String> {
    let url = if !admin_url.is_empty() {
        admin_url.to_string()
    } else if admin_port > 0 {
        format!("http://127.0.0.1:{admin_port}")
    } else {
        return None;
    };
    url_allowed(&url).then_some(url)
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
                    sub.stop();
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

        let active_urls: HashMap<String, String> = active
            .iter()
            .map(|(addr, sub)| (addr.clone(), sub.base_url.clone()))
            .collect();
        let (stop, start) = plan_subscription_changes(&active_urls, &current_nodes);

        for addr in &stop {
            if let Some(sub) = active.remove(addr) {
                match current_nodes.get(addr) {
                    Some(url) => tracing::info!(
                        "Node {addr} moved from {} to {url}; resubscribing",
                        sub.base_url
                    ),
                    None => tracing::info!("Unsubscribing from removed node {addr}"),
                }
                sub.stop();
                state.metrics.write().remove(addr);
            }
        }

        let mut new_node_url: Option<String> = None;

        for addr in start {
            let Some(base_url) = current_nodes.get(&addr) else {
                continue;
            };

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
                    base_url: base_url.clone(),
                    sse_handle,
                    metrics_handle,
                    cancel,
                },
            );

            if new_node_url.is_none() {
                new_node_url = Some(base_url.clone());
            }
        }

        if let Some(url) = new_node_url {
            sync_topology_from_node(&state, &url).await;
        }
    }
}

pub async fn uptime_check_loop(state: AppState, interval_secs: u64) {
    let client = node_http_client(Some(Duration::from_secs(5)));

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
                    // Rows written by older builds may hold an address that is
                    // no longer polled: count it as unreachable.
                    if !url_allowed(&admin_url) {
                        return (address, false);
                    }
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

pub async fn periodic_topology_sync(state: AppState) {
    let mut interval = tokio::time::interval(Duration::from_secs(300));
    interval.tick().await; // skip the immediate first tick

    loop {
        tokio::select! {
            _ = state.shutdown.cancelled() => {
                tracing::info!("periodic_topology_sync: shutting down");
                return;
            }
            _ = interval.tick() => {}
        }

        let maybe_url = {
            let nodes = state.nodes.read();
            nodes
                .values()
                .find(|n| n.status == NodeStatus::Online && url_allowed(&n.admin_url))
                .map(|n| n.admin_url.clone())
        };

        if let Some(url) = maybe_url {
            tracing::debug!("Periodic topology sync from {url}");
            sync_topology_from_node(&state, &url).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(addr, url)| (addr.to_string(), url.to_string()))
            .collect()
    }

    #[test]
    fn a_changed_node_url_restarts_its_subscription() {
        let active = urls(&[
            ("0xa", "http://1.1.1.1:15001"),
            ("0xb", "http://2.2.2.2:15001"),
            ("0xc", "http://3.3.3.3:15001"),
        ]);
        let current = urls(&[
            ("0xa", "http://1.1.1.1:15001"),
            ("0xb", "http://4.4.4.4:15001"),
            ("0xd", "http://5.5.5.5:15001"),
        ]);
        let (stop, start) = plan_subscription_changes(&active, &current);
        assert_eq!(stop, vec!["0xb", "0xc"]);
        assert_eq!(start, vec!["0xb", "0xd"]);
    }

    #[test]
    fn unchanged_subscriptions_are_left_alone() {
        let same = urls(&[("0xa", "http://1.1.1.1:15001")]);
        let (stop, start) = plan_subscription_changes(&same, &same);
        assert!(stop.is_empty() && start.is_empty());
    }
}
