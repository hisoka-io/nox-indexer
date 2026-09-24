//! Registry membership sync.
//!
//! `run_chain_sync` supervises everything: it resumes from the per-registry
//! checkpoint when the configured registry already has one, otherwise replays
//! from `FROM_BLOCK`, verifies the resulting member set against the registry's
//! `relayerCount` and `topologyFingerprint`, reconciles Postgres, then follows
//! new blocks. Failures are retried forever with backoff; a sync attempt never
//! gives up into a silent "database only" mode.

use ethers::prelude::*;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::broadcast::broadcast_cluster_snapshot;
use crate::state::{AppState, NodeState, NodeStatus, SyncPhase};

use super::retry::jittered_backoff;
use super::{apply_membership_event, retry_rpc, retry_rpc_attempts, ChainConfig, ChainNodeEvent};

const SYNC_BACKOFF_BASE: Duration = Duration::from_secs(2);
const SYNC_BACKOFF_CAP: Duration = Duration::from_secs(300);
/// How often the live loop re-checks membership against the registry.
const VERIFY_INTERVAL: Duration = Duration::from_secs(60);
/// Consecutive verification failures before the live loop forces a full replay.
const MISMATCHES_BEFORE_RESYNC: u32 = 2;
/// Floor between two forced full replays, so a persistent mismatch cannot spin.
/// The first forced replay of the process is not delayed: a missed event should
/// heal within `MISMATCHES_BEFORE_RESYNC` checks, not after ten minutes.
const MIN_RESYNC_INTERVAL: Duration = Duration::from_secs(600);
/// Full replays that disagree with the registry before the replayed set is
/// served anyway (flagged unverified) instead of retrying again.
const FULL_REPLAY_MISMATCH_TOLERANCE: u32 = 2;
/// Membership-check rounds per sync attempt, each at a fresh head.
const VERIFY_ATTEMPTS: u32 = 5;
/// Quick retries at the same block (rate limits) before re-targeting the head.
const SAME_BLOCK_VERIFY_ATTEMPTS: u32 = 3;
const PROFILE_REFRESH_CONCURRENCY: usize = 4;
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPlan {
    /// Replay every registry event from the deployment block.
    Full { from: u64 },
    /// Continue after a checkpoint, on top of the member set stored for this registry.
    Resume { from: u64 },
}

impl SyncPlan {
    fn start(self) -> u64 {
        match self {
            SyncPlan::Full { from } | SyncPlan::Resume { from } => from,
        }
    }

    fn is_resume(self) -> bool {
        matches!(self, SyncPlan::Resume { .. })
    }
}

/// Decide between resuming and a full replay. A checkpoint only counts when it
/// belongs to the configured `(chain_id, registry)` (the caller looks it up by
/// that key) and is not below the deployment block.
pub fn plan_sync(checkpoint: Option<u64>, from_block: u64, force_full: bool) -> SyncPlan {
    match checkpoint {
        Some(last) if !force_full && last.saturating_add(1) >= from_block => {
            SyncPlan::Resume { from: last + 1 }
        }
        _ => SyncPlan::Full { from: from_block },
    }
}

/// Replay state that survives failed attempts, so a 429 halfway through a long
/// replay resumes where it stopped instead of starting over.
struct ReplayProgress {
    chain_id: u64,
    plan: SyncPlan,
    next_block: u64,
    members: HashSet<Address>,
}

enum SyncError {
    Rpc(String),
    /// A full replay disagrees with the registry's count/fingerprint.
    Mismatch(String),
}

impl From<String> for SyncError {
    fn from(error: String) -> Self {
        SyncError::Rpc(error)
    }
}

enum LoopExit {
    Shutdown,
    Resync(String),
}

/// Supervise chain discovery for the lifetime of the process.
pub async fn run_chain_sync(state: AppState, chain: Arc<ChainConfig>) {
    let mut attempt = 0_u32;
    let mut progress: Option<ReplayProgress> = None;
    let mut force_full = false;
    let mut full_mismatches = 0_u32;
    let mut last_forced_resync: Option<Instant> = None;

    loop {
        if state.shutdown.is_cancelled() {
            return;
        }
        let accept_unverified = full_mismatches >= FULL_REPLAY_MISMATCH_TOLERANCE;
        let outcome =
            initial_chain_sync(&state, &chain, &mut progress, force_full, accept_unverified).await;

        let error = match outcome {
            Ok(last_block) => {
                attempt = 0;
                full_mismatches = 0;
                let resync_not_before =
                    last_forced_resync.map_or_else(Instant::now, |at| at + MIN_RESYNC_INTERVAL);
                match chain_event_loop(&state, &chain, last_block, resync_not_before).await {
                    LoopExit::Shutdown => return,
                    LoopExit::Resync(reason) => {
                        last_forced_resync = Some(Instant::now());
                        tracing::warn!(
                            "Registry membership drifted ({reason}); running a full replay"
                        );
                        force_full = true;
                        progress = None;
                        state.sync.write().phase = SyncPhase::Syncing;
                        continue;
                    }
                }
            }
            Err(SyncError::Mismatch(reason)) => {
                full_mismatches += 1;
                progress = None;
                // The checkpointed set already failed verification on the way here.
                force_full = true;
                format!("full replay disagrees with registry: {reason}")
            }
            Err(SyncError::Rpc(error)) => error,
        };

        if state.shutdown.is_cancelled() {
            return;
        }
        attempt += 1;
        let delay = jittered_backoff(attempt, SYNC_BACKOFF_BASE, SYNC_BACKOFF_CAP);
        tracing::error!(
            "Chain sync attempt {attempt} failed, retrying in {delay:?} (replay progress kept: {}): {error}",
            progress.is_some()
        );
        {
            let mut sync = state.sync.write();
            sync.phase = SyncPhase::Retrying;
            sync.attempts = attempt;
            sync.last_error = Some(error);
        }
        tokio::select! {
            () = state.shutdown.cancelled() => return,
            () = tokio::time::sleep(delay) => {}
        }
    }
}

async fn initial_chain_sync(
    state: &AppState,
    chain: &ChainConfig,
    progress: &mut Option<ReplayProgress>,
    force_full: bool,
    accept_unverified: bool,
) -> Result<u64, SyncError> {
    let chain_id = chain.chain_id().await?;
    {
        let mut sync = state.sync.write();
        sync.chain_id = Some(chain_id);
        if sync.phase != SyncPhase::Retrying {
            sync.phase = SyncPhase::Syncing;
        }
    }

    let mut force_full = force_full;
    loop {
        if progress.as_ref().is_none_or(|p| p.chain_id != chain_id) {
            *progress = Some(new_progress(state, chain, chain_id, force_full).await?);
        }
        let replay = progress.as_mut().expect("progress initialised above");
        let plan = replay.plan;

        // Verify at a block that was head moments ago: non-archive endpoints can
        // drop state within minutes, so a failed check re-targets the current head
        // (scanning the few new blocks) instead of retrying an ageing block.
        let mut check_attempt = 0_u32;
        let (target, verdict) = loop {
            let target = chain
                .safe_head()
                .await?
                .max(replay.next_block.saturating_sub(1));
            scan_membership(state, chain, replay, target).await?;
            let members = &replay.members;
            let check = retry_rpc_attempts(
                "registry membership check",
                &state.shutdown,
                SAME_BLOCK_VERIFY_ATTEMPTS,
                || chain.verify_membership(members, target),
            )
            .await;
            match check {
                Ok(verdict) => break (target, verdict),
                Err(error) => {
                    check_attempt += 1;
                    if check_attempt >= VERIFY_ATTEMPTS {
                        return Err(SyncError::Rpc(format!(
                            "registry membership check failed {check_attempt} times: {error}"
                        )));
                    }
                    let delay = jittered_backoff(
                        check_attempt,
                        Duration::from_millis(500),
                        SYNC_BACKOFF_BASE,
                    );
                    tracing::warn!(
                        "Registry membership check at block {target} failed \
                         ({check_attempt}/{VERIFY_ATTEMPTS}), re-targeting head in {delay:?}: {error}"
                    );
                    tokio::select! {
                        () = state.shutdown.cancelled() => {
                            return Err(SyncError::Rpc("shutting down".to_string()))
                        }
                        () = tokio::time::sleep(delay) => {}
                    }
                }
            }
        };

        let verified = match verdict {
            Ok(()) => true,
            Err(reason) if plan.is_resume() => {
                tracing::warn!(
                    "Checkpoint resume disagrees with the registry at block {target} ({reason}); \
                     falling back to a full replay from block {}",
                    chain.from_block
                );
                *progress = None;
                force_full = true;
                continue;
            }
            Err(reason) if !accept_unverified => return Err(SyncError::Mismatch(reason)),
            Err(reason) => {
                tracing::error!(
                    "Full replay still disagrees with the registry ({reason}); serving the \
                     replayed set flagged unverified"
                );
                false
            }
        };

        let members = replay.members.clone();
        reconcile_membership(state, chain, chain_id, &members, target, verified).await?;
        *progress = None;
        return Ok(target);
    }
}

async fn new_progress(
    state: &AppState,
    chain: &ChainConfig,
    chain_id: u64,
    force_full: bool,
) -> Result<ReplayProgress, String> {
    let checkpoint = state
        .db
        .get_checkpoint(chain_id, &chain.registry_hex)
        .await
        .map_err(|e| format!("load checkpoint: {e}"))?;
    let plan = plan_sync(checkpoint, chain.from_block, force_full);

    let members = match plan {
        SyncPlan::Full { .. } => HashSet::new(),
        SyncPlan::Resume { .. } => state
            .db
            .load_registry_member_addresses(chain_id, &chain.registry_hex)
            .await
            .map_err(|e| format!("load registry members: {e}"))?
            .iter()
            .filter_map(|address| address.parse::<Address>().ok())
            .collect(),
    };

    match plan {
        SyncPlan::Resume { from } => tracing::info!(
            "Chain sync: resuming registry {} (chain {chain_id}) from checkpoint block {} with {} stored members",
            chain.registry_hex,
            from - 1,
            members.len()
        ),
        SyncPlan::Full { from } => tracing::info!(
            "Chain sync: full replay of registry {} (chain {chain_id}) from block {from}{}",
            chain.registry_hex,
            if force_full { " (forced)" } else { " (no checkpoint for this registry)" }
        ),
    }

    Ok(ReplayProgress {
        chain_id,
        plan,
        next_block: plan.start(),
        members,
    })
}

async fn scan_membership(
    state: &AppState,
    chain: &ChainConfig,
    replay: &mut ReplayProgress,
    target: u64,
) -> Result<(), String> {
    let first = replay.next_block;
    let mut last_log = Instant::now();
    while replay.next_block <= target {
        let (logs, end) = chain
            .fetch_logs_chunk(replay.next_block, target, &state.shutdown)
            .await?;
        for log in &logs {
            if let Some(event) = chain.decode_log(log) {
                apply_membership_event(&mut replay.members, &event);
            }
        }
        replay.next_block = end + 1;

        if last_log.elapsed() >= PROGRESS_LOG_INTERVAL || replay.next_block > target {
            last_log = Instant::now();
            let total = target.saturating_sub(first) + 1;
            let done = end.saturating_sub(first) + 1;
            tracing::info!(
                "Chain sync: scanned {first}..{end} of {target} ({:.1}%), {} members so far",
                done as f64 * 100.0 / total.max(1) as f64,
                replay.members.len()
            );
            state.sync.write().scan_block = Some(end);
        }
    }
    Ok(())
}

/// Make Postgres and memory match a verified member set at `block`.
///
/// Registered members are upserted under this registry. Every other row that is
/// still marked registered (any registry) becomes `deregistered`. Stats tables
/// are keyed by address and never touched here, so a node that re-registers
/// (for example on a new registry) resumes its counters.
async fn reconcile_membership(
    state: &AppState,
    chain: &ChainConfig,
    chain_id: u64,
    members: &HashSet<Address>,
    block: u64,
    verified: bool,
) -> Result<(), String> {
    let previous: HashMap<String, NodeStatus> = state
        .nodes
        .read()
        .iter()
        .map(|(address, node)| (address.to_lowercase(), node.status.clone()))
        .collect();

    let mut ordered: Vec<Address> = members.iter().copied().collect();
    ordered.sort();
    let shutdown = &state.shutdown;
    let profiles: Vec<(Address, Option<crate::chain::OnChainNode>)> = stream::iter(ordered)
        .map(|address| async move {
            let info = retry_rpc("relayer profile", shutdown, || {
                chain.fetch_node_info(address)
            })
            .await?;
            Ok::<_, String>((address, info))
        })
        .buffered(PROFILE_REFRESH_CONCURRENCY)
        .try_collect()
        .await?;

    let mut fresh: HashMap<String, NodeState> = HashMap::with_capacity(profiles.len());
    for (address, info) in profiles {
        let Some(info) = info else {
            tracing::info!(
                "{address:?} left the registry after block {block}; the live loop will apply it"
            );
            continue;
        };
        let mut node = NodeState::from_chain_info(&info, chain_id, &chain.registry_hex);
        node.apply_geo(&state.geo);
        if let Some(status) = previous.get(&node.address) {
            if *status != NodeStatus::Deregistered {
                node.status = status.clone();
            }
        }
        state
            .db
            .upsert_node(&node)
            .await
            .map_err(|e| format!("persist node {}: {e}", node.address))?;
        fresh.insert(node.address.clone(), node);
    }

    let keep: Vec<String> = fresh.keys().cloned().collect();
    let pruned = state
        .db
        .deregister_nodes_except(&keep)
        .await
        .map_err(|e| format!("deregister stale nodes: {e}"))?;
    for address in &pruned {
        tracing::info!(
            "Deregistered {address}: not a member of registry {}",
            chain.registry_hex
        );
    }

    state
        .db
        .set_checkpoint(chain_id, &chain.registry_hex, block)
        .await
        .map_err(|e| format!("persist checkpoint: {e}"))?;

    let count = fresh.len();
    *state.nodes.write() = fresh;
    state
        .metrics
        .write()
        .retain(|address, _| keep.iter().any(|kept| kept == address));
    state.mark_synced(block, verified);
    state.bump_topology_version();
    broadcast_cluster_snapshot(state);
    state.probe_now.notify_one();

    tracing::info!(
        "Chain sync complete: {count} registered nodes at block {block} ({} deregistered, verified={verified})",
        pruned.len()
    );
    Ok(())
}

/// Whether the live loop should abandon incremental updates for a full replay.
fn should_force_resync(mismatches: u32, now: Instant, resync_not_before: Instant) -> bool {
    mismatches >= MISMATCHES_BEFORE_RESYNC && now >= resync_not_before
}

async fn chain_event_loop(
    state: &AppState,
    chain: &ChainConfig,
    mut last_block: u64,
    resync_not_before: Instant,
) -> LoopExit {
    let mut interval = tokio::time::interval(chain.poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_verified = Instant::now();
    let mut mismatches = 0_u32;

    loop {
        tokio::select! {
            () = state.shutdown.cancelled() => {
                tracing::info!("chain_event_loop: shutting down at block {last_block}");
                return LoopExit::Shutdown;
            }
            _ = interval.tick() => {}
        }

        let target = match chain.safe_head().await {
            Ok(block) => block,
            Err(error) => {
                tracing::warn!("Chain event loop: {error}");
                continue;
            }
        };

        let mut changed = false;
        while last_block < target {
            match chain
                .fetch_logs_chunk(last_block + 1, target, &state.shutdown)
                .await
            {
                Ok((logs, end)) => {
                    for log in &logs {
                        if let Some(event) = chain.decode_log(log) {
                            changed |= process_chain_event(state, chain, event).await;
                        }
                    }
                    last_block = end;
                    let chain_id = state.sync.read().chain_id.unwrap_or_default();
                    if let Err(error) = state
                        .db
                        .set_checkpoint(chain_id, &chain.registry_hex, last_block)
                        .await
                    {
                        tracing::warn!("Failed to persist checkpoint {last_block}: {error}");
                    }
                    state.sync.write().last_block = Some(last_block);
                }
                Err(error) => {
                    if state.shutdown.is_cancelled() {
                        return LoopExit::Shutdown;
                    }
                    tracing::warn!("Chain event loop: {error}");
                    break;
                }
            }
        }
        if changed {
            state.bump_topology_version();
        }

        if !changed && last_verified.elapsed() < VERIFY_INTERVAL {
            continue;
        }
        last_verified = Instant::now();
        let members: HashSet<Address> = state
            .nodes
            .read()
            .values()
            .filter(|node| node.status != NodeStatus::Deregistered)
            .filter_map(|node| node.address.parse::<Address>().ok())
            .collect();
        match chain.verify_membership(&members, last_block).await {
            Ok(Ok(())) => {
                mismatches = 0;
                state.sync.write().verified = Some(true);
            }
            Ok(Err(reason)) => {
                mismatches += 1;
                state.sync.write().verified = Some(false);
                tracing::warn!(
                    "Membership check {mismatches}/{MISMATCHES_BEFORE_RESYNC} at block {last_block} failed: {reason}"
                );
                if should_force_resync(mismatches, Instant::now(), resync_not_before) {
                    return LoopExit::Resync(reason);
                }
            }
            Err(error) => tracing::debug!("Membership check skipped: {error}"),
        }
    }
}

/// Apply one live event. Returns true when membership or a profile changed.
async fn process_chain_event(state: &AppState, chain: &ChainConfig, event: ChainNodeEvent) -> bool {
    match event {
        ChainNodeEvent::Registered { address } => {
            tracing::info!("Chain: new registration for {address:?}");
            refresh_chain_node(state, chain, address).await;
            state.probe_now.notify_one();
            true
        }
        ChainNodeEvent::ProfileChanged { address } => {
            tracing::info!("Chain: profile changed for {address:?}");
            refresh_chain_node(state, chain, address).await;
            true
        }
        ChainNodeEvent::Removed { address } => {
            tracing::info!("Chain: node deregistered {address:?}");
            remove_chain_node(state, address).await;
            true
        }
        ChainNodeEvent::Unstaked { address, amount } => {
            tracing::info!("Chain: node {address:?} unstaked {amount} and left the registry");
            remove_chain_node(state, address).await;
            true
        }
    }
}

async fn remove_chain_node(state: &AppState, address: Address) {
    let addr_str = format!("{address:?}");
    if let Err(error) = state.db.deregister_node(&addr_str).await {
        // Keep the in-memory entry: the next membership check flags the drift
        // and forces a replay rather than silently losing the removal.
        tracing::warn!("Failed to deregister node {addr_str} in DB: {error}");
        return;
    }
    state.nodes.write().remove(&addr_str);
    state.metrics.write().remove(&addr_str);
    broadcast_cluster_snapshot(state);
}

async fn refresh_chain_node(state: &AppState, chain: &ChainConfig, address: Address) {
    let address_text = format!("{address:?}");
    let info = retry_rpc("relayer profile", &state.shutdown, || {
        chain.fetch_node_info(address)
    })
    .await;
    match info {
        Ok(Some(info)) => {
            let chain_id = state.sync.read().chain_id.unwrap_or_default();
            let mut node = NodeState::from_chain_info(&info, chain_id, &chain.registry_hex);
            node.apply_geo(&state.geo);
            if let Some(existing) = state.nodes.read().get(&node.address) {
                if existing.status != NodeStatus::Deregistered {
                    node.status = existing.status.clone();
                }
            }

            if let Err(error) = state.db.upsert_node(&node).await {
                tracing::warn!("Failed to persist node {}: {error}", node.address);
                return;
            }
            state.nodes.write().insert(node.address.clone(), node);
            broadcast_cluster_snapshot(state);
        }
        Ok(None) => {
            tracing::info!("Chain: {address_text} is no longer registered; skipping refresh");
        }
        Err(error) => {
            tracing::warn!("Chain: failed to fetch profile for {address_text}: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_checkpoint_means_a_full_replay_from_the_deployment_block() {
        assert_eq!(plan_sync(None, 100, false), SyncPlan::Full { from: 100 });
    }

    #[test]
    fn a_checkpoint_for_the_configured_registry_resumes_after_it() {
        assert_eq!(
            plan_sync(Some(500), 100, false),
            SyncPlan::Resume { from: 501 }
        );
        // A checkpoint just before the deployment block is still contiguous.
        assert_eq!(
            plan_sync(Some(99), 100, false),
            SyncPlan::Resume { from: 100 }
        );
    }

    #[test]
    fn a_checkpoint_below_the_deployment_block_forces_a_full_replay() {
        assert_eq!(
            plan_sync(Some(50), 100, false),
            SyncPlan::Full { from: 100 }
        );
    }

    #[test]
    fn forced_resync_ignores_the_checkpoint() {
        assert_eq!(
            plan_sync(Some(500), 100, true),
            SyncPlan::Full { from: 100 }
        );
    }

    #[test]
    fn the_first_forced_resync_is_not_delayed_but_later_ones_are() {
        let now = Instant::now();
        // First drift of the process: resync once enough checks disagree.
        assert!(!should_force_resync(1, now, now));
        assert!(should_force_resync(MISMATCHES_BEFORE_RESYNC, now, now));
        // Right after a forced resync, the floor applies.
        let floor = now + MIN_RESYNC_INTERVAL;
        assert!(!should_force_resync(MISMATCHES_BEFORE_RESYNC, now, floor));
        assert!(should_force_resync(MISMATCHES_BEFORE_RESYNC, floor, floor));
    }

    #[test]
    fn membership_replay_handles_every_removal_path() {
        let a = Address::from_low_u64_be(1);
        let b = Address::from_low_u64_be(2);
        let mut set = HashSet::new();
        assert!(apply_membership_event(
            &mut set,
            &ChainNodeEvent::Registered { address: a }
        ));
        assert!(apply_membership_event(
            &mut set,
            &ChainNodeEvent::Registered { address: b }
        ));
        assert!(!apply_membership_event(
            &mut set,
            &ChainNodeEvent::ProfileChanged { address: a }
        ));
        assert!(apply_membership_event(
            &mut set,
            &ChainNodeEvent::Unstaked {
                address: b,
                amount: U256::zero()
            }
        ));
        assert!(apply_membership_event(
            &mut set,
            &ChainNodeEvent::Removed { address: a }
        ));
        assert!(set.is_empty());
        // Re-registration after removal is membership again.
        assert!(apply_membership_event(
            &mut set,
            &ChainNodeEvent::Registered { address: a }
        ));
        assert_eq!(set.len(), 1);
    }
}
