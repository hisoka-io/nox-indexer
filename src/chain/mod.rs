mod discovery;
pub mod profile;
pub mod retry;
pub mod rpc;

pub use discovery::run_chain_sync;

use ethers::prelude::*;
use ethers::types::transaction::eip2718::TypedTransaction;
use ethers::utils::keccak256;
use futures::stream::{self, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::state::primary_layer_for_role;
use profile::{decode_relayer_profile, relayers_calldata, RelayerProfile};
use retry::{classify_rpc_error, jittered_backoff, AdaptiveChunk, RpcErrorKind};
use rpc::FailoverHttp;

abigen!(
    NoxRegistryContract,
    r#"[
        event RelayerRegistered(address indexed relayer, bytes32 sphinxKey, string url, string ingressUrl, string metadataUrl, uint256 stake, uint8 nodeRole)
        event PrivilegedRelayerRegistered(address indexed relayer, bytes32 sphinxKey, string url, string ingressUrl, string metadataUrl, uint8 nodeRole)
        event RelayerRemoved(address indexed relayer, address indexed by)
        event RelayerUpdated(address indexed relayer, string newUrl)
        event IngressUrlUpdated(address indexed relayer, string newIngressUrl)
        event MetadataUrlUpdated(address indexed relayer, string newMetadataUrl)
        event KeyRotated(address indexed relayer, bytes32 newSphinxKey)
        event RoleUpdated(address indexed relayer, uint8 newRole)
        event RelayerFrozen(address indexed relayer, address indexed by)
        event RelayerUnfrozen(address indexed relayer, address indexed by)
        event Unstaked(address indexed relayer, uint256 amount)
        function relayerCount() view returns (uint256)
        function topologyFingerprint() view returns (bytes32)
        function getNodeRole(address _relayer) view returns (uint8)
    ]"#
);

pub type RegistryProvider = Provider<FailoverHttp>;

/// Attempts per `eth_getLogs` chunk before the error is handed to the caller
/// (whose own loop keeps its progress and retries with a longer backoff).
const MAX_CHUNK_ATTEMPTS: u32 = 8;
const CHUNK_BACKOFF_BASE: Duration = Duration::from_millis(500);
const CHUNK_BACKOFF_CAP: Duration = Duration::from_secs(30);
/// Attempts for single view calls (`eth_call`, `eth_blockNumber`).
const MAX_CALL_ATTEMPTS: u32 = 5;
/// Concurrent profile reads when pinning a topology snapshot.
const PROFILE_READ_CONCURRENCY: usize = 4;
/// Chunks ending within this many blocks of the sync target are checked against
/// the head of the endpoint that served them. Some providers (the official
/// Arbitrum RPC among them) silently cut `eth_getLogs` off at their own head
/// instead of erroring, so a lagging endpoint would otherwise advance the
/// checkpoint past events it never returned.
const HEAD_CHECK_WINDOW: u64 = 100_000;

/// `eth_getLogs` range tuning.
#[derive(Debug, Clone, Copy)]
pub struct LogChunkConfig {
    pub initial: u64,
    pub min: u64,
    pub max: u64,
}

pub struct ChainConfig {
    pub provider: RegistryProvider,
    pub registry_address: Address,
    /// Lowercase 0x-prefixed registry address, the key used in Postgres.
    pub registry_hex: String,
    pub contract: NoxRegistryContract<RegistryProvider>,
    pub poll_interval: Duration,
    /// Contract deployment block: the start of a full replay.
    pub from_block: u64,
    /// Blocks behind head that are treated as final. Keeps a lagging failover
    /// endpoint from silently returning an incomplete log range.
    pub confirmations: u64,
    pub expected_chain_id: Option<u64>,
    pub endpoint_labels: Vec<String>,
    chunk: Mutex<AdaptiveChunk>,
    /// Cached `eth_chainId`; 0 until first resolved.
    chain_id: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct OnChainNode {
    pub address: String,
    pub url: String,
    pub ingress_url: String,
    pub metadata_url: String,
    pub sphinx_key: String,
    pub role: u8,
    pub frozen: bool,
}

#[derive(Debug, Clone)]
pub struct PinnedTopologyNode {
    pub address: String,
    pub sphinx_key: String,
    pub url: String,
    pub stake: String,
    pub is_privileged: bool,
    pub layer: u8,
    pub role: u8,
    pub ingress_url: String,
    pub metadata_url: String,
    pub frozen: bool,
}

#[derive(Debug, Clone)]
pub enum ChainNodeEvent {
    Registered {
        address: Address,
    },
    ProfileChanged {
        address: Address,
    },
    Removed {
        address: Address,
    },
    /// `executeUnstake` deletes the registration on both registry versions, so
    /// this is a membership removal, not just a balance change.
    Unstaked {
        address: Address,
        amount: U256,
    },
}

/// Apply one event to a replayed membership set. Returns true when membership changed.
pub fn apply_membership_event(set: &mut HashSet<Address>, event: &ChainNodeEvent) -> bool {
    match event {
        ChainNodeEvent::Registered { address } => set.insert(*address),
        ChainNodeEvent::Removed { address } | ChainNodeEvent::Unstaked { address, .. } => {
            set.remove(address)
        }
        ChainNodeEvent::ProfileChanged { .. } => false,
    }
}

/// Retry a fallible RPC operation with jittered exponential backoff.
pub async fn retry_rpc<T, F, Fut>(
    label: &str,
    shutdown: &CancellationToken,
    operation: F,
) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    retry_rpc_attempts(label, shutdown, MAX_CALL_ATTEMPTS, operation).await
}

/// [`retry_rpc`] with an explicit attempt budget.
pub async fn retry_rpc_attempts<T, F, Fut>(
    label: &str,
    shutdown: &CancellationToken,
    max_attempts: u32,
    mut operation: F,
) -> Result<T, String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let mut attempt = 0_u32;
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                attempt += 1;
                if attempt >= max_attempts {
                    return Err(format!("{label} failed after {attempt} attempts: {error}"));
                }
                let delay = jittered_backoff(attempt, CHUNK_BACKOFF_BASE, CHUNK_BACKOFF_CAP);
                tracing::warn!(
                    "{label} failed (attempt {attempt}/{max_attempts}), retrying in {delay:?}: {error}"
                );
                tokio::select! {
                    () = shutdown.cancelled() => return Err(format!("{label}: shutting down")),
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

impl ChainConfig {
    pub fn new(
        rpc_urls: &[String],
        registry_hex: &str,
        poll_secs: u64,
        from_block: u64,
        confirmations: u64,
        expected_chain_id: Option<u64>,
        chunk: LogChunkConfig,
    ) -> Result<Self, String> {
        let transport = FailoverHttp::new(rpc_urls)?;
        let endpoint_labels = transport.labels();
        let provider = Provider::new(transport);

        let registry_address = registry_hex
            .parse::<Address>()
            .map_err(|e| format!("Invalid registry address '{registry_hex}': {e}"))?;

        let contract = NoxRegistryContract::new(registry_address, Arc::new(provider.clone()));

        Ok(Self {
            provider,
            registry_address,
            registry_hex: format!("{registry_address:?}").to_lowercase(),
            contract,
            poll_interval: Duration::from_secs(poll_secs),
            from_block,
            confirmations,
            expected_chain_id,
            endpoint_labels,
            chunk: Mutex::new(AdaptiveChunk::new(chunk.initial, chunk.min, chunk.max)),
            chain_id: AtomicU64::new(0),
        })
    }

    /// `eth_chainId`, validated against `EXPECTED_CHAIN_ID` when configured.
    pub async fn chain_id(&self) -> Result<u64, String> {
        let cached = self.chain_id.load(Ordering::Relaxed);
        if cached != 0 {
            return Ok(cached);
        }
        let id = self
            .provider
            .get_chainid()
            .await
            .map_err(|e| format!("Failed to get chain id: {e}"))?
            .as_u64();
        if let Some(expected) = self.expected_chain_id {
            if expected != id {
                return Err(format!(
                    "RPC reports chain id {id} but EXPECTED_CHAIN_ID is {expected}"
                ));
            }
        }
        self.chain_id.store(id, Ordering::Relaxed);
        Ok(id)
    }

    /// Latest block minus the configured confirmations.
    pub async fn safe_head(&self) -> Result<u64, String> {
        Ok(self
            .current_block()
            .await?
            .saturating_sub(self.confirmations))
    }

    /// Fetch registry logs starting at `from`, covering as many blocks up to `to`
    /// as the adaptive chunk allows. Returns the logs and the last block covered.
    /// Retries with backoff, shrinking the range on range/rate errors.
    pub async fn fetch_logs_chunk(
        &self,
        from: u64,
        to: u64,
        shutdown: &CancellationToken,
    ) -> Result<(Vec<Log>, u64), String> {
        let mut attempt = 0_u32;
        loop {
            let span = self.chunk.lock().size();
            let end = from.saturating_add(span.saturating_sub(1)).min(to);
            let requested = end - from + 1;
            let filter = Filter::new()
                .address(self.registry_address)
                .from_block(from)
                .to_block(end);

            let error = match self.provider.get_logs(&filter).await {
                Ok(logs) => match self.check_logs_endpoint_head(end, to).await {
                    Ok(()) => {
                        self.chunk.lock().on_success();
                        return Ok((logs, end));
                    }
                    Err(error) => error,
                },
                Err(error) => error.to_string(),
            };

            match classify_rpc_error(&error) {
                RpcErrorKind::RangeTooLarge { hint } => {
                    let mut chunk = self.chunk.lock();
                    chunk.on_range_error(requested, hint);
                    let shrunk = chunk.size() < requested;
                    drop(chunk);
                    if shrunk {
                        tracing::info!(
                            "eth_getLogs range {requested} rejected, retrying with {} blocks",
                            self.chunk.lock().size()
                        );
                        continue;
                    }
                }
                RpcErrorKind::RateLimited => self.chunk.lock().on_rate_limited(),
                RpcErrorKind::Transient => {}
            }

            attempt += 1;
            if attempt >= MAX_CHUNK_ATTEMPTS {
                return Err(format!(
                    "eth_getLogs {from}..{end} failed after {attempt} attempts: {error}"
                ));
            }
            let delay = jittered_backoff(attempt, CHUNK_BACKOFF_BASE, CHUNK_BACKOFF_CAP);
            tracing::warn!(
                "eth_getLogs {from}..{end} failed (attempt {attempt}/{MAX_CHUNK_ATTEMPTS}), \
                 retrying in {delay:?} with {} blocks: {error}",
                self.chunk.lock().size()
            );
            tokio::select! {
                () = shutdown.cancelled() => return Err("eth_getLogs: shutting down".to_string()),
                () = tokio::time::sleep(delay) => {}
            }
        }
    }

    /// Confirm the endpoint that just served logs up to `end` has actually
    /// reached `end`. A lagging endpoint is demoted so the retry goes elsewhere.
    async fn check_logs_endpoint_head(&self, end: u64, to: u64) -> Result<(), String> {
        if to.saturating_sub(end) >= HEAD_CHECK_WINDOW {
            return Ok(());
        }
        let transport = self.provider.as_ref();
        let index = transport.logs_served_by();
        let head = transport.block_number_on(index).await?;
        if head >= end {
            return Ok(());
        }
        transport.demote(index);
        Err(format!(
            "{} is behind: its head {head} is below requested log end {end}; \
             discarding the possibly truncated result",
            transport.label(index)
        ))
    }

    pub fn decode_log(&self, log: &Log) -> Option<ChainNodeEvent> {
        if log.address != self.registry_address {
            return None;
        }
        macro_rules! try_decode {
            ($filter:ty, $name:expr, $map:expr) => {
                if let Ok(e) = self.contract.decode_event::<$filter>(
                    $name,
                    log.topics.clone(),
                    log.data.clone(),
                ) {
                    return Some($map(e));
                }
            };
        }

        try_decode!(
            RelayerRegisteredFilter,
            "RelayerRegistered",
            |e: RelayerRegisteredFilter| ChainNodeEvent::Registered { address: e.relayer }
        );
        try_decode!(
            PrivilegedRelayerRegisteredFilter,
            "PrivilegedRelayerRegistered",
            |e: PrivilegedRelayerRegisteredFilter| ChainNodeEvent::Registered {
                address: e.relayer
            }
        );
        try_decode!(
            RelayerRemovedFilter,
            "RelayerRemoved",
            |e: RelayerRemovedFilter| ChainNodeEvent::Removed { address: e.relayer }
        );
        try_decode!(
            RelayerUpdatedFilter,
            "RelayerUpdated",
            |e: RelayerUpdatedFilter| { ChainNodeEvent::ProfileChanged { address: e.relayer } }
        );
        try_decode!(
            IngressUrlUpdatedFilter,
            "IngressUrlUpdated",
            |e: IngressUrlUpdatedFilter| ChainNodeEvent::ProfileChanged { address: e.relayer }
        );
        try_decode!(
            MetadataUrlUpdatedFilter,
            "MetadataUrlUpdated",
            |e: MetadataUrlUpdatedFilter| ChainNodeEvent::ProfileChanged { address: e.relayer }
        );
        try_decode!(KeyRotatedFilter, "KeyRotated", |e: KeyRotatedFilter| {
            ChainNodeEvent::ProfileChanged { address: e.relayer }
        });
        try_decode!(RoleUpdatedFilter, "RoleUpdated", |e: RoleUpdatedFilter| {
            ChainNodeEvent::ProfileChanged { address: e.relayer }
        });
        try_decode!(
            RelayerFrozenFilter,
            "RelayerFrozen",
            |e: RelayerFrozenFilter| { ChainNodeEvent::ProfileChanged { address: e.relayer } }
        );
        try_decode!(
            RelayerUnfrozenFilter,
            "RelayerUnfrozen",
            |e: RelayerUnfrozenFilter| ChainNodeEvent::ProfileChanged { address: e.relayer }
        );
        try_decode!(UnstakedFilter, "Unstaked", |e: UnstakedFilter| {
            ChainNodeEvent::Unstaked {
                address: e.relayer,
                amount: e.amount,
            }
        });

        None
    }

    /// `relayers(address)` at `block` (latest when `None`), for either registry layout.
    pub async fn fetch_profile(
        &self,
        address: Address,
        block: Option<u64>,
    ) -> Result<RelayerProfile, String> {
        let tx: TypedTransaction = TransactionRequest::new()
            .to(self.registry_address)
            .data(relayers_calldata(address))
            .into();
        let block_id = block.map(|n| BlockId::Number(BlockNumber::Number(n.into())));
        let raw = self
            .provider
            .call(&tx, block_id)
            .await
            .map_err(|e| format!("relayers({address:?}) call failed: {e}"))?;
        decode_relayer_profile(&raw).map_err(|e| format!("relayers({address:?}): {e}"))
    }

    async fn fetch_role(&self, address: Address, block: Option<u64>) -> Result<u8, String> {
        let call = self.contract.get_node_role(address);
        let call = match block {
            Some(n) => call.block(BlockId::Number(BlockNumber::Number(n.into()))),
            None => call,
        };
        call.call()
            .await
            .map_err(|e| format!("getNodeRole({address:?}): {e}"))
    }

    pub async fn fetch_node_info(&self, address: Address) -> Result<Option<OnChainNode>, String> {
        let profile = self.fetch_profile(address, None).await?;
        if !profile.is_member() {
            return Ok(None);
        }
        let role = self.fetch_role(address, None).await?;

        Ok(Some(OnChainNode {
            address: format!("{address:?}"),
            url: profile.url,
            ingress_url: profile.ingress_url,
            metadata_url: profile.metadata_url,
            sphinx_key: ethers::utils::hex::encode(profile.sphinx_key),
            role,
            frozen: profile.frozen,
        }))
    }

    /// `(relayerCount, topologyFingerprint)` at `block`.
    pub async fn membership_at(&self, block: u64) -> Result<(U256, [u8; 32]), String> {
        let block_id = BlockId::Number(BlockNumber::Number(block.into()));
        let count = self
            .contract
            .relayer_count()
            .block(block_id)
            .call()
            .await
            .map_err(|error| format!("relayerCount at block {block}: {error}"))?;
        let fingerprint = self
            .contract
            .topology_fingerprint()
            .block(block_id)
            .call()
            .await
            .map_err(|error| format!("topologyFingerprint at block {block}: {error}"))?;
        Ok((count, fingerprint))
    }

    /// Check a replayed member set against the registry's count and fingerprint.
    pub async fn verify_membership(
        &self,
        members: &HashSet<Address>,
        block: u64,
    ) -> Result<Result<(), String>, String> {
        let (count, fingerprint) = self.membership_at(block).await?;
        let addresses: Vec<Address> = members.iter().copied().collect();
        Ok(validate_pinned_membership(&addresses, count, fingerprint))
    }

    pub async fn pinned_topology_members(
        &self,
        member_addresses: &[String],
        block_number: u64,
    ) -> Result<(Vec<PinnedTopologyNode>, String), String> {
        if block_number == 0 {
            return Err("seed topology requires a non-zero processed block".to_string());
        }

        let addresses: Vec<Address> = member_addresses
            .iter()
            .map(|address| {
                address.parse::<Address>().map_err(|error| {
                    format!("stored topology address {address} is invalid: {error}")
                })
            })
            .collect::<Result<_, _>>()?;
        let (count, fingerprint) = self.membership_at(block_number).await?;
        validate_pinned_membership(&addresses, count, fingerprint)?;

        let mut nodes: Vec<PinnedTopologyNode> = stream::iter(addresses)
            .map(|address| async move {
                let profile = self.fetch_profile(address, Some(block_number)).await?;
                if !profile.is_member() {
                    return Err(format!(
                        "replayed topology member {address:?} is not registered at block {block_number}"
                    ));
                }
                let role = self.fetch_role(address, Some(block_number)).await?;
                if !(1..=3).contains(&role) {
                    return Err(format!(
                        "replayed topology member {address:?} has invalid role {role} at block {block_number}"
                    ));
                }
                let address_text = format!("{address:?}").to_lowercase();
                Ok(PinnedTopologyNode {
                    sphinx_key: ethers::utils::hex::encode(profile.sphinx_key),
                    url: profile.url,
                    ingress_url: profile.ingress_url,
                    metadata_url: profile.metadata_url,
                    stake: profile.staked_amount.to_string(),
                    is_privileged: profile.staked_amount.is_zero(),
                    layer: primary_layer_for_role(role, &address_text),
                    role,
                    address: address_text,
                    frozen: profile.frozen,
                })
            })
            .buffer_unordered(PROFILE_READ_CONCURRENCY)
            .try_collect()
            .await?;
        nodes.sort_by(|left, right| left.address.cmp(&right.address));
        Ok((nodes, ethers::utils::hex::encode(fingerprint)))
    }

    pub async fn current_block(&self) -> Result<u64, String> {
        self.provider
            .get_block_number()
            .await
            .map(|n| n.as_u64())
            .map_err(|e| format!("Failed to get block number: {e}"))
    }
}

pub fn validate_pinned_membership(
    addresses: &[Address],
    expected_count: U256,
    expected_fingerprint: [u8; 32],
) -> Result<(), String> {
    let mut seen = HashSet::with_capacity(addresses.len());
    let mut fingerprint = [0_u8; 32];
    for address in addresses {
        if !seen.insert(*address) {
            return Err(format!(
                "replayed topology has duplicate address {address:?}"
            ));
        }
        let address_hash = keccak256(address.as_bytes());
        for (accumulator, byte) in fingerprint.iter_mut().zip(address_hash) {
            *accumulator ^= byte;
        }
    }
    if expected_count != U256::from(addresses.len()) {
        return Err(format!(
            "replayed topology count {} differs from registry count {expected_count}",
            addresses.len()
        ));
    }
    if fingerprint != expected_fingerprint {
        return Err("replayed topology fingerprint differs from registry".to_string());
    }
    Ok(())
}

pub fn parse_multiaddr(url: &str) -> Option<(String, u16)> {
    let parts: Vec<&str> = url.split('/').collect();
    let mut ip = None;
    let mut port = None;

    let mut iter = parts.iter();
    while let Some(&segment) = iter.next() {
        match segment {
            "ip4" | "ip6" => ip = iter.next().map(|s| s.to_string()),
            "tcp" => port = iter.next().and_then(|s| s.parse().ok()),
            _ => {}
        }
    }

    ip.zip(port)
}

pub fn derive_admin_url(multiaddr: &str) -> String {
    if let Some((ip, tcp_port)) = parse_multiaddr(multiaddr) {
        format!("http://{ip}:{}", tcp_port + 1)
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A JSON-RPC endpoint that reports `head` and serves empty log ranges.
    async fn mock_endpoint(head: u64) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut buf = vec![0_u8; 8192];
                let read = socket.read(&mut buf).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..read]);
                let result = if request.contains("eth_blockNumber") {
                    format!("\"{head:#x}\"")
                } else {
                    "[]".to_string()
                };
                let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{result}}}"#);
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        format!("http://{addr}/")
    }

    fn test_chain(urls: &[String]) -> ChainConfig {
        ChainConfig::new(
            urls,
            "0x8626af80db409bed3c19871fadf9b0ce7aa641bc",
            1,
            0,
            20,
            None,
            LogChunkConfig {
                initial: 1_000,
                min: 10,
                max: 1_000,
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn logs_from_an_endpoint_behind_the_range_end_are_refetched_elsewhere() {
        let lagging = mock_endpoint(100).await;
        let healthy = mock_endpoint(200).await;
        let chain = test_chain(&[lagging, healthy]);
        let shutdown = CancellationToken::new();

        let (logs, end) = chain.fetch_logs_chunk(90, 150, &shutdown).await.unwrap();
        assert!(logs.is_empty());
        assert_eq!(end, 150);
        assert_eq!(
            chain.provider.as_ref().logs_served_by(),
            1,
            "the result must come from the endpoint that has reached block 150"
        );
    }

    #[tokio::test]
    async fn the_head_check_only_runs_near_the_target() {
        let lagging = mock_endpoint(100).await;
        let chain = test_chain(&[lagging]);
        // Deep in a replay: the endpoint's head is irrelevant.
        assert!(chain.check_logs_endpoint_head(150, 1_000_000).await.is_ok());
        let error = chain
            .check_logs_endpoint_head(150, 150)
            .await
            .expect_err("an endpoint at block 100 cannot have served logs up to 150");
        assert!(error.contains("is behind"), "{error}");
        assert_eq!(
            retry::classify_rpc_error(&error),
            RpcErrorKind::Transient,
            "a lagging endpoint must not shrink the chunk size"
        );
        assert!(chain.check_logs_endpoint_head(100, 100).await.is_ok());
    }

    #[test]
    fn pinned_membership_rejects_a_partial_replay() {
        let addresses = [Address::from_low_u64_be(1)];
        let error = validate_pinned_membership(&addresses, U256::from(2), [0_u8; 32])
            .expect_err("a replay missing a registered address must fail closed");

        assert!(error.contains("count"));
    }

    #[test]
    fn pinned_membership_rejects_a_fingerprint_mismatch() {
        let addresses = [Address::from_low_u64_be(1)];
        let error = validate_pinned_membership(&addresses, U256::one(), [0_u8; 32])
            .expect_err("a replay with the wrong membership fingerprint must fail closed");

        assert!(error.contains("fingerprint"));
    }
}
