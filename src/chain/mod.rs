mod discovery;

pub use discovery::{chain_event_loop, initial_chain_sync};

use ethers::prelude::*;
use ethers::utils::keccak256;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::state::primary_layer_for_role;

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
        function relayers(address) view returns (bytes32 sphinxKey, string url, string ingressUrl, string metadataUrl, uint256 stakedAmount, uint256 unstakeRequestTime, bool isRegistered, uint8 status, bool frozen)
        function getNodeRole(address _relayer) view returns (uint8)
    ]"#
);

pub struct ChainConfig {
    pub provider: Provider<Http>,
    pub registry_address: Address,
    pub contract: NoxRegistryContract<Provider<Http>>,
    pub poll_interval: Duration,
    /// Contract deployment block -- always used for initial event replay.
    pub from_block: u64,
}

#[derive(Debug, Clone)]
pub struct OnChainNode {
    pub address: String,
    pub url: String,
    pub ingress_url: String,
    pub metadata_url: String,
    pub sphinx_key: String,
    pub role: u8,
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
}

#[derive(Debug, Clone)]
pub enum ChainNodeEvent {
    Registered { address: Address },
    ProfileChanged { address: Address },
    Removed { address: Address },
    Unstaked { address: Address, amount: U256 },
}

impl ChainConfig {
    pub fn new(
        rpc_url: &str,
        registry_hex: &str,
        poll_secs: u64,
        from_block: u64,
    ) -> Result<Self, String> {
        let provider = Provider::<Http>::try_from(rpc_url)
            .map_err(|e| format!("Invalid RPC URL '{rpc_url}': {e}"))?;

        let registry_address = registry_hex
            .parse::<Address>()
            .map_err(|e| format!("Invalid registry address '{registry_hex}': {e}"))?;

        let contract = NoxRegistryContract::new(registry_address, Arc::new(provider.clone()));

        Ok(Self {
            provider,
            registry_address,
            contract,
            poll_interval: Duration::from_secs(poll_secs),
            from_block,
        })
    }

    pub async fn scan_events(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<ChainNodeEvent>, String> {
        const CHUNK_SIZE: u64 = 10_000;
        let mut events = Vec::new();
        let mut start = from_block;

        while start <= to_block {
            let end = (start + CHUNK_SIZE - 1).min(to_block);

            let filter = Filter::new()
                .address(self.registry_address)
                .from_block(start)
                .to_block(end);

            let logs = self
                .provider
                .get_logs(&filter)
                .await
                .map_err(|e| format!("Failed to fetch logs {start}..{end}: {e}"))?;

            for log in logs {
                if let Some(event) = self.decode_log(&log) {
                    events.push(event);
                }
            }

            start = end + 1;
        }

        Ok(events)
    }

    pub fn decode_log(&self, log: &Log) -> Option<ChainNodeEvent> {
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

    pub async fn fetch_node_info(&self, address: Address) -> Result<Option<OnChainNode>, String> {
        let info = self
            .contract
            .relayers(address)
            .call()
            .await
            .map_err(|e| format!("Failed to call relayers({address:?}): {e}"))?;

        if !info.6 {
            return Ok(None);
        }

        let role = self
            .contract
            .get_node_role(address)
            .call()
            .await
            .unwrap_or(1);

        let sphinx_key = ethers::utils::hex::encode(info.0);

        Ok(Some(OnChainNode {
            address: format!("{address:?}"),
            url: info.1,
            ingress_url: info.2,
            metadata_url: info.3,
            sphinx_key,
            role,
        }))
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
        let block = BlockId::Number(BlockNumber::Number(block_number.into()));
        let count = self
            .contract
            .relayer_count()
            .block(block)
            .call()
            .await
            .map_err(|error| format!("relayerCount at block {block_number}: {error}"))?;
        let fingerprint = self
            .contract
            .topology_fingerprint()
            .block(block)
            .call()
            .await
            .map_err(|error| format!("topologyFingerprint at block {block_number}: {error}"))?;
        validate_pinned_membership(&addresses, count, fingerprint)?;

        let mut nodes = Vec::with_capacity(addresses.len());
        for address in addresses {
            let profile = self
                .contract
                .relayers(address)
                .block(block)
                .call()
                .await
                .map_err(|error| {
                    format!("relayer profile {address:?} at block {block_number}: {error}")
                })?;
            if !profile.6 {
                return Err(format!(
                    "replayed topology member {address:?} is not registered at block {block_number}"
                ));
            }
            let role = self
                .contract
                .get_node_role(address)
                .block(block)
                .call()
                .await
                .map_err(|error| {
                    format!("node role {address:?} at block {block_number}: {error}")
                })?;
            if !(1..=3).contains(&role) {
                return Err(format!(
                    "replayed topology member {address:?} has invalid role {role} at block {block_number}"
                ));
            }
            let address_text = format!("{address:?}").to_lowercase();
            nodes.push(PinnedTopologyNode {
                sphinx_key: ethers::utils::hex::encode(profile.0),
                url: profile.1,
                ingress_url: profile.2,
                metadata_url: profile.3,
                stake: profile.4.to_string(),
                is_privileged: profile.4.is_zero(),
                layer: primary_layer_for_role(role, &address_text),
                role,
                address: address_text,
            });
        }
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

    pub async fn replay_to_current_set(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<HashSet<Address>, String> {
        let events = self.scan_events(from_block, to_block).await?;
        let mut registered = HashSet::new();

        for event in events {
            match event {
                ChainNodeEvent::Registered { address } => {
                    registered.insert(address);
                }
                ChainNodeEvent::Removed { address } => {
                    registered.remove(&address);
                }
                ChainNodeEvent::ProfileChanged { .. } | ChainNodeEvent::Unstaked { .. } => {}
            }
        }

        Ok(registered)
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
