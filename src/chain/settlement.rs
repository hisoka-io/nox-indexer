//! Paid execution settlements and exit credit claims.
//!
//! When `ENTRY_POINT_ADDRESS` is set, the indexer follows `PaidExecutionSettled`
//! on the NoxEntryPoint and, with `REWARD_POOL_ADDRESS`, `ExitCreditClaimed` on
//! the NoxRewardPool. Events are stored per `(chain_id, tx_hash, log_index)`, so
//! a replay is idempotent. After new events (and every few minutes) the
//! per-exit totals are recomputed and the pool's balances read on chain.

use ethers::contract::{EthEvent, EthLogDecode};
use ethers::prelude::*;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::state::AppState;

use super::{retry_rpc, ChainConfig};

abigen!(
    NoxEntryPointEvents,
    r#"[
        event PaidExecutionSettled(bytes32 indexed executionId, bytes32 indexed paymentId, address indexed exit, address feeAsset, uint256 exitFee, uint256 networkFee, address actionTarget, bool actionSuccess, uint256 returnDataLength, bytes32 returnPrefixHash)
    ]"#
);

abigen!(
    NoxRewardPoolReader,
    r#"[
        event ExitCreditClaimed(address indexed exit, address indexed recipient, address indexed asset, uint256 amount)
        function claimableExit(address exit, address asset) view returns (uint256)
        function totalCollected(address asset) view returns (uint256)
        function totalDistributed(address asset) view returns (uint256)
        function networkOutstanding(address asset) view returns (uint256)
        function exitOutstanding(address asset) view returns (uint256)
    ]"#
);

/// How often new settlement logs are fetched.
const POLL_INTERVAL: Duration = Duration::from_secs(60);
/// Pool balances are re-read at least this often even without new events.
const GAUGE_REFRESH: Duration = Duration::from_secs(600);

#[derive(Debug, Clone)]
pub struct SettlementConfig {
    pub entry_point: Address,
    pub reward_pool: Option<Address>,
    /// First block scanned when there is no checkpoint for these contracts.
    pub from_block: u64,
}

impl SettlementConfig {
    /// `None` when `ENTRY_POINT_ADDRESS` is unset: settlement indexing is off.
    pub fn new(
        entry_point: Option<&str>,
        reward_pool: Option<&str>,
        from_block: u64,
    ) -> Result<Option<Self>, String> {
        let parse = |name: &str, value: &str| {
            value
                .trim()
                .parse::<Address>()
                .map_err(|e| format!("Invalid {name} '{value}': {e}"))
        };
        let entry_point = entry_point.map(str::trim).filter(|v| !v.is_empty());
        let reward_pool = reward_pool.map(str::trim).filter(|v| !v.is_empty());
        let Some(entry_point) = entry_point else {
            if reward_pool.is_some() {
                return Err("REWARD_POOL_ADDRESS requires ENTRY_POINT_ADDRESS".to_string());
            }
            return Ok(None);
        };
        Ok(Some(Self {
            entry_point: parse("ENTRY_POINT_ADDRESS", entry_point)?,
            reward_pool: reward_pool
                .map(|pool| parse("REWARD_POOL_ADDRESS", pool))
                .transpose()?,
            from_block,
        }))
    }

    /// Checkpoint key: changing either contract starts a new scan.
    pub fn source_key(&self) -> String {
        match self.reward_pool {
            Some(pool) => format!("{}|{}", hex_address(self.entry_point), hex_address(pool)),
            None => hex_address(self.entry_point),
        }
    }

    fn filter(&self) -> Filter {
        let mut addresses = vec![self.entry_point];
        let mut topics = vec![Some(PaidExecutionSettledFilter::signature())];
        if let Some(pool) = self.reward_pool {
            addresses.push(pool);
            topics.push(Some(ExitCreditClaimedFilter::signature()));
        }
        Filter::new()
            .address(addresses)
            .topic0(ValueOrArray::Array(topics))
    }
}

fn hex_address(address: Address) -> String {
    format!("{address:?}").to_lowercase()
}

fn hex_bytes32(bytes: [u8; 32]) -> String {
    format!("0x{}", ethers::utils::hex::encode(bytes))
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PaidSettlement {
    pub tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub execution_id: String,
    pub payment_id: String,
    pub exit_address: String,
    pub fee_asset: String,
    /// Token base units as a decimal string.
    pub exit_fee: String,
    pub network_fee: String,
    pub action_target: String,
    pub action_success: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExitCreditClaim {
    pub tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub exit_address: String,
    pub recipient: String,
    pub asset: String,
    pub amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementLog {
    Settled(PaidSettlement),
    Claimed(ExitCreditClaim),
}

impl SettlementLog {
    fn block_number(&self) -> u64 {
        match self {
            SettlementLog::Settled(s) => s.block_number,
            SettlementLog::Claimed(c) => c.block_number,
        }
    }

    fn set_timestamp(&mut self, timestamp: u64) {
        match self {
            SettlementLog::Settled(s) => s.block_timestamp = timestamp,
            SettlementLog::Claimed(c) => c.block_timestamp = timestamp,
        }
    }
}

/// Decode one log from the configured contracts. Logs from other addresses,
/// other events and removed (reorged) logs are ignored.
pub fn decode_settlement_log(log: &Log, config: &SettlementConfig) -> Option<SettlementLog> {
    if log.removed == Some(true) {
        return None;
    }
    let topic0 = *log.topics.first()?;
    let raw = ethers::abi::RawLog {
        topics: log.topics.clone(),
        data: log.data.to_vec(),
    };
    let tx_hash = format!("{:?}", log.transaction_hash?);
    let log_index = log.log_index?.as_u64();
    let block_number = log.block_number?.as_u64();

    if log.address == config.entry_point && topic0 == PaidExecutionSettledFilter::signature() {
        let event = <PaidExecutionSettledFilter as EthLogDecode>::decode_log(&raw).ok()?;
        return Some(SettlementLog::Settled(PaidSettlement {
            tx_hash,
            log_index,
            block_number,
            block_timestamp: 0,
            execution_id: hex_bytes32(event.execution_id),
            payment_id: hex_bytes32(event.payment_id),
            exit_address: hex_address(event.exit),
            fee_asset: hex_address(event.fee_asset),
            exit_fee: event.exit_fee.to_string(),
            network_fee: event.network_fee.to_string(),
            action_target: hex_address(event.action_target),
            action_success: event.action_success,
        }));
    }
    if Some(log.address) == config.reward_pool && topic0 == ExitCreditClaimedFilter::signature() {
        let event = <ExitCreditClaimedFilter as EthLogDecode>::decode_log(&raw).ok()?;
        return Some(SettlementLog::Claimed(ExitCreditClaim {
            tx_hash,
            log_index,
            block_number,
            block_timestamp: 0,
            exit_address: hex_address(event.exit),
            recipient: hex_address(event.recipient),
            asset: hex_address(event.asset),
            amount: event.amount.to_string(),
        }));
    }
    None
}

/// Lifetime figures for one exit and fee asset.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExitSettlementTotals {
    pub exit_address: String,
    pub fee_asset: String,
    pub executions: u64,
    /// Sum of `exitFee` over settled executions (base units, decimal string).
    pub exit_fees: String,
    pub network_fees: String,
    /// Sum of `ExitCreditClaimed` amounts.
    pub claimed: String,
    /// `claimableExit(exit, asset)` read on chain at `pool_read_block`.
    pub claimable: Option<String>,
}

/// NoxRewardPool balances for one asset, read on chain.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PoolAssetGauge {
    pub asset: String,
    pub total_collected: String,
    pub total_distributed: String,
    pub network_outstanding: String,
    pub exit_outstanding: String,
}

/// Settlement indexing progress and totals, served on `/v1/state` and
/// `/v1/settlements`.
#[derive(Debug, Clone, Serialize, Default)]
pub struct SettlementStatus {
    pub enabled: bool,
    pub entry_point: Option<String>,
    pub reward_pool: Option<String>,
    /// Last block whose settlement logs are stored.
    pub last_block: Option<u64>,
    pub last_synced_at_ms: Option<i64>,
    pub executions: u64,
    pub exits: Vec<ExitSettlementTotals>,
    pub pool: Vec<PoolAssetGauge>,
    pub pool_read_block: Option<u64>,
    #[serde(serialize_with = "crate::state::serialize_public_error")]
    pub last_error: Option<String>,
}

/// Rows from the settlement aggregate query.
pub struct SettledTotalsRow {
    pub exit_address: String,
    pub fee_asset: String,
    pub executions: i64,
    pub exit_fees: String,
    pub network_fees: String,
}

/// Rows from the claim aggregate query.
pub struct ClaimedTotalsRow {
    pub exit_address: String,
    pub asset: String,
    pub claimed: String,
}

/// Join settlement and claim aggregates per `(exit, asset)`, ordered by exit
/// then asset. A pair with claims but no settlement under this EntryPoint is
/// kept with zero executions.
pub fn merge_totals(
    settled: Vec<SettledTotalsRow>,
    claimed: Vec<ClaimedTotalsRow>,
) -> Vec<ExitSettlementTotals> {
    let mut merged: BTreeMap<(String, String), ExitSettlementTotals> = BTreeMap::new();
    for row in settled {
        merged.insert(
            (row.exit_address.clone(), row.fee_asset.clone()),
            ExitSettlementTotals {
                exit_address: row.exit_address,
                fee_asset: row.fee_asset,
                executions: u64::try_from(row.executions).unwrap_or(0),
                exit_fees: row.exit_fees,
                network_fees: row.network_fees,
                claimed: "0".to_string(),
                claimable: None,
            },
        );
    }
    for row in claimed {
        merged
            .entry((row.exit_address.clone(), row.asset.clone()))
            .or_insert_with(|| ExitSettlementTotals {
                exit_address: row.exit_address.clone(),
                fee_asset: row.asset.clone(),
                executions: 0,
                exit_fees: "0".to_string(),
                network_fees: "0".to_string(),
                claimed: "0".to_string(),
                claimable: None,
            })
            .claimed = row.claimed;
    }
    merged.into_values().collect()
}

/// Follow settlement events for the lifetime of the process.
pub async fn run_settlement_sync(
    state: AppState,
    chain: Arc<ChainConfig>,
    config: SettlementConfig,
) {
    {
        let mut status = state.settlements.write();
        status.enabled = true;
        status.entry_point = Some(hex_address(config.entry_point));
        status.reward_pool = config.reward_pool.map(hex_address);
    }
    tracing::info!(
        "Settlement indexing: EntryPoint {}, RewardPool {}, from block {}",
        hex_address(config.entry_point),
        config
            .reward_pool
            .map_or_else(|| "not set".to_string(), hex_address),
        config.from_block
    );

    let mut interval = tokio::time::interval(POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut next_block: Option<u64> = None;
    let mut last_summary: Option<Instant> = None;

    loop {
        tokio::select! {
            () = state.shutdown.cancelled() => {
                tracing::info!("settlement sync: shutting down");
                return;
            }
            _ = interval.tick() => {}
        }

        let outcome = sync_once(&state, &chain, &config, &mut next_block).await;
        let stored = match outcome {
            Ok(stored) => {
                state.settlements.write().last_error = None;
                stored
            }
            Err(error) => {
                if state.shutdown.is_cancelled() {
                    return;
                }
                tracing::warn!("Settlement sync failed, retrying next poll: {error}");
                state.settlements.write().last_error = Some(error);
                0
            }
        };

        let summary_due = last_summary.is_none_or(|at| at.elapsed() >= GAUGE_REFRESH);
        if stored > 0 || summary_due {
            match refresh_summary(&state, &chain, &config).await {
                Ok(()) => last_summary = Some(Instant::now()),
                Err(error) => {
                    tracing::warn!("Settlement summary refresh failed: {error}");
                    state.settlements.write().last_error = Some(error);
                }
            }
        }
    }
}

/// Store every settlement log up to the safe head. Returns how many were new.
async fn sync_once(
    state: &AppState,
    chain: &ChainConfig,
    config: &SettlementConfig,
    next_block: &mut Option<u64>,
) -> Result<usize, String> {
    let chain_id = chain.chain_id().await?;
    let source = config.source_key();
    let mut next = match *next_block {
        Some(next) => next,
        None => {
            let checkpoint = state
                .db
                .get_settlement_checkpoint(chain_id, &source)
                .await
                .map_err(|e| format!("load settlement checkpoint: {e}"))?;
            let start = checkpoint.map_or(config.from_block, |block| {
                block.saturating_add(1).max(config.from_block)
            });
            tracing::info!("Settlement sync: starting at block {start}");
            start
        }
    };
    *next_block = Some(next);

    let target = chain.safe_head().await?;
    let filter = config.filter();
    let mut stored = 0_usize;
    while next <= target {
        let (logs, end) = chain
            .fetch_filtered_logs_chunk(&filter, next, target, &state.shutdown)
            .await?;
        let mut decoded: Vec<SettlementLog> = logs
            .iter()
            .filter_map(|log| decode_settlement_log(log, config))
            .collect();

        if !decoded.is_empty() {
            let blocks: BTreeSet<u64> = decoded.iter().map(SettlementLog::block_number).collect();
            let mut timestamps: HashMap<u64, u64> = HashMap::with_capacity(blocks.len());
            for block in blocks {
                let timestamp = retry_rpc("block timestamp", &state.shutdown, || {
                    chain.block_timestamp(block)
                })
                .await?;
                timestamps.insert(block, timestamp);
            }
            for entry in &mut decoded {
                entry.set_timestamp(
                    timestamps
                        .get(&entry.block_number())
                        .copied()
                        .unwrap_or_default(),
                );
            }
            for entry in &decoded {
                let inserted = match entry {
                    SettlementLog::Settled(settlement) => state
                        .db
                        .insert_paid_settlement(chain_id, config.entry_point, settlement)
                        .await
                        .map_err(|e| format!("persist settlement {}: {e}", settlement.tx_hash))?,
                    SettlementLog::Claimed(claim) => match config.reward_pool {
                        Some(pool) => state
                            .db
                            .insert_exit_credit_claim(chain_id, pool, claim)
                            .await
                            .map_err(|e| format!("persist exit claim {}: {e}", claim.tx_hash))?,
                        None => false,
                    },
                };
                if inserted {
                    stored += 1;
                    match entry {
                        SettlementLog::Settled(s) => tracing::info!(
                            "Paid execution settled: exit {} fee {} (tx {}, block {})",
                            s.exit_address,
                            s.exit_fee,
                            s.tx_hash,
                            s.block_number
                        ),
                        SettlementLog::Claimed(c) => tracing::info!(
                            "Exit credit claimed: exit {} amount {} (tx {}, block {})",
                            c.exit_address,
                            c.amount,
                            c.tx_hash,
                            c.block_number
                        ),
                    }
                }
            }
        }

        state
            .db
            .set_settlement_checkpoint(chain_id, &source, end)
            .await
            .map_err(|e| format!("persist settlement checkpoint {end}: {e}"))?;
        next = end + 1;
        *next_block = Some(next);
        state.settlements.write().last_block = Some(end);
    }

    state.settlements.write().last_synced_at_ms = Some(chrono::Utc::now().timestamp_millis());
    Ok(stored)
}

/// Recompute per-exit totals from Postgres and read the pool's balances at the
/// last stored block.
async fn refresh_summary(
    state: &AppState,
    chain: &ChainConfig,
    config: &SettlementConfig,
) -> Result<(), String> {
    let chain_id = chain.chain_id().await?;
    let settled = state
        .db
        .paid_settlement_totals(chain_id, config.entry_point)
        .await
        .map_err(|e| format!("load settlement totals: {e}"))?;
    let claimed = match config.reward_pool {
        Some(pool) => state
            .db
            .exit_credit_claim_totals(chain_id, pool)
            .await
            .map_err(|e| format!("load settlement totals: {e}"))?,
        None => Vec::new(),
    };
    let mut exits = merge_totals(settled, claimed);
    let executions = exits.iter().map(|exit| exit.executions).sum();

    let read_block = state.settlements.read().last_block;
    let mut pool_gauges = Vec::new();
    let mut pool_read_block = None;
    if let (Some(pool), Some(block)) = (config.reward_pool, read_block) {
        match read_pool_gauges(state, chain, pool, block, &mut exits).await {
            Ok(gauges) => {
                pool_gauges = gauges;
                pool_read_block = Some(block);
            }
            Err(error) => tracing::warn!("Reward pool read at block {block} failed: {error}"),
        }
    }

    let mut status = state.settlements.write();
    status.executions = executions;
    status.exits = exits;
    // On a failed read the previous balances stay, dated by their older
    // pool_read_block, and per-exit claimable is left unset.
    if pool_read_block.is_some() {
        status.pool = pool_gauges;
        status.pool_read_block = pool_read_block;
    }
    Ok(())
}

/// One `uint256` view call at a pinned block, retried with backoff.
async fn read_view(
    label: &str,
    shutdown: &tokio_util::sync::CancellationToken,
    call: ethers::contract::ContractCall<super::RegistryProvider, U256>,
    at: BlockId,
) -> Result<String, String> {
    retry_rpc(label, shutdown, || {
        let call = call.clone().block(at);
        async move { call.call().await.map_err(|e| e.to_string()) }
    })
    .await
    .map(|value| value.to_string())
}

async fn read_pool_gauges(
    state: &AppState,
    chain: &ChainConfig,
    pool: Address,
    block: u64,
    exits: &mut [ExitSettlementTotals],
) -> Result<Vec<PoolAssetGauge>, String> {
    let reader = NoxRewardPoolReader::new(pool, Arc::new(chain.provider.clone()));
    let at = BlockId::Number(BlockNumber::Number(block.into()));
    let shutdown = &state.shutdown;

    for exit in exits.iter_mut() {
        let (Ok(exit_address), Ok(asset)) = (
            exit.exit_address.parse::<Address>(),
            exit.fee_asset.parse::<Address>(),
        ) else {
            continue;
        };
        exit.claimable = Some(
            read_view(
                "claimableExit",
                shutdown,
                reader.claimable_exit(exit_address, asset),
                at,
            )
            .await?,
        );
    }

    let assets: BTreeSet<String> = exits.iter().map(|exit| exit.fee_asset.clone()).collect();
    let mut gauges = Vec::with_capacity(assets.len());
    for asset_text in assets {
        let Ok(asset) = asset_text.parse::<Address>() else {
            continue;
        };
        gauges.push(PoolAssetGauge {
            total_collected: read_view(
                "totalCollected",
                shutdown,
                reader.total_collected(asset),
                at,
            )
            .await?,
            total_distributed: read_view(
                "totalDistributed",
                shutdown,
                reader.total_distributed(asset),
                at,
            )
            .await?,
            network_outstanding: read_view(
                "networkOutstanding",
                shutdown,
                reader.network_outstanding(asset),
                at,
            )
            .await?,
            exit_outstanding: read_view(
                "exitOutstanding",
                shutdown,
                reader.exit_outstanding(asset),
                at,
            )
            .await?,
            asset: asset_text,
        });
    }
    Ok(gauges)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENTRY_POINT: &str = "0xad911ca217c6dc779fce6a6538bdda3071c38e7e";
    const REWARD_POOL: &str = "0xa487baa4f2c3faa01c70066ee88b6f7fd6f1361d";

    fn config() -> SettlementConfig {
        SettlementConfig::new(Some(ENTRY_POINT), Some(REWARD_POOL), 312_414_608)
            .unwrap()
            .unwrap()
    }

    fn h256(hex: &str) -> H256 {
        hex.parse().unwrap()
    }

    /// The first live settlement on Arbitrum Sepolia (block 312421630).
    fn live_settlement_log() -> Log {
        Log {
            address: ENTRY_POINT.parse().unwrap(),
            topics: vec![
                h256("0x08999181408049964f922b10eccdf5d39b08dcb8920aac22cd2fc19282d63012"),
                h256("0xa412c383d17399f82b23f035e4c32e5499e2745f064fd46c01801352d6adf2bf"),
                h256("0x28fe69284e8b20a101fb71984bee0ef8296126e224e6a70a22caee05f355f7eb"),
                h256("0x0000000000000000000000001efa385556a6e8643df0033048ba1dd67ec43f93"),
            ],
            data: "0x0000000000000000000000000f69cf1c9f4ff72471701036dd789c934458e630\
                   0000000000000000000000000000000000000000000000001dbc661d961bbb30\
                   000000000000000000000000000000000000000000000000017c9eb4ade7c95c\
                   0000000000000000000000006dabb5682a62b3827ee7e0e02f1dace896ad457a\
                   0000000000000000000000000000000000000000000000000000000000000001\
                   0000000000000000000000000000000000000000000000000000000000000000\
                   c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
                .parse()
                .unwrap(),
            block_number: Some(312_421_630_u64.into()),
            transaction_hash: Some(h256(
                "0x35c3a974a660991cec78a68125651bf655fb3dbc920d146fd58353ad532315e7",
            )),
            log_index: Some(17_u64.into()),
            removed: Some(false),
            ..Log::default()
        }
    }

    #[test]
    fn event_signatures_match_the_deployed_contracts() {
        assert_eq!(
            PaidExecutionSettledFilter::signature(),
            h256("0x08999181408049964f922b10eccdf5d39b08dcb8920aac22cd2fc19282d63012")
        );
        assert_eq!(
            ExitCreditClaimedFilter::signature(),
            h256("0x92e0ae99e3f3d04ceee88f4f453b6bffbdfc7625523b73eea93710adab80d4ca")
        );
    }

    #[test]
    fn a_live_settlement_log_decodes() {
        let Some(SettlementLog::Settled(settled)) =
            decode_settlement_log(&live_settlement_log(), &config())
        else {
            panic!("the live settlement must decode");
        };
        assert_eq!(
            settled.execution_id,
            "0xa412c383d17399f82b23f035e4c32e5499e2745f064fd46c01801352d6adf2bf"
        );
        assert_eq!(
            settled.exit_address,
            "0x1efa385556a6e8643df0033048ba1dd67ec43f93"
        );
        assert_eq!(
            settled.fee_asset,
            "0x0f69cf1c9f4ff72471701036dd789c934458e630"
        );
        assert_eq!(settled.exit_fee, "2142699799979998000");
        assert_eq!(settled.network_fee, "107134989998999900");
        assert_eq!(
            settled.action_target,
            "0x6dabb5682a62b3827ee7e0e02f1dace896ad457a"
        );
        assert!(settled.action_success);
        assert_eq!(settled.block_number, 312_421_630);
        assert_eq!(settled.log_index, 17);
        assert_eq!(
            settled.tx_hash,
            "0x35c3a974a660991cec78a68125651bf655fb3dbc920d146fd58353ad532315e7"
        );
    }

    #[test]
    fn logs_from_other_contracts_or_reorged_blocks_are_ignored() {
        let mut other = live_settlement_log();
        other.address = REWARD_POOL.parse().unwrap();
        assert_eq!(decode_settlement_log(&other, &config()), None);

        let mut removed = live_settlement_log();
        removed.removed = Some(true);
        assert_eq!(decode_settlement_log(&removed, &config()), None);
    }

    #[test]
    fn exit_credit_claims_decode() {
        let exit: Address = "0x03a42846000000000000000000000000000000aa"
            .parse()
            .unwrap();
        let recipient: Address = "0x00000000000000000000000000000000000000bb"
            .parse()
            .unwrap();
        let asset: Address = "0x0f69cf1c9f4ff72471701036dd789c934458e630"
            .parse()
            .unwrap();
        let log = Log {
            address: REWARD_POOL.parse().unwrap(),
            topics: vec![
                ExitCreditClaimedFilter::signature(),
                H256::from(exit),
                H256::from(recipient),
                H256::from(asset),
            ],
            data: ethers::abi::encode(&[ethers::abi::Token::Uint(U256::from(5_u64))]).into(),
            block_number: Some(10_u64.into()),
            transaction_hash: Some(H256::repeat_byte(1)),
            log_index: Some(0_u64.into()),
            ..Log::default()
        };
        let Some(SettlementLog::Claimed(claim)) = decode_settlement_log(&log, &config()) else {
            panic!("claim must decode");
        };
        assert_eq!(claim.exit_address, hex_address(exit));
        assert_eq!(claim.recipient, hex_address(recipient));
        assert_eq!(claim.amount, "5");

        let without_pool = SettlementConfig::new(Some(ENTRY_POINT), None, 1)
            .unwrap()
            .unwrap();
        assert_eq!(decode_settlement_log(&log, &without_pool), None);
    }

    #[test]
    fn settlement_indexing_is_off_without_an_entry_point() {
        assert!(SettlementConfig::new(None, None, 1).unwrap().is_none());
        assert!(SettlementConfig::new(Some(" "), None, 1).unwrap().is_none());
        assert!(SettlementConfig::new(None, Some(REWARD_POOL), 1).is_err());
        assert!(SettlementConfig::new(Some("0xnope"), None, 1).is_err());
        let config = config();
        assert_eq!(config.source_key(), format!("{ENTRY_POINT}|{REWARD_POOL}"));
    }

    #[test]
    fn totals_join_settlements_and_claims_per_exit_and_asset() {
        let merged = merge_totals(
            vec![SettledTotalsRow {
                exit_address: "0xb".to_string(),
                fee_asset: "0xsoka".to_string(),
                executions: 2,
                exit_fees: "40".to_string(),
                network_fees: "2".to_string(),
            }],
            vec![
                ClaimedTotalsRow {
                    exit_address: "0xb".to_string(),
                    asset: "0xsoka".to_string(),
                    claimed: "15".to_string(),
                },
                ClaimedTotalsRow {
                    exit_address: "0xa".to_string(),
                    asset: "0xsoka".to_string(),
                    claimed: "3".to_string(),
                },
            ],
        );
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].exit_address, "0xa");
        assert_eq!(merged[0].executions, 0);
        assert_eq!(merged[0].claimed, "3");
        assert_eq!(merged[1].executions, 2);
        assert_eq!(merged[1].exit_fees, "40");
        assert_eq!(merged[1].claimed, "15");
    }
}
