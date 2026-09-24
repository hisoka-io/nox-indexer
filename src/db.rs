use std::collections::HashMap;

use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::node::offsets::{CumulativeMetrics, NodeOffset};
use crate::state::{NodeState, NodeStatus};

const EMA_ALPHA: f64 = 0.01;

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct NodeReputation {
    pub address: String,
    pub total_checks: i32,
    pub passed_checks: i32,
    pub streak: i32,
    pub score: f64,
    pub status: String,
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await?;

        Self::run_migrations(&pool).await?;
        Ok(Self { pool })
    }

    async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
        let migration_files = [
            include_str!("../migrations/001_create_nodes.sql"),
            include_str!("../migrations/002_add_geo_columns.sql"),
            include_str!("../migrations/003_add_ingress_metadata_url.sql"),
            include_str!("../migrations/004_metric_offsets.sql"),
            include_str!("../migrations/005_registry_checkpoints.sql"),
        ];

        for file in migration_files {
            let stmts: Vec<&str> = file
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();
            for stmt in stmts {
                sqlx::query(stmt).execute(pool).await?;
            }
        }

        Ok(())
    }

    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Registered nodes last seen in `registry_address`, served until the first
    /// chain sync of this process completes.
    pub async fn load_registry_nodes(
        &self,
        registry_address: &str,
    ) -> Result<HashMap<String, NodeState>, sqlx::Error> {
        let rows = sqlx::query_as::<_, NodeRow>(
            // Every NodeRow field must appear here: a missing column makes FromRow
            // fail to decode, which previously surfaced as a silent "0 nodes from
            // DB" and forced a full chain replay on every boot.
            "SELECT address, id, admin_port, ingress_port, p2p_addr,
                    sphinx_key, admin_url, ingress_url, metadata_url,
                    status, role, layer, latitude, longitude,
                    frozen, registry_address, chain_id
             FROM nodes
             WHERE status != 'deregistered' AND registry_address = $1",
        )
        .bind(registry_address.to_lowercase())
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let node = NodeState::from(row);
                (node.address.clone(), node)
            })
            .collect())
    }

    pub async fn upsert_node(&self, node: &NodeState) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO nodes (address, id, admin_port, ingress_port, p2p_addr,
                               sphinx_key, admin_url, ingress_url, metadata_url,
                               status, role, layer, latitude, longitude,
                               frozen, registry_address, chain_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                     $15, $16, $17)
             ON CONFLICT (address) DO UPDATE SET
                id = EXCLUDED.id,
                admin_port = EXCLUDED.admin_port,
                ingress_port = EXCLUDED.ingress_port,
                p2p_addr = EXCLUDED.p2p_addr,
                sphinx_key = EXCLUDED.sphinx_key,
                admin_url = EXCLUDED.admin_url,
                ingress_url = EXCLUDED.ingress_url,
                metadata_url = EXCLUDED.metadata_url,
                status = EXCLUDED.status,
                role = EXCLUDED.role,
                layer = EXCLUDED.layer,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                frozen = EXCLUDED.frozen,
                registry_address = EXCLUDED.registry_address,
                chain_id = EXCLUDED.chain_id",
        )
        .bind(&node.address)
        .bind(&node.id)
        .bind(node.admin_port as i32)
        .bind(node.ingress_port as i32)
        .bind(&node.p2p_addr)
        .bind(&node.sphinx_key)
        .bind(&node.admin_url)
        .bind(&node.ingress_url)
        .bind(&node.metadata_url)
        .bind(node.status.as_str())
        .bind(node.role as i32)
        .bind(node.layer as i32)
        .bind(node.latitude)
        .bind(node.longitude)
        .bind(node.frozen)
        .bind(&node.registry_address)
        .bind(i64::try_from(node.chain_id).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_reputation(
        &self,
        address: &str,
        reachable: bool,
    ) -> Result<(), sqlx::Error> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let passed: i32 = i32::from(reachable);
        let sample: f64 = if reachable { 100.0 } else { 0.0 };

        sqlx::query(
            "INSERT INTO node_reputation (address, total_checks, passed_checks, last_check_ms, streak, score)
             VALUES ($1, 1, $2, $3, $2, $6)
             ON CONFLICT (address) DO UPDATE SET
                total_checks  = node_reputation.total_checks + 1,
                passed_checks = node_reputation.passed_checks + $2,
                last_check_ms = $3,
                streak        = CASE WHEN $4 THEN node_reputation.streak + 1 ELSE 0 END,
                score         = $5 * $6 + (1.0 - $5) * node_reputation.score",
        )
        .bind(address)
        .bind(passed)
        .bind(now_ms)
        .bind(reachable)
        .bind(EMA_ALPHA)
        .bind(sample)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Decay scores of registered nodes that stopped being checked. Deregistered
    /// nodes keep their final score so a node that re-registers resumes it.
    pub async fn decay_stale_scores(&self, stale_hours: i64) -> Result<u64, sqlx::Error> {
        let cutoff_ms =
            (chrono::Utc::now() - chrono::Duration::hours(stale_hours)).timestamp_millis();
        let result = sqlx::query(
            "UPDATE node_reputation r SET score = r.score * 0.95
             FROM nodes n
             WHERE n.address = r.address AND n.status != 'deregistered'
               AND r.last_check_ms < $1 AND r.last_check_ms > 0 AND r.score > 1.0",
        )
        .bind(cutoff_ms)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn get_reputation_ranking(&self) -> Result<Vec<NodeReputation>, sqlx::Error> {
        sqlx::query_as::<_, NodeReputation>(
            "SELECT n.address,
                    COALESCE(r.total_checks, 0) AS total_checks,
                    COALESCE(r.passed_checks, 0) AS passed_checks,
                    COALESCE(r.streak, 0) AS streak,
                    COALESCE(r.score::float8, 100.0) AS score,
                    n.status
             FROM nodes n
             LEFT JOIN node_reputation r ON n.address = r.address
             WHERE n.status != 'deregistered'
             ORDER BY score DESC, n.address",
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn update_node_status(
        &self,
        address: &str,
        status: &NodeStatus,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE nodes SET status = $1 WHERE address = $2")
            .bind(status.as_str())
            .bind(address)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn load_liveness_observed_at(&self) -> Result<HashMap<String, u64>, sqlx::Error> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT address, last_check_ms FROM node_reputation")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows
            .into_iter()
            .map(|(address, observed_at_ms)| {
                let observed_at_unix = u64::try_from(observed_at_ms)
                    .unwrap_or(0)
                    .saturating_div(1_000);
                (address, observed_at_unix)
            })
            .collect())
    }

    pub async fn load_all_uptime_targets(&self) -> Result<Vec<(String, String)>, sqlx::Error> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT address, admin_url FROM nodes WHERE admin_url != '' AND status != 'deregistered'",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Load banked lifetime metric offsets for every known node.
    ///
    /// JSONB columns are read as text and decoded here, avoiding a dependency on
    /// sqlx's `json` feature. A row whose payload fails to decode is skipped with
    /// a warning rather than aborting startup, so one corrupt row cannot take the
    /// indexer down.
    pub async fn load_metric_offsets(&self) -> Result<HashMap<String, NodeOffset>, sqlx::Error> {
        let rows: Vec<(String, i64, String, String)> = sqlx::query_as(
            "SELECT address, last_node_start_time, offsets::text, last_raw::text
             FROM node_metric_offsets",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = HashMap::with_capacity(rows.len());
        for (address, last_node_start_time, offsets, last_raw) in rows {
            let banked = match serde_json::from_str::<CumulativeMetrics>(&offsets) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("Skipping corrupt metric offsets for {address}: {e}");
                    continue;
                }
            };
            let last_raw = serde_json::from_str::<CumulativeMetrics>(&last_raw).unwrap_or_default();
            out.insert(
                address,
                NodeOffset {
                    last_node_start_time,
                    banked,
                    last_raw,
                    dirty: false,
                },
            );
        }
        Ok(out)
    }

    /// Persist one node's banked offsets. Called on restart detection and on the
    /// periodic flush, not on every scrape.
    pub async fn save_metric_offset(
        &self,
        address: &str,
        offset: &NodeOffset,
        now_ms: i64,
    ) -> Result<(), sqlx::Error> {
        let banked = serde_json::to_string(&offset.banked).unwrap_or_else(|_| "{}".to_string());
        let last_raw = serde_json::to_string(&offset.last_raw).unwrap_or_else(|_| "{}".to_string());

        sqlx::query(
            "INSERT INTO node_metric_offsets
                 (address, last_node_start_time, offsets, last_raw, updated_at_ms)
             VALUES ($1, $2, $3::jsonb, $4::jsonb, $5)
             ON CONFLICT (address) DO UPDATE SET
                 last_node_start_time = EXCLUDED.last_node_start_time,
                 offsets              = EXCLUDED.offsets,
                 last_raw             = EXCLUDED.last_raw,
                 updated_at_ms        = EXCLUDED.updated_at_ms",
        )
        .bind(address)
        .bind(offset.last_node_start_time)
        .bind(&banked)
        .bind(&last_raw)
        .bind(now_ms)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn deregister_node(&self, address: &str) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE nodes SET status = 'deregistered' WHERE address = $1")
            .bind(address)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Mark every registered row outside `keep` as deregistered, whatever
    /// registry it came from. Returns the addresses that changed.
    pub async fn deregister_nodes_except(
        &self,
        keep: &[String],
    ) -> Result<Vec<String>, sqlx::Error> {
        let keep: Vec<String> = keep.iter().map(|a| a.to_lowercase()).collect();
        let rows: Vec<(String,)> = sqlx::query_as(
            "UPDATE nodes SET status = 'deregistered'
             WHERE status != 'deregistered' AND NOT (lower(address) = ANY($1))
             RETURNING address",
        )
        .bind(&keep)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(address,)| address).collect())
    }

    /// Addresses currently registered in `(chain_id, registry_address)`.
    pub async fn load_registry_member_addresses(
        &self,
        chain_id: u64,
        registry_address: &str,
    ) -> Result<Vec<String>, sqlx::Error> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT address FROM nodes
             WHERE status != 'deregistered' AND chain_id = $1 AND registry_address = $2",
        )
        .bind(i64::try_from(chain_id).unwrap_or(i64::MAX))
        .bind(registry_address.to_lowercase())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(address,)| address).collect())
    }

    /// Last block fully applied for `(chain_id, registry_address)`.
    pub async fn get_checkpoint(
        &self,
        chain_id: u64,
        registry_address: &str,
    ) -> Result<Option<u64>, sqlx::Error> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT last_block FROM indexer_checkpoints
             WHERE chain_id = $1 AND registry_address = $2",
        )
        .bind(i64::try_from(chain_id).unwrap_or(i64::MAX))
        .bind(registry_address.to_lowercase())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(block,)| u64::try_from(block).ok()))
    }

    pub async fn set_checkpoint(
        &self,
        chain_id: u64,
        registry_address: &str,
        block: u64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO indexer_checkpoints (chain_id, registry_address, last_block, updated_at_ms)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (chain_id, registry_address) DO UPDATE SET
                 last_block = EXCLUDED.last_block,
                 updated_at_ms = EXCLUDED.updated_at_ms",
        )
        .bind(i64::try_from(chain_id).unwrap_or(i64::MAX))
        .bind(registry_address.to_lowercase())
        .bind(i64::try_from(block).unwrap_or(i64::MAX))
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Persist the network's genesis estimate once: the earliest first uptime
    /// check implied by `last_check_ms - total_checks * interval`, or now when
    /// there is no history. Returns the stored value.
    pub async fn ensure_network_genesis(
        &self,
        check_interval_ms: i64,
    ) -> Result<Option<i64>, sqlx::Error> {
        sqlx::query(
            "INSERT INTO indexer_state (key, value)
             SELECT 'network_genesis_ms', genesis FROM (
                 SELECT MIN(last_check_ms - total_checks::bigint * $1) AS genesis
                 FROM node_reputation
                 WHERE total_checks > 0 AND last_check_ms > 0
             ) evidence
             WHERE genesis IS NOT NULL
             ON CONFLICT (key) DO NOTHING",
        )
        .bind(check_interval_ms)
        .execute(&self.pool)
        .await?;
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT value FROM indexer_state WHERE key = 'network_genesis_ms'")
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(value,)| value))
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct NodeRow {
    pub address: String,
    pub id: String,
    pub admin_port: i32,
    pub ingress_port: i32,
    pub p2p_addr: String,
    pub sphinx_key: String,
    pub admin_url: String,
    pub ingress_url: String,
    pub metadata_url: String,
    pub status: String,
    pub role: i16,
    pub layer: i16,
    pub latitude: f64,
    pub longitude: f64,
    pub frozen: bool,
    pub registry_address: String,
    pub chain_id: i64,
}
