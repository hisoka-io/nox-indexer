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
        ];

        for file in migration_files {
            let stmts: Vec<&str> = file.split(';').map(str::trim).filter(|s| !s.is_empty()).collect();
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

    pub async fn load_all_nodes(&self) -> Result<HashMap<String, NodeState>, sqlx::Error> {
        let rows = sqlx::query_as::<_, NodeRow>(
            // Every NodeRow field must appear here: a missing column makes FromRow
            // fail to decode, which previously surfaced as a silent "0 nodes from
            // DB" and forced a full chain replay on every boot.
            "SELECT address, id, admin_port, ingress_port, p2p_addr,
                    sphinx_key, admin_url, ingress_url, metadata_url,
                    status, role, layer, latitude, longitude
             FROM nodes
             WHERE status != 'deregistered'",
        )
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
                               status, role, layer, latitude, longitude)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
                longitude = EXCLUDED.longitude",
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

    pub async fn decay_stale_scores(&self, stale_hours: i64) -> Result<u64, sqlx::Error> {
        let cutoff_ms =
            (chrono::Utc::now() - chrono::Duration::hours(stale_hours)).timestamp_millis();
        let result = sqlx::query(
            "UPDATE node_reputation SET score = score * 0.95
             WHERE last_check_ms < $1 AND last_check_ms > 0 AND score > 1.0",
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
             ORDER BY score DESC",
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

    pub async fn get_last_chain_block(&self) -> Result<Option<u64>, sqlx::Error> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT value FROM indexer_state WHERE key = 'last_chain_block'")
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(v,)| v as u64))
    }

    pub async fn set_last_chain_block(&self, block: u64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO indexer_state (key, value) VALUES ('last_chain_block', $1)
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(block as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
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
}
