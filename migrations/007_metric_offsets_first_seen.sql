-- When the indexer first saw each node (Unix ms). Lifetime uptime is capped at
-- the wall time since then. Zero means unknown: rows from before this column
-- are backfilled at startup with network_genesis_ms.
ALTER TABLE node_metric_offsets ADD COLUMN IF NOT EXISTS first_seen_ms BIGINT NOT NULL DEFAULT 0
