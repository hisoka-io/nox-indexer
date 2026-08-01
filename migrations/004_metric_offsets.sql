-- Cumulative metric offsets, banked across node restarts.
--
-- Node counters (packets_received, cover traffic, exit ops, uptime, ...) live in
-- the node process and reset to zero when its container restarts. This table
-- stores, per node, the sum of every previous incarnation's final values so the
-- indexer can present a continuous lifetime total.
--
-- `offsets`  - banked totals from all prior incarnations (JSONB, CumulativeMetrics)
-- `last_raw` - most recent reading of the current incarnation, banked on next restart
CREATE TABLE IF NOT EXISTS node_metric_offsets (
    address              TEXT PRIMARY KEY,
    last_node_start_time BIGINT NOT NULL DEFAULT 0,
    offsets              JSONB  NOT NULL DEFAULT '{}'::jsonb,
    last_raw             JSONB  NOT NULL DEFAULT '{}'::jsonb,
    updated_at_ms        BIGINT NOT NULL DEFAULT 0
);
