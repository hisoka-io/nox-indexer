-- Registry-scoped chain progress and node attribution.
--
-- The indexer used to keep one global `indexer_state.last_chain_block` and an
-- unscoped `nodes` table, so pointing it at a new NoxRegistry either replayed
-- from FROM_BLOCK forever or mixed two registries' members. Progress is now
-- keyed by (chain_id, registry_address) and every node row records the
-- registry it was last seen in. Stats tables (node_reputation,
-- node_metric_offsets) stay keyed by address only, so a node that re-registers
-- on a new registry resumes its lifetime counters.
--
-- Every statement is idempotent: migrations run on every boot.

ALTER TABLE nodes ADD COLUMN IF NOT EXISTS registry_address TEXT NOT NULL DEFAULT '';
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS chain_id BIGINT NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS frozen BOOLEAN NOT NULL DEFAULT FALSE;

-- Rows written before this migration all came from the April 2026 NoxRegistry
-- on Arbitrum Sepolia, the only registry this indexer was ever deployed with.
UPDATE nodes
   SET registry_address = '0x8626af80db409bed3c19871fadf9b0ce7aa641bc',
       chain_id = 421614
 WHERE registry_address = '';

CREATE INDEX IF NOT EXISTS idx_nodes_registry ON nodes(chain_id, registry_address, status);

CREATE TABLE IF NOT EXISTS indexer_checkpoints (
    chain_id         BIGINT NOT NULL,
    registry_address TEXT   NOT NULL,
    last_block       BIGINT NOT NULL,
    updated_at_ms    BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (chain_id, registry_address)
);

-- Adopt the legacy global cursor as that registry's checkpoint. It is only a
-- starting point: a resumed member set is verified against the registry's
-- count and fingerprint, and a mismatch falls back to a full replay.
INSERT INTO indexer_checkpoints (chain_id, registry_address, last_block, updated_at_ms)
SELECT 421614, '0x8626af80db409bed3c19871fadf9b0ce7aa641bc', value, 0
  FROM indexer_state
 WHERE key = 'last_chain_block' AND value > 0
ON CONFLICT (chain_id, registry_address) DO NOTHING;

-- Adopt once. Leaving the legacy key would re-create a deliberately deleted
-- checkpoint on the next boot. Pre-cutover builds only read it for logging and
-- seed pinning, and they rewrite it, so a rollback is unaffected.
DELETE FROM indexer_state WHERE key = 'last_chain_block';
