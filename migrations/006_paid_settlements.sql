-- Paid execution settlements (NoxEntryPoint PaidExecutionSettled) and exit
-- credit claims (NoxRewardPool ExitCreditClaimed), indexed only when
-- ENTRY_POINT_ADDRESS is configured. Rows are keyed by the log, so replaying
-- a block range is idempotent. Token amounts are base units.
--
-- Every statement is idempotent: migrations run on every boot.

CREATE TABLE IF NOT EXISTS paid_settlements (
    chain_id        BIGINT        NOT NULL,
    tx_hash         TEXT          NOT NULL,
    log_index       BIGINT        NOT NULL,
    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    entry_point     TEXT          NOT NULL,
    execution_id    TEXT          NOT NULL,
    payment_id      TEXT          NOT NULL,
    exit_address    TEXT          NOT NULL,
    fee_asset       TEXT          NOT NULL,
    exit_fee        NUMERIC(78,0) NOT NULL,
    network_fee     NUMERIC(78,0) NOT NULL,
    action_target   TEXT          NOT NULL,
    action_success  BOOLEAN       NOT NULL,
    PRIMARY KEY (chain_id, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_paid_settlements_entry_point
    ON paid_settlements(chain_id, entry_point, block_number);

CREATE TABLE IF NOT EXISTS exit_credit_claims (
    chain_id        BIGINT        NOT NULL,
    tx_hash         TEXT          NOT NULL,
    log_index       BIGINT        NOT NULL,
    block_number    BIGINT        NOT NULL,
    block_timestamp BIGINT        NOT NULL,
    reward_pool     TEXT          NOT NULL,
    exit_address    TEXT          NOT NULL,
    recipient       TEXT          NOT NULL,
    asset           TEXT          NOT NULL,
    amount          NUMERIC(78,0) NOT NULL,
    PRIMARY KEY (chain_id, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_exit_credit_claims_pool
    ON exit_credit_claims(chain_id, reward_pool, block_number);

-- Progress per (chain, contract set). Deleting a row rescans from
-- SETTLEMENT_FROM_BLOCK (default FROM_BLOCK) on the next restart.
CREATE TABLE IF NOT EXISTS settlement_checkpoints (
    chain_id      BIGINT NOT NULL,
    source        TEXT   NOT NULL,
    last_block    BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (chain_id, source)
)
