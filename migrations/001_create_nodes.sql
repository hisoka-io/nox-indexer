CREATE TABLE IF NOT EXISTS nodes (
    address                  TEXT PRIMARY KEY,
    id                       TEXT NOT NULL,
    admin_port               INTEGER NOT NULL DEFAULT 0,
    ingress_port             INTEGER NOT NULL DEFAULT 0,
    p2p_addr                 TEXT NOT NULL DEFAULT '',
    sphinx_key               TEXT NOT NULL DEFAULT '',
    admin_url                TEXT NOT NULL DEFAULT '',
    status                   TEXT NOT NULL DEFAULT 'offline',
    role                     SMALLINT NOT NULL DEFAULT 0,
    layer                    SMALLINT NOT NULL DEFAULT 0,
    latitude                 DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    longitude                DOUBLE PRECISION NOT NULL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
CREATE INDEX IF NOT EXISTS idx_nodes_uptime_targets ON nodes(status, admin_url)
    WHERE admin_url != '' AND status != 'deregistered';

CREATE TABLE IF NOT EXISTS node_reputation (
    address        TEXT PRIMARY KEY,
    total_checks   INTEGER NOT NULL DEFAULT 0,
    passed_checks  INTEGER NOT NULL DEFAULT 0,
    last_check_ms  BIGINT  NOT NULL DEFAULT 0,
    streak         INTEGER NOT NULL DEFAULT 0,
    score          DOUBLE PRECISION NOT NULL DEFAULT 100.0
);

CREATE TABLE IF NOT EXISTS indexer_state (
    key   TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);
