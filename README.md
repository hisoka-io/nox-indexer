# Nox Indexer

Indexes mix nodes registered on the NoxRegistry contract. Polls the chain for registrations, scrapes node metrics over SSE, tracks uptime with an EMA score, and serves everything over HTTP + WebSocket.

<img src="assets/indexer.png" alt="architecture" width="600" />

## Setup

**Prerequisites:** Rust 1.75+ and PostgreSQL 14+

```bash
docker run -d --name nox-postgres -p 5432:5432 \
  -e POSTGRES_DB=indexer -e POSTGRES_HOST_AUTH_METHOD=trust \
  postgres:17-alpine

# configure
cp .env.sample .env

# run
cargo run
```

tables are created automatically on first run.

## Config

All options are configurable via env vars or CLI flags. See `.env.sample` for defaults.

| Variable | Description | Required |
|----------|-------------|----------|
| `REGISTRY_ADDRESS` | NoxRegistry contract address | Yes |
| `ETH_RPC_URL` | RPC endpoint, or a comma-separated failover list | Yes (testnet/mainnet) |
| `DATABASE_URL` | Postgres connection string | Yes |
| `NETWORK` | `localtestnet`, `testnet`, or `mainnet` | No |
| `FROM_BLOCK` | Exact NoxRegistry deployment block | Yes |
| `EXPECTED_CHAIN_ID` | Refuse to sync against any other chain | No |
| `CHAIN_CONFIRMATIONS` | Blocks behind head treated as final (default 20; 0 on localtestnet) | No |
| `LOG_CHUNK_SIZE` / `LOG_CHUNK_MIN` / `LOG_CHUNK_MAX` | `eth_getLogs` range: start, floor, ceiling | No |

### Chain sync

- Progress is checkpointed per `(chain_id, registry_address)` in `indexer_checkpoints`. A restart
  resumes after the checkpoint; a registry with no checkpoint is replayed from `FROM_BLOCK`.
- Every sync ends by checking the replayed member set against the registry's `relayerCount()` and
  `topologyFingerprint()`. A resumed set that disagrees falls back to a full replay.
- Failed syncs are retried forever with jittered exponential backoff (2s up to 5min); progress
  made before the failure is kept. `eth_getLogs` ranges shrink on range and rate-limit errors
  and grow back after successes.
- RPC endpoints fail over in order on transport errors, timeouts, 429s, range limits and missing
  state; the endpoint that last worked is tried first. Logs show only endpoint hosts.
- Switching `REGISTRY_ADDRESS` marks every node that is not a member of the new registry
  `deregistered`. Stats (`node_reputation`, `node_metric_offsets`) are keyed by address and kept,
  so a node that registers again resumes its counters.
- Both `relayers()` layouts are decoded: the April 2026 7-field registry and the 9-field UUPS
  registry (`status`, `frozen`).
- To force a full replay: `DELETE FROM indexer_checkpoints WHERE registry_address = '<lowercase registry>';`
  then restart.

## API

```
GET  /v1/state        nodes + metrics + events + reputation + network totals + sync status
GET  /v1/reputation   uptime rankings (registered nodes only)
GET  /seed/topology   chain-pinned SDK topology snapshot
GET  /healthz         200 if db is up; body includes chain sync status
WS   /v1/live         pushes CLUSTER / METRICS / EVENT messages
```

`/v1/state` fields beyond the per-node data:

| Field | Meaning |
|-------|---------|
| `network_totals` | Lifetime counters (`packetsReceived`, `packetsForwarded`, ...) summed over every node ever scraped, registered or not, plus `scrapedNodeCount` (how many nodes feed the totals; not the registered count, which is `nodes.length`) |
| `network_genesis_ms` | Earliest uptime-check evidence (Unix ms), estimated once as `last_check_ms - total_checks * interval` |
| `network_reputation_avg` | Mean reputation of registered nodes only |
| `indexer` | Sync `phase` (`starting`, `syncing`, `retrying`, `live`), `chain_id`, `registry_address`, `last_block`, `verified`, `attempts`, `last_error` (a coarse category such as `rpc rate limited`; the full error is only in the logs) |

Nodes carry `frozen`: a frozen node is still a registry member but must not be routed through.

`/seed/topology` returns schema version 2. Its `nodes` array contains every registered member,
with profiles read at the last processed block (a few seconds behind head), and its count and
fingerprint are checked against the registry at that block. `liveness` is a separate
online/offline observation for the same member set; frozen members stay in `nodes` (the
fingerprint includes them) but are always `offline`. Snapshots are cached for 30s and rebuilt
when membership changes; the endpoint returns 503 until the first sync completes or when the
registry cannot be read.

## Docker

```bash
docker build -t nox-indexer .
docker run -d -p 4000:4000 --env-file .env nox-indexer
```

## Reputation Scoring

Node uptime is tracked via an exponential moving average (EMA) computed every check interval (default: 60s). Scores range from 0 to 100 based on `/metrics/json` endpoint availability.

| Parameter | Value |
|-----------|-------|
| Alpha (α) | 0.01 |
| Responsive sample | 100 |
| Unresponsive sample | 0 |
| Stale penalty (>6h without check) | 5% per round |

New nodes initialize from their first check result. Deregistered nodes are excluded from rankings and
their scores do not decay, so a node that registers again resumes its score.

## License

[MIT](./LICENSE)
