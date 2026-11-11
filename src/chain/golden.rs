//! Golden vectors for everything the indexer sends to and reads from the chain.
//!
//! Each case runs production code against local JSON-RPC endpoints that serve
//! the canned replies in `tests/golden/chain_inputs.json`, and compares the
//! JSON-RPC requests that reached the endpoints plus the decoded results with
//! `tests/golden/chain_outputs.json`. `GOLDEN_RECORD=1 cargo test golden`
//! rewrites the outputs file.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::settlement::{self, ExitSettlementTotals, SettlementConfig};
use super::*;

const INPUTS: &str = include_str!("../../tests/golden/chain_inputs.json");
const OUTPUTS_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/chain_outputs.json"
);

type Handler = Arc<dyn Fn(&Value) -> (u16, Value) + Send + Sync>;

/// One local JSON-RPC endpoint that records every request it receives.
struct Endpoint {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl Endpoint {
    async fn start(handler: Handler) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let handler = handler.clone();
                let log = log.clone();
                tokio::spawn(async move {
                    let Some(body) = read_body(&mut socket).await else {
                        return;
                    };
                    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
                        return;
                    };
                    let (status, reply) = match &request {
                        Value::Array(batch) => {
                            let replies: Vec<Value> = batch
                                .iter()
                                .map(|one| respond(&handler, &log, one).1)
                                .collect();
                            (200, Value::Array(replies))
                        }
                        one => respond(&handler, &log, one),
                    };
                    let body = reply.to_string();
                    let reason = if status == 200 { "OK" } else { "Error" };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            url: format!("http://{addr}/golden"),
            requests,
        }
    }

    fn take(&self) -> Vec<Value> {
        std::mem::take(&mut *self.requests.lock())
    }
}

fn respond(handler: &Handler, log: &Mutex<Vec<Value>>, request: &Value) -> (u16, Value) {
    log.lock().push(json!({
        "method": request["method"],
        "params": request["params"],
    }));
    let (status, mut reply) = handler(request);
    reply["jsonrpc"] = json!("2.0");
    reply["id"] = request["id"].clone();
    (status, reply)
}

async fn read_body(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = vec![0_u8; 16384];
    loop {
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);
        let Some(split) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buf[..split]).to_string();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0);
        if buf.len() >= split + 4 + length {
            return Some(buf[split + 4..split + 4 + length].to_vec());
        }
    }
}

fn ok(result: Value) -> (u16, Value) {
    (200, json!({ "result": result }))
}

fn rpc_error(status: u16, code: i64, message: &str) -> (u16, Value) {
    (
        status,
        json!({ "error": { "code": code, "message": message } }),
    )
}

fn inputs() -> Value {
    serde_json::from_str(INPUTS).unwrap()
}

/// Canned replies for the registry, the entry point and the reward pool.
fn chain_handler(inputs: Value) -> Handler {
    Arc::new(move |request: &Value| {
        let params = &request["params"];
        match request["method"].as_str().unwrap_or_default() {
            "eth_chainId" => ok(json!("0x66eee")),
            "eth_blockNumber" => ok(json!("0x12a05f20")),
            "eth_getBlockByNumber" => {
                if params[0] == json!("0x1") {
                    ok(Value::Null)
                } else {
                    ok(inputs["block"].clone())
                }
            }
            "eth_getLogs" => {
                let filter = params[0].to_string();
                if filter.contains(inputs["entry_point"].as_str().unwrap()) {
                    ok(inputs["settlement_logs"].clone())
                } else {
                    ok(inputs["registry_logs"].clone())
                }
            }
            "eth_call" => {
                let call = &params[0];
                let data = call["data"]
                    .as_str()
                    .or_else(|| call["input"].as_str())
                    .unwrap_or_default()
                    .to_lowercase();
                let selector = data.get(..10).unwrap_or_default().to_string();
                let argument = data
                    .get(34..74)
                    .map(|address| format!("0x{address}"))
                    .unwrap_or_default();
                match selector.as_str() {
                    "0x5300f841" => ok(inputs["profiles"][&argument].clone()),
                    "0x55b211e3" => ok(inputs["roles"][&argument].clone()),
                    "0xcf1a7a21" => ok(inputs["relayer_count"].clone()),
                    "0x3cce4d3d" => ok(inputs["fingerprint"].clone()),
                    other => match inputs["pool_views"].get(other) {
                        Some(value) => ok(value.clone()),
                        None => rpc_error(200, 3, "execution reverted"),
                    },
                }
            }
            other => rpc_error(200, -32601, &format!("method {other} not found")),
        }
    })
}

fn chain_for(urls: &[String], confirmations: u64) -> ChainConfig {
    ChainConfig::new(
        urls,
        "0x8626AF80DB409BED3C19871FADF9B0CE7AA641BC",
        1,
        0,
        confirmations,
        None,
        LogChunkConfig {
            initial: 1_000,
            min: 10,
            max: 1_000,
        },
    )
    .unwrap()
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn event_json(event: Option<ChainNodeEvent>) -> Value {
    match event {
        None => Value::Null,
        Some(ChainNodeEvent::Registered { address }) => {
            json!({ "kind": "registered", "address": format!("{address:?}") })
        }
        Some(ChainNodeEvent::ProfileChanged { address }) => {
            json!({ "kind": "profile_changed", "address": format!("{address:?}") })
        }
        Some(ChainNodeEvent::Removed { address }) => {
            json!({ "kind": "removed", "address": format!("{address:?}") })
        }
        Some(ChainNodeEvent::Unstaked { address, amount }) => json!({
            "kind": "unstaked",
            "address": format!("{address:?}"),
            "amount": amount.to_string(),
        }),
    }
}

fn profile_json(result: Result<profile::RelayerProfile, String>) -> Value {
    match result {
        Ok(profile) => json!({
            "sphinx_key": hex(&profile.sphinx_key),
            "url": profile.url,
            "ingress_url": profile.ingress_url,
            "metadata_url": profile.metadata_url,
            "staked_amount": profile.staked_amount.to_string(),
            "unstake_time": profile.unstake_time.to_string(),
            "is_registered": profile.is_registered,
            "status": profile.status,
            "frozen": profile.frozen,
            "layout": format!("{:?}", profile.layout),
            "is_member": profile.is_member(),
        }),
        Err(error) => json!({ "error": error }),
    }
}

fn node_json(result: Result<Option<OnChainNode>, String>) -> Value {
    match result {
        Ok(Some(node)) => json!({
            "address": node.address,
            "url": node.url,
            "ingress_url": node.ingress_url,
            "metadata_url": node.metadata_url,
            "sphinx_key": node.sphinx_key,
            "role": node.role,
            "frozen": node.frozen,
        }),
        Ok(None) => Value::Null,
        Err(error) => json!({ "error": error }),
    }
}

fn pinned_json(result: Result<(Vec<PinnedTopologyNode>, String), String>) -> Value {
    match result {
        Ok((nodes, fingerprint)) => json!({
            "fingerprint": fingerprint,
            "nodes": nodes
                .into_iter()
                .map(|node| json!({
                    "address": node.address,
                    "sphinx_key": node.sphinx_key,
                    "url": node.url,
                    "stake": node.stake,
                    "is_privileged": node.is_privileged,
                    "layer": node.layer,
                    "role": node.role,
                    "ingress_url": node.ingress_url,
                    "metadata_url": node.metadata_url,
                    "frozen": node.frozen,
                }))
                .collect::<Vec<_>>(),
        }),
        Err(error) => json!({ "error": error }),
    }
}

fn settlement_json(log: Option<settlement::SettlementLog>) -> Value {
    match log {
        None => Value::Null,
        Some(settlement::SettlementLog::Settled(settled)) => {
            json!({ "settled": serde_json::to_value(settled).unwrap() })
        }
        Some(settlement::SettlementLog::Claimed(claim)) => {
            json!({ "claimed": serde_json::to_value(claim).unwrap() })
        }
    }
}

fn sorted(mut requests: Vec<Value>) -> Vec<Value> {
    requests.sort_by_key(Value::to_string);
    requests
}

/// Endpoint URLs carry random ports; error texts are compared with labels.
fn normalize(text: &str, endpoints: &[&Endpoint]) -> String {
    let mut out = text.to_string();
    for (index, endpoint) in endpoints.iter().enumerate() {
        out = out.replace(&endpoint.url, &format!("<endpoint {index}>"));
        out = out.replace(
            &redact_url_of(&endpoint.url),
            &format!("<endpoint {index}>"),
        );
    }
    out
}

fn redact_url_of(url: &str) -> String {
    rpc::redact_url(url)
}

fn address(text: &str) -> Address {
    text.parse().unwrap()
}

async fn record_all() -> BTreeMap<String, Value> {
    let inputs = inputs();
    let a = inputs["addresses"]["a"].as_str().unwrap().to_string();
    let b = inputs["addresses"]["b"].as_str().unwrap().to_string();
    let c = inputs["addresses"]["c"].as_str().unwrap().to_string();
    let d = inputs["addresses"]["d"].as_str().unwrap().to_string();
    let entry_point = inputs["entry_point"].as_str().unwrap().to_string();
    let reward_pool = inputs["reward_pool"].as_str().unwrap().to_string();
    let shutdown = CancellationToken::new();
    let mut cases = BTreeMap::new();

    let endpoint = Endpoint::start(chain_handler(inputs.clone())).await;
    let chain = chain_for(std::slice::from_ref(&endpoint.url), 20);

    // Chain id, head and the safe head.
    let chain_id = chain.chain_id().await;
    let head = chain.current_block().await;
    let safe = chain.safe_head().await;
    cases.insert(
        "chain_id_and_head".to_string(),
        json!({
            "requests": endpoint.take(),
            "output": { "chain_id": chain_id, "head": head, "safe_head": safe },
        }),
    );

    // Registry logs near the head (the endpoint head check runs).
    let (logs, end) = chain
        .fetch_logs_chunk(312_400_000, 312_400_500, &shutdown)
        .await
        .unwrap();
    let events: Vec<Value> = logs
        .iter()
        .map(|log| event_json(chain.decode_log(log)))
        .collect();
    cases.insert(
        "registry_logs".to_string(),
        json!({
            "requests": endpoint.take(),
            "output": { "end": end, "count": logs.len(), "events": events },
        }),
    );

    // Settlement logs, with and without the reward pool.
    let with_pool = SettlementConfig::new(Some(&entry_point), Some(&reward_pool), 1)
        .unwrap()
        .unwrap();
    let (logs, end) = chain
        .fetch_filtered_logs_chunk(&with_pool.filter(), 312_421_000, 312_421_700, &shutdown)
        .await
        .unwrap();
    let decoded: Vec<Value> = logs
        .iter()
        .map(|log| settlement_json(settlement::decode_settlement_log(log, &with_pool)))
        .collect();
    let without_pool = SettlementConfig::new(Some(&entry_point), None, 1)
        .unwrap()
        .unwrap();
    let without: Vec<Value> = logs
        .iter()
        .map(|log| settlement_json(settlement::decode_settlement_log(log, &without_pool)))
        .collect();
    let requests = endpoint.take();
    let _ = chain
        .fetch_filtered_logs_chunk(&without_pool.filter(), 312_421_000, 312_421_000, &shutdown)
        .await
        .unwrap();
    cases.insert(
        "settlement_logs".to_string(),
        json!({
            "requests": requests,
            "requests_without_pool": endpoint.take(),
            "output": {
                "end": end,
                "with_pool": decoded,
                "without_pool": without,
                "source_key": with_pool.source_key(),
                "source_key_without_pool": without_pool.source_key(),
            },
        }),
    );

    // relayers(address) at a block and at latest, on both layouts.
    let profiles = vec![
        profile_json(chain.fetch_profile(address(&a), Some(312_500_000)).await),
        profile_json(chain.fetch_profile(address(&b), None).await),
        profile_json(chain.fetch_profile(address(&c), Some(1)).await),
        profile_json(chain.fetch_profile(address(&d), None).await),
    ];
    cases.insert(
        "profiles".to_string(),
        json!({ "requests": endpoint.take(), "output": profiles }),
    );

    // Node info (profile + getNodeRole at latest).
    let nodes = vec![
        node_json(chain.fetch_node_info(address(&a)).await),
        node_json(chain.fetch_node_info(address(&b)).await),
        node_json(chain.fetch_node_info(address(&c)).await),
        node_json(chain.fetch_node_info(address(&d)).await),
    ];
    cases.insert(
        "node_info".to_string(),
        json!({ "requests": endpoint.take(), "output": nodes }),
    );

    // relayerCount and topologyFingerprint at a block.
    let (count, fingerprint) = chain.membership_at(312_500_000).await.unwrap();
    cases.insert(
        "membership".to_string(),
        json!({
            "requests": endpoint.take(),
            "output": { "count": count.to_string(), "fingerprint": hex(&fingerprint) },
        }),
    );

    // Pinned topology: the full member set, then a partial one.
    let full = chain
        .pinned_topology_members(&[c.clone(), a.clone(), b.clone()], 312_500_000)
        .await;
    let full_requests = sorted(endpoint.take());
    let partial = chain
        .pinned_topology_members(&[a.clone(), b.clone()], 312_500_000)
        .await;
    let duplicate = chain
        .pinned_topology_members(&[a.clone(), a.clone(), b.clone()], 312_500_000)
        .await;
    let zero = chain
        .pinned_topology_members(std::slice::from_ref(&a), 0)
        .await;
    cases.insert(
        "pinned_topology".to_string(),
        json!({
            "requests": full_requests,
            "requests_rejected": sorted(endpoint.take()),
            "output": {
                "full": pinned_json(full),
                "partial": pinned_json(partial),
                "duplicate": pinned_json(duplicate),
                "zero_block": pinned_json(zero),
            },
        }),
    );

    // Block timestamps, including a block the endpoint does not have.
    let timestamp = chain.block_timestamp(312_500_000).await;
    let missing = chain.block_timestamp(1).await;
    cases.insert(
        "block_timestamp".to_string(),
        json!({
            "requests": endpoint.take(),
            "output": { "timestamp": timestamp, "missing": missing },
        }),
    );

    // Reward pool views at the last stored block.
    let mut exits = vec![
        ExitSettlementTotals {
            exit_address: a.clone(),
            fee_asset: c.clone(),
            executions: 2,
            exit_fees: "40".to_string(),
            network_fees: "2".to_string(),
            claimed: "15".to_string(),
            claimable: None,
        },
        ExitSettlementTotals {
            exit_address: b.clone(),
            fee_asset: c.clone(),
            executions: 1,
            exit_fees: "4".to_string(),
            network_fees: "1".to_string(),
            claimed: "0".to_string(),
            claimable: None,
        },
    ];
    let gauges = settlement::read_pool_gauges(
        &shutdown,
        &chain,
        address(&reward_pool),
        312_421_700,
        &mut exits,
    )
    .await
    .unwrap();
    cases.insert(
        "pool_gauges".to_string(),
        json!({
            "requests": endpoint.take(),
            "output": {
                "exits": serde_json::to_value(&exits).unwrap(),
                "gauges": serde_json::to_value(&gauges).unwrap(),
            },
        }),
    );

    // Pure encodings.
    let calldata: Vec<Value> = [&a, &b, &d]
        .iter()
        .map(|text| json!(hex(&profile::relayers_calldata(address(text)))))
        .collect();
    let mut fingerprint_members: Vec<Address> = vec![address(&a), address(&b), address(&c)];
    fingerprint_members.sort();
    let fingerprint = {
        let expected: [u8; 32] = {
            let raw = inputs["fingerprint"].as_str().unwrap();
            let mut out = [0_u8; 32];
            for (index, byte) in out.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&raw[2 + index * 2..4 + index * 2], 16).unwrap();
            }
            out
        };
        validate_pinned_membership(&fingerprint_members, U256::from(3_u64), expected)
    };
    cases.insert(
        "encodings".to_string(),
        json!({
            "output": {
                "relayers_calldata": calldata,
                "registry_hex": chain.registry_hex,
                "fingerprint_check": fingerprint,
            },
        }),
    );

    // Range errors shrink the chunk to the advertised limit.
    let limited = Endpoint::start(Arc::new(|request: &Value| {
        match request["method"].as_str().unwrap_or_default() {
            "eth_blockNumber" => ok(json!("0x12a05f20")),
            "eth_getLogs" => {
                let filter = &request["params"][0];
                let block = |key: &str| {
                    u64::from_str_radix(
                        filter[key]
                            .as_str()
                            .unwrap_or("0x0")
                            .trim_start_matches("0x"),
                        16,
                    )
                    .unwrap_or(0)
                };
                if block("toBlock") - block("fromBlock") + 1 > 100 {
                    rpc_error(200, -32602, "exceed maximum block range: 100")
                } else {
                    ok(json!([]))
                }
            }
            _ => rpc_error(200, -32601, "not found"),
        }
    }))
    .await;
    let narrow = chain_for(std::slice::from_ref(&limited.url), 20);
    let (logs, end) = narrow
        .fetch_logs_chunk(1_000, 1_500, &shutdown)
        .await
        .unwrap();
    let (_, second_end) = narrow
        .fetch_logs_chunk(end + 1, 1_500, &shutdown)
        .await
        .unwrap();
    cases.insert(
        "range_shrink".to_string(),
        json!({
            "requests": limited.take(),
            "output": { "end": end, "second_end": second_end, "logs": logs.len() },
        }),
    );

    // A rate-limited endpoint (HTTP 429 with a JSON-RPC error body) is failed
    // over and the healthy one stays preferred.
    let throttled = Endpoint::start(Arc::new(|_: &Value| {
        rpc_error(429, 429, "Too Many Requests")
    }))
    .await;
    let healthy = Endpoint::start(chain_handler(inputs.clone())).await;
    let pair = chain_for(&[throttled.url.clone(), healthy.url.clone()], 20);
    let first = pair.current_block().await;
    let second = pair.current_block().await;
    cases.insert(
        "rate_limited_failover".to_string(),
        json!({
            "requests": { "throttled": throttled.take(), "healthy": healthy.take() },
            "output": { "first": first, "second": second },
        }),
    );

    // A revert is answered once, never failed over.
    let reverting = Endpoint::start(Arc::new(|_: &Value| {
        rpc_error(200, 3, "execution reverted")
    }))
    .await;
    let healthy = Endpoint::start(chain_handler(inputs.clone())).await;
    let pair = chain_for(&[reverting.url.clone(), healthy.url.clone()], 20);
    let reverted = pair.fetch_profile(address(&a), None).await;
    let text = reverted.as_ref().err().cloned().unwrap_or_default();
    cases.insert(
        "revert_not_failed_over".to_string(),
        json!({
            "requests": { "reverting": reverting.take(), "healthy": healthy.take() },
            "output": {
                "is_err": reverted.is_err(),
                "mentions_revert": text.contains("execution reverted"),
                "kind": format!("{:?}", retry::classify_rpc_error(&text)),
            },
        }),
    );

    // Every endpoint failing: one summary naming each endpoint by host only.
    let lagging = |_: &Value| rpc_error(200, -32000, "header not found");
    let first_down = Endpoint::start(Arc::new(lagging)).await;
    let second_down = Endpoint::start(Arc::new(lagging)).await;
    let pair = chain_for(&[first_down.url.clone(), second_down.url.clone()], 20);
    let error = pair.current_block().await.unwrap_err();
    let range = Endpoint::start(Arc::new(|_: &Value| {
        rpc_error(200, -32005, "query returned more than 10000 results")
    }))
    .await;
    let throttled = Endpoint::start(Arc::new(|_: &Value| {
        rpc_error(429, 429, "Too Many Requests")
    }))
    .await;
    let mixed = chain_for(&[throttled.url.clone(), range.url.clone()], 20);
    let mixed_error = mixed.current_block().await.unwrap_err();
    cases.insert(
        "all_endpoints_fail".to_string(),
        json!({
            "requests": { "first": first_down.take(), "second": second_down.take() },
            "output": {
                "error": normalize(&error, &[&first_down, &second_down]),
                "kind": format!("{:?}", retry::classify_rpc_error(&error)),
                "mixed_kind": format!("{:?}", retry::classify_rpc_error(&mixed_error)),
                "leaks_path": error.contains("/golden") || mixed_error.contains("/golden"),
            },
        }),
    );

    cases
}

#[tokio::test]
async fn chain_reads_match_the_golden_vectors() {
    let actual = record_all().await;
    if std::env::var("GOLDEN_RECORD").as_deref() == Ok("1") {
        let text = serde_json::to_string_pretty(&actual).unwrap();
        std::fs::write(OUTPUTS_PATH, text + "\n").unwrap();
        return;
    }
    let expected: BTreeMap<String, Value> =
        serde_json::from_str(&std::fs::read_to_string(OUTPUTS_PATH).unwrap()).unwrap();
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "golden case names"
    );
    for (name, value) in &actual {
        let want = &expected[name];
        assert_eq!(
            value,
            want,
            "golden case {name}\nactual:   {}\nexpected: {}",
            serde_json::to_string_pretty(value).unwrap(),
            serde_json::to_string_pretty(want).unwrap()
        );
    }
}
