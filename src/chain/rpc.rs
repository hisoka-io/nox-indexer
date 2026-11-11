//! Multi-endpoint JSON-RPC transport with sticky failover.
//!
//! `ETH_RPC_URL` may list several endpoints separated by commas. Requests go to
//! the endpoint that last succeeded; transport errors, timeouts, rate limits,
//! range limits and missing-state errors move on to the next endpoint. Only
//! deterministic failures (an `execution reverted` from a view call) return
//! immediately, since every endpoint would answer the same way.
//!
//! Endpoint URLs can embed API keys, so logs and error messages only ever show
//! the host.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use alloy::rpc::client::RpcClient;
use alloy::rpc::json_rpc::{ErrorPayload, RequestPacket, ResponsePacket};
use alloy::transports::{TransportError, TransportErrorKind, TransportFut};
use serde::Deserialize;

/// Upper bound for one request against one endpoint. The HTTP client has no
/// timeout of its own, so a hung endpoint would otherwise stall the sync.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

struct Endpoint {
    url: reqwest::Url,
    label: String,
    secrets: Vec<String>,
}

/// JSON-RPC transport over an ordered list of HTTP endpoints.
#[derive(Clone)]
pub struct FailoverHttp {
    client: reqwest::Client,
    endpoints: Arc<Vec<Endpoint>>,
    preferred: Arc<AtomicUsize>,
    /// Index of the endpoint that served the most recent `eth_getLogs`.
    logs_served_by: Arc<AtomicUsize>,
}

impl fmt::Debug for FailoverHttp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FailoverHttp")
            .field("endpoints", &self.labels())
            .finish()
    }
}

/// Split a comma-separated `ETH_RPC_URL` value into trimmed, non-empty URLs.
pub fn parse_rpc_urls(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_string)
        .collect()
}

/// `scheme://host[:port]` only: no path, query or credentials.
pub fn redact_url(raw: &str) -> String {
    match reqwest::Url::parse(raw) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("?");
            match url.port() {
                Some(port) => format!("{}://{host}:{port}", url.scheme()),
                None => format!("{}://{host}", url.scheme()),
            }
        }
        Err(_) => "<invalid-url>".to_string(),
    }
}

/// One endpoint's answer to one request.
enum Attempt {
    /// A JSON-RPC response that is not an error object.
    Answered(ResponsePacket),
    /// A JSON-RPC error object, as `(code: .., message: .., data: ..)`.
    Rejected { text: String, deterministic: bool },
    /// Transport, HTTP or decoding failure.
    Failed(String),
}

/// JSON-RPC error text in the form the logs and the error classifier use.
fn error_text(error: &ErrorPayload) -> String {
    let data = match &error.data {
        Some(data) => format!("Some({})", data.get()),
        None => "None".to_string(),
    };
    format!(
        "(code: {}, message: {}, data: {data})",
        error.code, error.message
    )
}

/// Errors that every endpoint would reproduce, so failing over is pointless.
fn is_deterministic(error: &ErrorPayload) -> bool {
    let message = error.message.to_lowercase();
    error.code == 3 || message.contains("execution reverted") || message.contains("revert")
}

impl FailoverHttp {
    pub fn new(urls: &[String]) -> Result<Self, String> {
        if urls.is_empty() {
            return Err("at least one RPC URL is required".to_string());
        }
        let endpoints = urls
            .iter()
            .map(|raw| {
                let url = reqwest::Url::parse(raw)
                    .map_err(|e| format!("Invalid RPC URL '{}': {e}", redact_url(raw)))?;
                let normalized = url.to_string();
                let label = redact_url(raw);
                // Longest first so a full URL is replaced before its prefix.
                let mut secrets = vec![normalized, raw.to_string()];
                secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
                secrets.dedup();
                Ok(Endpoint {
                    url,
                    label,
                    secrets,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            client: reqwest::Client::new(),
            endpoints: Arc::new(endpoints),
            preferred: Arc::new(AtomicUsize::new(0)),
            logs_served_by: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// JSON-RPC client that sends every request through this transport.
    pub fn rpc_client(&self) -> RpcClient {
        RpcClient::new(self.clone(), false)
    }

    /// The endpoint that served the most recent `eth_getLogs`. Only one task
    /// fetches logs at a time, so this identifies the endpoint behind a result.
    pub fn logs_served_by(&self) -> usize {
        self.logs_served_by.load(Ordering::Relaxed)
    }

    pub fn label(&self, index: usize) -> &str {
        self.endpoints
            .get(index)
            .map_or("?", |endpoint| endpoint.label.as_str())
    }

    /// `eth_blockNumber` on one specific endpoint, bypassing failover.
    pub async fn block_number_on(&self, index: usize) -> Result<u64, String> {
        #[derive(Deserialize)]
        struct BlockNumber {
            result: alloy::primitives::U64,
        }

        let endpoint = self
            .endpoints
            .get(index)
            .ok_or_else(|| format!("no RPC endpoint #{index}"))?;
        let body = br#"{"jsonrpc":"2.0","id":0,"method":"eth_blockNumber"}"#.to_vec();
        let outcome = tokio::time::timeout(REQUEST_TIMEOUT, self.post(endpoint, body)).await;
        let failed = |error: String| {
            format!(
                "{}: eth_blockNumber failed: {}",
                endpoint.label,
                self.redact(&error)
            )
        };
        match outcome {
            Ok(Ok(body)) => match serde_json::from_slice::<ErrorReply>(&body) {
                Ok(ErrorReply { error: Some(error) }) => Err(failed(error_text(&error))),
                _ => serde_json::from_slice::<BlockNumber>(&body)
                    .map(|reply| reply.result.to::<u64>())
                    .map_err(|error| failed(deserialization_text(&error, &body))),
            },
            Ok(Err(error)) => Err(failed(error)),
            Err(_) => Err(format!(
                "{}: eth_blockNumber timed out after {}s",
                endpoint.label,
                REQUEST_TIMEOUT.as_secs()
            )),
        }
    }

    /// Stop preferring `index` (when it is preferred), so the next request tries
    /// the following endpoint first.
    pub fn demote(&self, index: usize) {
        let count = self.endpoints.len();
        if count > 1 {
            let _ = self.preferred.compare_exchange(
                index,
                (index + 1) % count,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    pub fn labels(&self) -> Vec<String> {
        self.endpoints.iter().map(|e| e.label.clone()).collect()
    }

    fn redact(&self, message: &str) -> String {
        let mut out = message.to_string();
        for endpoint in self.endpoints.iter() {
            for secret in &endpoint.secrets {
                if !secret.is_empty() {
                    out = out.replace(secret.as_str(), &endpoint.label);
                }
            }
        }
        out
    }

    /// POST one JSON body. The reply body is returned whatever the HTTP status,
    /// so a JSON-RPC error object on a 4xx/5xx reply is still read as one.
    async fn post(&self, endpoint: &Endpoint, body: Vec<u8>) -> Result<Vec<u8>, String> {
        let response = self
            .client
            .post(endpoint.url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        let bytes = response.bytes().await.map_err(|error| error.to_string())?;
        Ok(bytes.to_vec())
    }

    async fn attempt(&self, endpoint: &Endpoint, body: Vec<u8>) -> Attempt {
        let body = match self.post(endpoint, body).await {
            Ok(body) => body,
            Err(error) => return Attempt::Failed(error),
        };
        let packet = match serde_json::from_slice::<ResponsePacket>(&body) {
            Ok(packet) => packet,
            Err(error) => return Attempt::Failed(deserialization_text(&error, &body)),
        };
        match &packet {
            ResponsePacket::Single(response) => match response.payload.as_error() {
                Some(error) => Attempt::Rejected {
                    text: error_text(error),
                    deterministic: is_deterministic(error),
                },
                None => Attempt::Answered(packet),
            },
            // Batches are never sent by the indexer; each entry carries its own
            // result or error, so only transport failures fail over.
            ResponsePacket::Batch(_) => Attempt::Answered(packet),
        }
    }

    async fn send(self, request: RequestPacket) -> Result<ResponsePacket, TransportError> {
        let method = request.method_names().next().unwrap_or("?").to_string();
        let body = serde_json::to_vec(&request).map_err(TransportError::ser_err)?;

        let count = self.endpoints.len();
        let start = self.preferred.load(Ordering::Relaxed) % count;
        let mut failures = Vec::with_capacity(count);

        for offset in 0..count {
            let index = (start + offset) % count;
            let endpoint = &self.endpoints[index];
            let outcome =
                tokio::time::timeout(REQUEST_TIMEOUT, self.attempt(endpoint, body.clone())).await;
            let text = match outcome {
                Ok(Attempt::Answered(packet)) => {
                    if method == "eth_getLogs" {
                        self.logs_served_by.store(index, Ordering::Relaxed);
                    }
                    if index != start {
                        self.preferred.store(index, Ordering::Relaxed);
                        tracing::warn!(
                            "RPC failover: {method} served by {} after {} failed endpoint(s): {}",
                            endpoint.label,
                            failures.len(),
                            failures.join(" ")
                        );
                    }
                    return Ok(packet);
                }
                Ok(Attempt::Rejected {
                    text,
                    deterministic: true,
                }) => {
                    return Err(TransportErrorKind::custom_str(&format!(
                        "{}: {}",
                        endpoint.label,
                        self.redact(&text)
                    )));
                }
                Ok(Attempt::Rejected { text, .. } | Attempt::Failed(text)) => self.redact(&text),
                Err(_) => format!("timed out after {}s", REQUEST_TIMEOUT.as_secs()),
            };
            failures.push(format!("[{}: {text}]", endpoint.label));
        }

        // Every endpoint failed: start the next request on the following one so a
        // persistently broken primary does not absorb the first attempt forever.
        self.preferred.store((start + 1) % count, Ordering::Relaxed);
        Err(TransportErrorKind::custom_str(&format!(
            "all {count} RPC endpoint(s) failed for {method}: {}",
            failures.join(" ")
        )))
    }
}

#[derive(Deserialize)]
struct ErrorReply {
    error: Option<ErrorPayload>,
}

fn deserialization_text(error: &serde_json::Error, body: &[u8]) -> String {
    format!(
        "Deserialization Error: {error}. Response: {}",
        String::from_utf8_lossy(body)
    )
}

impl tower::Service<RequestPacket> for FailoverHttp {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: RequestPacket) -> Self::Future {
        Box::pin(self.clone().send(request))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_comma_separated_urls() {
        let urls = parse_rpc_urls(" https://a.example/rpc , ,https://b.example/v1/KEY ");
        assert_eq!(
            urls,
            vec!["https://a.example/rpc", "https://b.example/v1/KEY"]
        );
    }

    #[test]
    fn redaction_hides_paths_queries_and_credentials() {
        assert_eq!(
            redact_url("https://user:pw@rpc.example.com:8443/v2/SECRETKEY?x=1"),
            "https://rpc.example.com:8443"
        );
        assert_eq!(
            redact_url("https://arb.example/SECRET"),
            "https://arb.example"
        );

        let client = FailoverHttp::new(&["https://arb.example/SECRET".to_string()]).unwrap();
        let message = "error sending request for url (https://arb.example/SECRET)";
        assert_eq!(
            client.redact(message),
            "error sending request for url (https://arb.example)"
        );
    }

    #[test]
    fn rejects_an_empty_endpoint_list() {
        assert!(FailoverHttp::new(&[]).is_err());
    }

    #[test]
    fn reverts_are_not_failed_over() {
        let payload = |code: i64, message: &'static str| ErrorPayload {
            code,
            message: message.into(),
            data: None,
        };
        assert!(is_deterministic(&payload(3, "execution reverted")));
        assert!(!is_deterministic(&payload(429, "Too Many Requests")));
        assert_eq!(
            error_text(&payload(-32000, "header not found")),
            "(code: -32000, message: header not found, data: None)"
        );
    }

    #[tokio::test]
    async fn fails_over_to_a_healthy_endpoint_and_sticks_to_it() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Endpoint A: always 429. Endpoint B: returns a block number.
        async fn serve(body: &'static str) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        return;
                    };
                    let mut buf = vec![0_u8; 4096];
                    let _ = socket.read(&mut buf).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                }
            });
            format!("http://{addr}/secret-path")
        }

        let limited =
            serve(r#"{"jsonrpc":"2.0","id":1,"error":{"code":429,"message":"Too Many Requests"}}"#)
                .await;
        let healthy = serve(r#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#).await;
        let client = FailoverHttp::new(&[limited, healthy]).unwrap();

        let rpc = client.rpc_client();
        let block: alloy::primitives::U64 = rpc.request_noparams("eth_blockNumber").await.unwrap();
        assert_eq!(block.to::<u64>(), 42);
        assert_eq!(client.preferred.load(Ordering::Relaxed), 1);

        let again: alloy::primitives::U64 = rpc.request_noparams("eth_blockNumber").await.unwrap();
        assert_eq!(again.to::<u64>(), 42);
    }

    #[tokio::test]
    async fn reports_every_endpoint_when_all_fail_without_leaking_paths() {
        let client = FailoverHttp::new(&[
            "http://127.0.0.1:1/KEY-ONE".to_string(),
            "http://127.0.0.1:2/KEY-TWO".to_string(),
        ])
        .unwrap();
        let error = client
            .rpc_client()
            .request_noparams::<alloy::primitives::U64>("eth_blockNumber")
            .await
            .expect_err("closed ports must fail");
        let text = error.to_string();
        assert!(text.contains("all 2 RPC endpoint(s) failed"), "{text}");
        assert!(
            !text.contains("KEY-ONE") && !text.contains("KEY-TWO"),
            "{text}"
        );
    }
}
