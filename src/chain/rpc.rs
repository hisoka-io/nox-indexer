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
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ethers::providers::{
    Http, HttpClientError, JsonRpcClient, JsonRpcError, ProviderError, RpcError,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Upper bound for one request against one endpoint. ethers' HTTP transport has
/// no timeout of its own, so a hung endpoint would otherwise stall the sync.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

struct Endpoint {
    http: Http,
    label: String,
    secrets: Vec<String>,
}

/// JSON-RPC client over an ordered list of HTTP endpoints.
#[derive(Clone)]
pub struct FailoverHttp {
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

impl FailoverHttp {
    pub fn new(urls: &[String]) -> Result<Self, String> {
        if urls.is_empty() {
            return Err("at least one RPC URL is required".to_string());
        }
        let endpoints = urls
            .iter()
            .map(|raw| {
                let http = Http::from_str(raw)
                    .map_err(|e| format!("Invalid RPC URL '{}': {e}", redact_url(raw)))?;
                let normalized = http.url().to_string();
                let label = redact_url(raw);
                // Longest first so a full URL is replaced before its prefix.
                let mut secrets = vec![normalized, raw.to_string()];
                secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
                secrets.dedup();
                Ok(Endpoint {
                    http,
                    label,
                    secrets,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self {
            endpoints: Arc::new(endpoints),
            preferred: Arc::new(AtomicUsize::new(0)),
            logs_served_by: Arc::new(AtomicUsize::new(0)),
        })
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
        let endpoint = self
            .endpoints
            .get(index)
            .ok_or_else(|| format!("no RPC endpoint #{index}"))?;
        let outcome = tokio::time::timeout(
            REQUEST_TIMEOUT,
            endpoint
                .http
                .request::<_, ethers::types::U64>("eth_blockNumber", ()),
        )
        .await;
        match outcome {
            Ok(Ok(block)) => Ok(block.as_u64()),
            Ok(Err(error)) => Err(format!(
                "{}: eth_blockNumber failed: {}",
                endpoint.label,
                self.redact(&error.to_string())
            )),
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
}

/// Errors that every endpoint would reproduce, so failing over is pointless.
fn is_deterministic(error: &HttpClientError) -> bool {
    match error {
        HttpClientError::JsonRpcError(rpc) => {
            let message = rpc.message.to_lowercase();
            rpc.code == 3 || message.contains("execution reverted") || message.contains("revert")
        }
        _ => false,
    }
}

/// Error surfaced once every endpoint failed (or one failed deterministically).
#[derive(Debug)]
pub struct FailoverError {
    summary: String,
    last: Option<HttpClientError>,
}

impl fmt::Display for FailoverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.summary)
    }
}

impl std::error::Error for FailoverError {}

impl RpcError for FailoverError {
    fn as_error_response(&self) -> Option<&JsonRpcError> {
        self.last.as_ref().and_then(RpcError::as_error_response)
    }

    fn as_serde_error(&self) -> Option<&serde_json::Error> {
        self.last.as_ref().and_then(RpcError::as_serde_error)
    }
}

impl From<FailoverError> for ProviderError {
    fn from(error: FailoverError) -> Self {
        ProviderError::JsonRpcClientError(Box::new(error))
    }
}

#[async_trait]
impl JsonRpcClient for FailoverHttp {
    type Error = FailoverError;

    async fn request<T, R>(&self, method: &str, params: T) -> Result<R, Self::Error>
    where
        T: fmt::Debug + Serialize + Send + Sync,
        R: DeserializeOwned + Send,
    {
        let params = serde_json::to_value(&params).map_err(|e| FailoverError {
            summary: format!("failed to serialize {method} params: {e}"),
            last: None,
        })?;

        let count = self.endpoints.len();
        let start = self.preferred.load(Ordering::Relaxed) % count;
        let mut failures = Vec::with_capacity(count);
        let mut last = None;

        for offset in 0..count {
            let index = (start + offset) % count;
            let endpoint = &self.endpoints[index];
            let outcome =
                tokio::time::timeout(REQUEST_TIMEOUT, endpoint.http.request(method, &params)).await;
            match outcome {
                Ok(Ok(value)) => {
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
                    return Ok(value);
                }
                Ok(Err(error)) => {
                    let text = self.redact(&error.to_string());
                    if is_deterministic(&error) {
                        return Err(FailoverError {
                            summary: format!("{}: {text}", endpoint.label),
                            last: Some(error),
                        });
                    }
                    failures.push(format!("[{}: {text}]", endpoint.label));
                    last = Some(error);
                }
                Err(_) => {
                    failures.push(format!(
                        "[{}: timed out after {}s]",
                        endpoint.label,
                        REQUEST_TIMEOUT.as_secs()
                    ));
                }
            }
        }

        // Every endpoint failed: start the next request on the following one so a
        // persistently broken primary does not absorb the first attempt forever.
        self.preferred.store((start + 1) % count, Ordering::Relaxed);
        Err(FailoverError {
            summary: format!(
                "all {count} RPC endpoint(s) failed for {method}: {}",
                failures.join(" ")
            ),
            last,
        })
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
        let revert = HttpClientError::JsonRpcError(JsonRpcError {
            code: 3,
            message: "execution reverted".to_string(),
            data: None,
        });
        let rate_limit = HttpClientError::JsonRpcError(JsonRpcError {
            code: 429,
            message: "Too Many Requests".to_string(),
            data: None,
        });
        assert!(is_deterministic(&revert));
        assert!(!is_deterministic(&rate_limit));
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

        let block: ethers::types::U64 = client.request("eth_blockNumber", ()).await.unwrap();
        assert_eq!(block.as_u64(), 42);
        assert_eq!(client.preferred.load(Ordering::Relaxed), 1);

        let again: ethers::types::U64 = client.request("eth_blockNumber", ()).await.unwrap();
        assert_eq!(again.as_u64(), 42);
    }

    #[tokio::test]
    async fn reports_every_endpoint_when_all_fail_without_leaking_paths() {
        let client = FailoverHttp::new(&[
            "http://127.0.0.1:1/KEY-ONE".to_string(),
            "http://127.0.0.1:2/KEY-TWO".to_string(),
        ])
        .unwrap();
        let error = client
            .request::<_, ethers::types::U64>("eth_blockNumber", ())
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
