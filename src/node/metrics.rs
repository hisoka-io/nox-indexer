use serde::{Deserialize, Serialize};

fn bool_from_number_or_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct BoolVisitor;

    impl de::Visitor<'_> for BoolVisitor {
        type Value = bool;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a boolean or numeric value")
        }

        fn visit_bool<E: de::Error>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<bool, E> {
            Ok(v >= 1)
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<bool, E> {
            Ok(v >= 1)
        }

        fn visit_f64<E: de::Error>(self, v: f64) -> Result<bool, E> {
            Ok(v >= 1.0)
        }
    }

    deserializer.deserialize_any(BoolVisitor)
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct StructuredMetrics {
    pub active_peers: f64,
    pub uptime_seconds: f64,
    /// Unix epoch seconds at which the node process started. Changes on restart,
    /// which is how lifetime metric offsets detect a new incarnation.
    pub node_start_time: f64,
    pub health_status: f64,
    pub packets_received: f64,
    pub packets_forwarded: f64,
    pub worker_queue_depth: f64,
    pub mix_queue_depth: f64,
    pub egress_queue_depth: f64,
    pub cover_loop_generated: f64,
    pub cover_drop_generated: f64,
    #[serde(deserialize_with = "bool_from_number_or_bool")]
    pub cover_loop_degraded: bool,
    #[serde(deserialize_with = "bool_from_number_or_bool")]
    pub cover_drop_degraded: bool,
    pub sphinx_errors: f64,
    pub replay_duplicate: f64,
    pub cumulative_revenue_usd: f64,
    pub cumulative_cost_usd: f64,
    pub exit_payloads_dispatched: f64,
    pub latency_p50: f64,
    pub latency_p95: f64,
    pub latency_p99: f64,
    pub build_version: String,
    #[serde(skip_serializing)]
    pub ingress_response_buffer: f64,

    pub exit_echo: f64,
    pub exit_http: f64,
    pub exit_rpc: f64,
    pub exit_broadcast: f64,
    pub exit_ethereum: f64,
    pub exit_traffic: f64,

    pub profitable_count: f64,
    pub unprofitable_count: f64,
    pub eth_pending: f64,
    pub eth_transactions_submitted: f64,

    pub egress_forwarded: f64,
    pub egress_exited: f64,
}
