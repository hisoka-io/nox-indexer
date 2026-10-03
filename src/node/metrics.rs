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
    pub cumulative_authorized_revenue_usd: f64,
    /// Planned initial transaction cost. The exporter retains this legacy JSON key.
    pub cumulative_cost_usd: f64,
    pub cumulative_maximum_cost_usd: f64,
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

/// Response header in which nodes report their build version. Their JSON
/// metrics body has no version field.
pub const NODE_VERSION_HEADER: &str = "x-nox-version";
const MAX_BUILD_VERSION_LEN: usize = 128;

fn clean_version(raw: &str) -> String {
    raw.chars()
        .filter(char::is_ascii_graphic)
        .take(MAX_BUILD_VERSION_LEN)
        .collect()
}

impl StructuredMetrics {
    /// Set `build_version` from the body if it carries one, else from the
    /// version header. Only printable ASCII is kept, and the length is capped.
    pub fn fill_build_version(&mut self, header: Option<&str>) {
        let from_body = clean_version(&self.build_version);
        self.build_version = if from_body.is_empty() {
            header.map(clean_version).unwrap_or_default()
        } else {
            from_body
        };
    }
}

#[cfg(test)]
mod tests {
    use super::StructuredMetrics;

    #[test]
    fn build_version_comes_from_the_header_when_the_body_has_none() {
        let mut metrics = StructuredMetrics::default();
        metrics.fill_build_version(Some("0.1.0+48aca982e0cc05a76836800239563674e1b16499"));
        assert_eq!(
            metrics.build_version,
            "0.1.0+48aca982e0cc05a76836800239563674e1b16499"
        );

        let mut metrics = StructuredMetrics {
            build_version: "0.4.0".to_string(),
            ..StructuredMetrics::default()
        };
        metrics.fill_build_version(Some("0.1.0"));
        assert_eq!(metrics.build_version, "0.4.0");

        let mut metrics = StructuredMetrics::default();
        metrics.fill_build_version(None);
        assert_eq!(metrics.build_version, "");

        let mut metrics = StructuredMetrics::default();
        metrics.fill_build_version(Some(&format!("<b>v1</b>\n{}", "x".repeat(500))));
        assert!(metrics.build_version.starts_with("<b>v1</b>x"));
        assert_eq!(metrics.build_version.len(), 128);
    }

    #[test]
    fn paid_economics_schema_matches_node_exporter() {
        let metrics: StructuredMetrics = serde_json::from_str(
            r#"{
                "cumulativeAuthorizedRevenueUsd": 2.5,
                "cumulativeCostUsd": 1.0,
                "cumulativeMaximumCostUsd": 1.2,
                "profitableCount": 7,
                "unprofitableCount": 3
            }"#,
        )
        .expect("node metrics fixture must decode");

        assert_eq!(metrics.cumulative_authorized_revenue_usd, 2.5);
        assert_eq!(metrics.cumulative_cost_usd, 1.0);
        assert_eq!(metrics.cumulative_maximum_cost_usd, 1.2);
        assert_eq!(metrics.profitable_count, 7.0);
        assert_eq!(metrics.unprofitable_count, 3.0);
    }
}
