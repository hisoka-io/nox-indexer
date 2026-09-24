//! Lifetime metric continuity across node restarts.
//!
//! Node counters live in the node process and reset to zero whenever its
//! container restarts. The indexer scrapes those counters every 5s, so it holds
//! the pre-restart values that the node itself is about to forget. This module
//! banks them: on detecting a restart it folds the previous incarnation's final
//! reading into a running total, then adds that total to every subsequent
//! reading. The result is a continuous lifetime figure for the dashboard.
//!
//! Only monotonically increasing counters are offset. Gauges (queue depths,
//! active peers, latency percentiles, health) reflect instantaneous state and
//! are passed through untouched.

use serde::{Deserialize, Serialize};

use super::metrics::StructuredMetrics;

/// Defines `CumulativeMetrics` over the monotonic fields of [`StructuredMetrics`]
/// and keeps snapshot/accumulate/apply in lockstep, so a field can never be
/// banked but not re-applied (or vice versa).
macro_rules! cumulative_metrics {
    ($($field:ident),* $(,)?) => {
        /// Monotonic counters that must survive a node restart.
        #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
        #[serde(default)]
        pub struct CumulativeMetrics {
            $(pub $field: f64,)*
        }

        impl CumulativeMetrics {
            /// Capture the cumulative fields of a scraped reading.
            pub fn snapshot(m: &StructuredMetrics) -> Self {
                Self { $($field: m.$field,)* }
            }

            /// Fold another set of totals into this one.
            pub fn accumulate(&mut self, other: &Self) {
                $(self.$field += other.$field;)*
            }

            /// Add the banked totals onto a live reading, in place.
            pub fn apply(&self, m: &mut StructuredMetrics) {
                $(m.$field += self.$field;)*
            }

            /// True when nothing has been banked yet.
            pub fn is_zero(&self) -> bool {
                true $(&& self.$field == 0.0)*
            }
        }
    };
}

cumulative_metrics!(
    uptime_seconds,
    packets_received,
    packets_forwarded,
    cover_loop_generated,
    cover_drop_generated,
    sphinx_errors,
    replay_duplicate,
    cumulative_authorized_revenue_usd,
    cumulative_cost_usd,
    cumulative_maximum_cost_usd,
    exit_payloads_dispatched,
    exit_echo,
    exit_http,
    exit_rpc,
    exit_broadcast,
    exit_ethereum,
    exit_traffic,
    profitable_count,
    unprofitable_count,
    eth_transactions_submitted,
    egress_forwarded,
    egress_exited,
);

impl CumulativeMetrics {
    /// The same totals keyed like the node exporter's JSON (`packetsReceived`, ...),
    /// matching the per-node `metrics` objects served by `/v1/state`.
    pub fn to_camel_case_json(&self) -> serde_json::Map<String, serde_json::Value> {
        let Ok(serde_json::Value::Object(fields)) = serde_json::to_value(self) else {
            return serde_json::Map::new();
        };
        fields
            .into_iter()
            .map(|(key, value)| (snake_to_camel(&key), value))
            .collect()
    }
}

fn snake_to_camel(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    let mut upper = false;
    for c in key.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

/// Lifetime totals across every node ever scraped, whatever its registration
/// status: each node's banked prior incarnations plus the latest reading of its
/// current one. For a node still being scraped this equals its live `metrics`
/// value; for a deregistered node it is the last lifetime value seen.
pub fn network_totals<'a>(offsets: impl IntoIterator<Item = &'a NodeOffset>) -> CumulativeMetrics {
    let mut total = CumulativeMetrics::default();
    for offset in offsets {
        total.accumulate(&offset.banked);
        total.accumulate(&offset.last_raw);
    }
    total
}

/// Per-node restart bookkeeping.
#[derive(Clone, Debug, Default)]
pub struct NodeOffset {
    /// `nodeStartTime` of the incarnation currently being scraped. Zero until
    /// the first reading is seen.
    pub last_node_start_time: i64,
    /// Sum of every prior incarnation's final reading.
    pub banked: CumulativeMetrics,
    /// Most recent reading of the current incarnation. Banked on next restart.
    pub last_raw: CumulativeMetrics,
    /// Set when state changed and has not yet been flushed to Postgres.
    pub dirty: bool,
}

impl NodeOffset {
    /// Record a fresh reading, banking the previous incarnation if the node restarted.
    ///
    /// A restart is detected by `nodeStartTime` changing. Older nodes that do not
    /// report that field fall back to `uptime_seconds` going backwards, which is
    /// only possible across a process restart.
    ///
    /// Returns `true` if a restart was detected and totals were banked.
    pub fn observe(&mut self, m: &StructuredMetrics) -> bool {
        let start = m.node_start_time as i64;

        let restarted = if start > 0 && self.last_node_start_time > 0 {
            start != self.last_node_start_time
        } else {
            // No usable start time: fall back to the uptime counter rewinding.
            self.last_node_start_time != 0 && m.uptime_seconds < self.last_raw.uptime_seconds
        };

        if restarted {
            let previous = self.last_raw.clone();
            self.banked.accumulate(&previous);
            self.last_raw = CumulativeMetrics::default();
            self.dirty = true;
        }

        // Track the incarnation even on first sight, so the next restart is detectable.
        if start > 0 && self.last_node_start_time != start {
            self.last_node_start_time = start;
            self.dirty = true;
        } else if start <= 0 && self.last_node_start_time == 0 {
            // Mark as seen so the uptime-rewind fallback can arm itself.
            self.last_node_start_time = -1;
            self.dirty = true;
        }

        self.last_raw = CumulativeMetrics::snapshot(m);
        restarted
    }

    /// Add banked totals onto a live reading.
    pub fn apply(&self, m: &mut StructuredMetrics) {
        if !self.banked.is_zero() {
            self.banked.apply(m);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(start: i64, uptime: f64, packets: f64) -> StructuredMetrics {
        StructuredMetrics {
            node_start_time: start as f64,
            uptime_seconds: uptime,
            packets_received: packets,
            ..Default::default()
        }
    }

    #[test]
    fn first_reading_banks_nothing() {
        let mut off = NodeOffset::default();
        assert!(!off.observe(&reading(1000, 50.0, 500.0)));
        assert!(off.banked.is_zero());
    }

    #[test]
    fn steady_state_does_not_bank() {
        let mut off = NodeOffset::default();
        off.observe(&reading(1000, 50.0, 500.0));
        assert!(!off.observe(&reading(1000, 60.0, 600.0)));
        assert!(off.banked.is_zero());
    }

    #[test]
    fn restart_banks_previous_incarnation() {
        let mut off = NodeOffset::default();
        off.observe(&reading(1000, 50.0, 500.0));
        off.observe(&reading(1000, 99.0, 900.0));

        // Container restarts: new start time, counters back near zero.
        assert!(off.observe(&reading(2000, 1.0, 10.0)));
        assert_eq!(off.banked.packets_received, 900.0);
        assert_eq!(off.banked.uptime_seconds, 99.0);

        let mut live = reading(2000, 1.0, 10.0);
        off.apply(&mut live);
        assert_eq!(live.packets_received, 910.0);
        assert_eq!(live.uptime_seconds, 100.0);
    }

    #[test]
    fn restart_banks_maximum_authorized_cost() {
        let mut offset = NodeOffset::default();
        offset.observe(&StructuredMetrics {
            node_start_time: 1_000.0,
            cumulative_maximum_cost_usd: 1.2,
            ..Default::default()
        });
        assert!(offset.observe(&StructuredMetrics {
            node_start_time: 2_000.0,
            cumulative_maximum_cost_usd: 0.3,
            ..Default::default()
        }));

        let mut live = StructuredMetrics {
            cumulative_maximum_cost_usd: 0.3,
            ..Default::default()
        };
        offset.apply(&mut live);
        assert_eq!(live.cumulative_maximum_cost_usd, 1.5);
    }

    #[test]
    fn multiple_restarts_accumulate() {
        let mut off = NodeOffset::default();
        off.observe(&reading(1000, 100.0, 1000.0));
        off.observe(&reading(2000, 5.0, 50.0));
        off.observe(&reading(2000, 200.0, 2000.0));
        off.observe(&reading(3000, 1.0, 7.0));

        assert_eq!(off.banked.packets_received, 3000.0);

        let mut live = reading(3000, 1.0, 7.0);
        off.apply(&mut live);
        assert_eq!(live.packets_received, 3007.0);
    }

    #[test]
    fn uptime_rewind_detects_restart_without_start_time() {
        let mut off = NodeOffset::default();
        off.observe(&reading(0, 100.0, 1000.0));
        off.observe(&reading(0, 150.0, 1500.0));

        assert!(off.observe(&reading(0, 2.0, 20.0)));
        assert_eq!(off.banked.packets_received, 1500.0);
    }

    #[test]
    fn network_totals_match_live_values_and_keep_departed_nodes() {
        // Node A restarted once and is still scraped.
        let mut a = NodeOffset::default();
        a.observe(&reading(1000, 100.0, 1_000.0));
        a.observe(&reading(2000, 10.0, 50.0));
        let mut live_a = reading(2000, 10.0, 50.0);
        a.apply(&mut live_a);

        // Node B was deregistered: no longer scraped, last reading retained.
        let mut b = NodeOffset::default();
        b.observe(&reading(3000, 40.0, 400.0));

        let totals = network_totals([&a, &b]);
        assert_eq!(totals.packets_received, live_a.packets_received + 400.0);
        assert_eq!(totals.packets_received, 1_450.0);
        assert_eq!(totals.uptime_seconds, 150.0);

        let json = totals.to_camel_case_json();
        assert_eq!(json["packetsReceived"], serde_json::json!(1_450.0));
        assert!(json.contains_key("cumulativeAuthorizedRevenueUsd"));
        assert!(!json.contains_key("packets_received"));
    }

    #[test]
    fn gauges_are_never_offset() {
        let mut off = NodeOffset::default();
        off.observe(&StructuredMetrics {
            node_start_time: 1000.0,
            packets_received: 500.0,
            active_peers: 9.0,
            mix_queue_depth: 3.0,
            latency_p50: 43.0,
            ..Default::default()
        });
        off.observe(&StructuredMetrics {
            node_start_time: 2000.0,
            packets_received: 1.0,
            active_peers: 8.0,
            mix_queue_depth: 2.0,
            latency_p50: 51.0,
            ..Default::default()
        });

        let mut live = StructuredMetrics {
            node_start_time: 2000.0,
            packets_received: 1.0,
            active_peers: 8.0,
            mix_queue_depth: 2.0,
            latency_p50: 51.0,
            ..Default::default()
        };
        off.apply(&mut live);

        assert_eq!(live.packets_received, 501.0);
        // Instantaneous state passes through untouched.
        assert_eq!(live.active_peers, 8.0);
        assert_eq!(live.mix_queue_depth, 2.0);
        assert_eq!(live.latency_p50, 51.0);
    }
}
