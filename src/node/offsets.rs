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
//!
//! Counters are self-reported by registrants, so a reading is never taken at
//! face value. Each counter may only move forward, and its growth is limited
//! by a per-counter allowance: the allowance fills at the counter's rate
//! ceiling for every second between readings, up to [`MAX_READING_WINDOW`]
//! worth, and each accepted increase spends it. Counters that the node only
//! refreshes periodically (uptime moves in 30s steps) therefore pass unchanged,
//! while sustained growth can never exceed the rate ceiling. A counter that
//! goes backwards without a restart is held at its last accepted value, and a
//! jump larger than the allowance is accepted in allowance-sized steps. A node
//! that really is ahead (first sight, or after an indexer outage) therefore
//! catches up over a few scrapes.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::metrics::StructuredMetrics;

/// Sustained increase a counter may show per second of wall time. These are
/// far above anything the network has produced (live nodes run below one
/// packet per second). They stop absurd values, not plausible ones: they are a
/// sanity bound on the totals, not a check that the traffic was real.
const PACKETS_PER_SEC: f64 = 1_000.0;
const SUBMISSIONS_PER_SEC: f64 = 10.0;
const USD_PER_SEC: f64 = 1.0;
/// Above 1 so uptime can catch up after an indexer outage.
const UPTIME_PER_SEC: f64 = 2.0;

/// Time credited to a reading with no earlier reading in this process (a node
/// seen for the first time, or the first scrape after an indexer restart).
pub const FIRST_READING_WINDOW: Duration = Duration::from_secs(60);
/// Bounds on the time credited between two readings. The scrape interval is 5s.
/// The upper bound is also the most allowance a counter can hold, so a burst
/// never exceeds this much time at the rate ceiling.
const MIN_READING_WINDOW: Duration = Duration::from_secs(1);
const MAX_READING_WINDOW: Duration = Duration::from_secs(60);

/// Defines `CumulativeMetrics` over the monotonic fields of [`StructuredMetrics`]
/// and keeps snapshot/accumulate/apply in lockstep, so a field can never be
/// banked but not re-applied (or vice versa). Each field carries its rate
/// ceiling (see [`CumulativeMetrics::advance_toward`]).
macro_rules! cumulative_metrics {
    ($($field:ident: $ceiling:expr),* $(,)?) => {
        /// Monotonic counters that must survive a node restart.
        #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
        #[serde(default)]
        pub struct CumulativeMetrics {
            $(pub $field: f64,)*
        }

        /// Largest increase per second accepted for each counter.
        const RATE_CEILINGS: CumulativeMetrics = CumulativeMetrics { $($field: $ceiling,)* };

        impl CumulativeMetrics {
            /// Capture the cumulative fields of a scraped reading.
            pub fn snapshot(m: &StructuredMetrics) -> Self {
                Self { $($field: m.$field,)* }
            }

            /// Fold another set of totals into this one.
            pub fn accumulate(&mut self, other: &Self) {
                $(self.$field += other.$field;)*
            }

            /// Replace the cumulative fields of a live reading with these totals.
            pub fn write_into(&self, m: &mut StructuredMetrics) {
                $(m.$field = self.$field;)*
            }

            /// True when nothing has been banked yet.
            pub fn is_zero(&self) -> bool {
                true $(&& self.$field == 0.0)*
            }

            /// Move these accepted values toward a reported reading, never
            /// backwards. Each field's `allowance` first grows by
            /// `RATE_CEILINGS * window_secs` (up to `MAX_READING_WINDOW` worth),
            /// and every accepted increase is taken out of it.
            /// Returns true if any reported value was not taken as is.
            fn advance_toward(
                &mut self,
                reported: &Self,
                allowance: &mut Self,
                window_secs: f64,
            ) -> bool {
                let max_window_secs = MAX_READING_WINDOW.as_secs() as f64;
                let mut adjusted = false;
                $(adjusted |= advance_counter(
                    &mut self.$field,
                    reported.$field,
                    &mut allowance.$field,
                    RATE_CEILINGS.$field * window_secs,
                    RATE_CEILINGS.$field * max_window_secs,
                );)*
                adjusted
            }
        }
    };
}

cumulative_metrics!(
    uptime_seconds: UPTIME_PER_SEC,
    packets_received: PACKETS_PER_SEC,
    packets_forwarded: PACKETS_PER_SEC,
    cover_loop_generated: PACKETS_PER_SEC,
    cover_drop_generated: PACKETS_PER_SEC,
    sphinx_errors: PACKETS_PER_SEC,
    replay_duplicate: PACKETS_PER_SEC,
    cumulative_authorized_revenue_usd: USD_PER_SEC,
    cumulative_cost_usd: USD_PER_SEC,
    cumulative_maximum_cost_usd: USD_PER_SEC,
    exit_payloads_dispatched: PACKETS_PER_SEC,
    exit_echo: PACKETS_PER_SEC,
    exit_http: PACKETS_PER_SEC,
    exit_rpc: PACKETS_PER_SEC,
    exit_broadcast: PACKETS_PER_SEC,
    exit_ethereum: SUBMISSIONS_PER_SEC,
    exit_traffic: PACKETS_PER_SEC,
    profitable_count: SUBMISSIONS_PER_SEC,
    unprofitable_count: SUBMISSIONS_PER_SEC,
    eth_transactions_submitted: SUBMISSIONS_PER_SEC,
    egress_forwarded: PACKETS_PER_SEC,
    egress_exited: PACKETS_PER_SEC,
);

/// Advance one accepted counter toward its reported value. `allowance` is
/// first refilled by `refill` (capped at `capacity`), then spent on the
/// accepted increase. Returns true when the report was not taken as is (not a
/// finite number, lower than the accepted value, or above the allowance).
fn advance_counter(
    accepted: &mut f64,
    reported: f64,
    allowance: &mut f64,
    refill: f64,
    capacity: f64,
) -> bool {
    *allowance = (*allowance + refill).min(capacity);
    if !reported.is_finite() || reported < *accepted {
        return true;
    }
    let step = reported - *accepted;
    if step > *allowance {
        *accepted += *allowance;
        *allowance = 0.0;
        true
    } else {
        *accepted = reported;
        *allowance -= step;
        false
    }
}

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
    /// Accepted counters of the current incarnation (the latest reading after
    /// the checks in [`CumulativeMetrics::advance_toward`]). Banked on next restart.
    pub last_raw: CumulativeMetrics,
    /// Set when state changed and has not yet been flushed to Postgres.
    pub dirty: bool,
    /// When the previous reading was observed in this process. Not persisted.
    pub last_observed_at: Option<Instant>,
    /// Growth each counter may still take (see
    /// [`CumulativeMetrics::advance_toward`]). Not persisted: a fresh process
    /// starts empty and credits [`FIRST_READING_WINDOW`] on the first reading.
    pub allowance: CumulativeMetrics,
    /// Whether the previous reading was adjusted, so callers can log only when
    /// a node starts reporting out-of-bounds values. Not persisted.
    pub adjusting: bool,
}

/// Outcome of [`NodeOffset::observe`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Observation {
    /// A restart was detected and the previous incarnation was banked.
    pub restarted: bool,
    /// At least one counter was held back or capped instead of taken as reported.
    pub adjusted: bool,
}

impl NodeOffset {
    /// Record a fresh reading taken at `now`, banking the previous incarnation
    /// if the node restarted.
    ///
    /// A restart is detected by `nodeStartTime` changing. Older nodes that do not
    /// report that field fall back to `uptime_seconds` going backwards, which is
    /// only possible across a process restart.
    ///
    /// The reported counters are then checked against the accepted ones (see
    /// [`CumulativeMetrics::advance_toward`]): they may not move backwards within
    /// an incarnation, and may not grow by more than the allowance built up at
    /// their rate ceilings. A new incarnation starts from zero and the allowance
    /// carries over, so the ceilings also hold across restarts.
    pub fn observe(&mut self, m: &StructuredMetrics, now: Instant) -> Observation {
        let start = m.node_start_time as i64;

        let restarted = if start > 0 && self.last_node_start_time > 0 {
            start != self.last_node_start_time
        } else {
            // No usable start time: fall back to the uptime counter rewinding.
            self.last_node_start_time != 0 && m.uptime_seconds < self.last_raw.uptime_seconds
        };

        if restarted {
            let previous = std::mem::take(&mut self.last_raw);
            self.banked.accumulate(&previous);
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

        let window = self
            .last_observed_at
            .map_or(FIRST_READING_WINDOW, |previous| {
                now.saturating_duration_since(previous)
            })
            .clamp(MIN_READING_WINDOW, MAX_READING_WINDOW);
        self.last_observed_at = Some(now);

        // Whole seconds keep capped counts whole.
        let window_secs = window.as_secs() as f64;
        let adjusted = self.last_raw.advance_toward(
            &CumulativeMetrics::snapshot(m),
            &mut self.allowance,
            window_secs,
        );
        Observation {
            restarted,
            adjusted,
        }
    }

    /// Replace the counters of a live reading with this node's lifetime totals:
    /// banked prior incarnations plus the accepted current ones.
    pub fn apply(&self, m: &mut StructuredMetrics) {
        let mut lifetime = self.banked.clone();
        lifetime.accumulate(&self.last_raw);
        lifetime.write_into(m);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feeds readings to one node's offset on the 5s scrape cadence.
    struct Scraper {
        offset: NodeOffset,
        now: Instant,
    }

    impl Scraper {
        fn new() -> Self {
            Self {
                offset: NodeOffset::default(),
                now: Instant::now(),
            }
        }

        fn observe(&mut self, m: &StructuredMetrics) -> Observation {
            self.observe_after(Duration::from_secs(5), m)
        }

        fn observe_after(&mut self, gap: Duration, m: &StructuredMetrics) -> Observation {
            self.now += gap;
            self.offset.observe(m, self.now)
        }

        fn live(&self, m: &StructuredMetrics) -> StructuredMetrics {
            let mut live = m.clone();
            self.offset.apply(&mut live);
            live
        }
    }

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
        let mut s = Scraper::new();
        let seen = s.observe(&reading(1000, 50.0, 500.0));
        assert!(!seen.restarted && !seen.adjusted);
        assert!(s.offset.banked.is_zero());
    }

    #[test]
    fn steady_state_does_not_bank() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 50.0, 500.0));
        let seen = s.observe(&reading(1000, 55.0, 600.0));
        assert_eq!(seen, Observation::default());
        assert!(s.offset.banked.is_zero());
    }

    #[test]
    fn restart_banks_previous_incarnation() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 50.0, 500.0));
        s.observe(&reading(1000, 55.0, 900.0));

        // Container restarts: new start time, counters back near zero.
        assert!(s.observe(&reading(2000, 1.0, 10.0)).restarted);
        assert_eq!(s.offset.banked.packets_received, 900.0);
        assert_eq!(s.offset.banked.uptime_seconds, 55.0);

        let live = s.live(&reading(2000, 1.0, 10.0));
        assert_eq!(live.packets_received, 910.0);
        assert_eq!(live.uptime_seconds, 56.0);
    }

    #[test]
    fn restart_banks_maximum_authorized_cost() {
        let mut s = Scraper::new();
        s.observe(&StructuredMetrics {
            node_start_time: 1_000.0,
            cumulative_maximum_cost_usd: 1.2,
            ..Default::default()
        });
        assert!(
            s.observe(&StructuredMetrics {
                node_start_time: 2_000.0,
                cumulative_maximum_cost_usd: 0.3,
                ..Default::default()
            })
            .restarted
        );

        let live = s.live(&StructuredMetrics {
            cumulative_maximum_cost_usd: 0.3,
            ..Default::default()
        });
        assert_eq!(live.cumulative_maximum_cost_usd, 1.5);
    }

    #[test]
    fn multiple_restarts_accumulate() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 50.0, 1000.0));
        s.observe(&reading(2000, 5.0, 50.0));
        s.observe(&reading(2000, 10.0, 2000.0));
        s.observe(&reading(3000, 1.0, 7.0));

        assert_eq!(s.offset.banked.packets_received, 3000.0);
        assert_eq!(s.live(&reading(3000, 1.0, 7.0)).packets_received, 3007.0);
    }

    #[test]
    fn uptime_rewind_detects_restart_without_start_time() {
        let mut s = Scraper::new();
        s.observe(&reading(0, 50.0, 1000.0));
        s.observe(&reading(0, 55.0, 1500.0));

        assert!(s.observe(&reading(0, 2.0, 20.0)).restarted);
        assert_eq!(s.offset.banked.packets_received, 1500.0);
    }

    #[test]
    fn network_totals_match_live_values_and_keep_departed_nodes() {
        // Node A restarted once and is still scraped.
        let mut a = Scraper::new();
        a.observe(&reading(1000, 100.0, 1_000.0));
        a.observe(&reading(2000, 10.0, 50.0));
        let live_a = a.live(&reading(2000, 10.0, 50.0));

        // Node B was deregistered: no longer scraped, last reading retained.
        let mut b = Scraper::new();
        b.observe(&reading(3000, 40.0, 400.0));

        let totals = network_totals([&a.offset, &b.offset]);
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
        let mut s = Scraper::new();
        s.observe(&StructuredMetrics {
            node_start_time: 1000.0,
            packets_received: 500.0,
            active_peers: 9.0,
            mix_queue_depth: 3.0,
            latency_p50: 43.0,
            ..Default::default()
        });
        let current = StructuredMetrics {
            node_start_time: 2000.0,
            packets_received: 1.0,
            active_peers: 8.0,
            mix_queue_depth: 2.0,
            latency_p50: 51.0,
            ..Default::default()
        };
        s.observe(&current);
        let live = s.live(&current);

        assert_eq!(live.packets_received, 501.0);
        // Instantaneous state passes through untouched.
        assert_eq!(live.active_peers, 8.0);
        assert_eq!(live.mix_queue_depth, 2.0);
        assert_eq!(live.latency_p50, 51.0);
    }

    #[test]
    fn a_jump_beyond_the_rate_ceiling_is_accepted_in_capped_steps() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 10.0, 100.0));

        // The whole allowance (60s at 1_000/s) can be spent at once...
        let inflated = reading(1000, 15.0, 1.0e12);
        let seen = s.observe(&inflated);
        assert!(seen.adjusted && !seen.restarted);
        assert_eq!(s.offset.last_raw.packets_received, 60_100.0);
        assert_eq!(s.live(&inflated).packets_received, 60_100.0);
        // Values inside their allowance are still taken as reported.
        assert_eq!(s.offset.last_raw.uptime_seconds, 15.0);

        // ...after which each 5s reading adds at most 5_000.
        s.observe(&inflated);
        assert_eq!(s.offset.last_raw.packets_received, 65_100.0);
        s.observe(&inflated);
        assert_eq!(s.offset.last_raw.packets_received, 70_100.0);
    }

    #[test]
    fn a_node_that_is_really_ahead_catches_up() {
        let mut s = Scraper::new();
        // First sight of a node that has been running for a while: 60s worth
        // of the packet ceiling is credited, the rest arrives on later scrapes.
        s.observe(&reading(1000, 100.0, 70_000.0));
        assert_eq!(s.offset.last_raw.packets_received, 60_000.0);
        assert_eq!(s.offset.last_raw.uptime_seconds, 100.0);

        assert!(s.observe(&reading(1000, 105.0, 70_010.0)).adjusted);
        assert!(s.observe(&reading(1000, 110.0, 70_020.0)).adjusted);
        assert_eq!(s.offset.last_raw.packets_received, 70_000.0);
        let caught_up = s.observe(&reading(1000, 115.0, 70_030.0));
        assert!(!caught_up.adjusted);
        assert_eq!(s.offset.last_raw.packets_received, 70_030.0);
    }

    #[test]
    fn capped_counts_stay_whole() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 10.0, 0.0));
        s.observe(&reading(1000, 15.0, 1.0e9));
        assert_eq!(s.offset.last_raw.packets_received, 60_000.0);
        s.observe_after(Duration::from_millis(5_700), &reading(1000, 20.0, 1.0e9));
        assert_eq!(s.offset.last_raw.packets_received, 65_000.0);
    }

    #[test]
    fn the_credited_window_is_bounded() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 10.0, 0.0));
        // An hour without readings still credits at most 60s of growth.
        s.observe_after(Duration::from_secs(3_600), &reading(1000, 3_610.0, 1.0e9));
        assert_eq!(s.offset.last_raw.packets_received, 60_000.0);
        assert_eq!(s.offset.last_raw.uptime_seconds, 130.0);
    }

    #[test]
    fn counters_never_move_backwards_within_an_incarnation() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 10.0, 500.0));
        let seen = s.observe(&reading(1000, 15.0, 20.0));
        assert!(seen.adjusted && !seen.restarted);
        assert_eq!(s.offset.last_raw.packets_received, 500.0);

        // Recovering above the held value adds only the new growth.
        s.observe(&reading(1000, 20.0, 600.0));
        assert_eq!(s.offset.last_raw.packets_received, 600.0);
        assert!(s.offset.banked.is_zero());
    }

    #[test]
    fn negative_or_non_finite_counters_are_ignored() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 10.0, 500.0));
        assert!(s.observe(&reading(1000, 15.0, -1.0e12)).adjusted);
        assert!(s.observe(&reading(1000, 20.0, f64::NAN)).adjusted);
        assert!(s.observe(&reading(1000, 25.0, f64::INFINITY)).adjusted);
        assert_eq!(s.offset.last_raw.packets_received, 500.0);
        assert!(s.offset.banked.is_zero());
    }

    #[test]
    fn ceilings_hold_across_restarts() {
        let mut s = Scraper::new();
        for start in 1..=10 {
            s.observe(&reading(1000 + start, 1.0, 1.0e12));
        }
        let totals = network_totals([&s.offset]);
        // 60s on first sight, then 5s per reading.
        assert_eq!(totals.packets_received, 60_000.0 + 9.0 * 5_000.0);
    }

    #[test]
    fn money_counters_have_their_own_ceiling() {
        let mut s = Scraper::new();
        s.observe(&StructuredMetrics {
            node_start_time: 1000.0,
            ..Default::default()
        });
        s.observe(&StructuredMetrics {
            node_start_time: 1000.0,
            cumulative_authorized_revenue_usd: 1.0e6,
            profitable_count: 1.0e6,
            ..Default::default()
        });
        // 60s worth of allowance: $60 and 600 submissions.
        assert_eq!(s.offset.last_raw.cumulative_authorized_revenue_usd, 60.0);
        assert_eq!(s.offset.last_raw.profitable_count, 600.0);
    }

    /// Readings every 5s from a node that refreshes uptime every 30s and
    /// forwards a little traffic, as live nodes do.
    fn stepped_uptime_readings(start_uptime: f64, scrapes: u64) -> Vec<StructuredMetrics> {
        (1..=scrapes)
            .map(|i| {
                let elapsed = (i * 5) as f64;
                let uptime = start_uptime + (elapsed / 30.0).floor() * 30.0;
                reading(1000, uptime, 1_000.0 + elapsed * 2.0)
            })
            .collect()
    }

    #[test]
    fn uptime_refreshed_in_30s_steps_is_taken_as_reported() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 0.0, 1_000.0));
        for m in stepped_uptime_readings(0.0, 720) {
            let seen = s.observe(&m);
            assert_eq!(seen, Observation::default(), "uptime {}", m.uptime_seconds);
            assert_eq!(s.offset.last_raw.uptime_seconds, m.uptime_seconds);
        }
    }

    #[test]
    fn uptime_refreshed_in_30s_steps_is_taken_as_reported_after_an_indexer_restart() {
        // Offsets loaded from Postgres, node kept running through the deploy.
        let mut s = Scraper::new();
        s.offset = NodeOffset {
            last_node_start_time: 1000,
            last_raw: CumulativeMetrics {
                uptime_seconds: 681_330.0,
                packets_received: 1_000.0,
                ..Default::default()
            },
            ..NodeOffset::default()
        };
        for m in stepped_uptime_readings(681_330.0, 720) {
            assert_eq!(s.observe(&m), Observation::default());
        }
        assert_eq!(s.offset.last_raw.uptime_seconds, 681_330.0 + 3_600.0);
    }

    #[test]
    fn sustained_growth_stays_within_the_rate_ceiling() {
        let mut s = Scraper::new();
        s.observe(&reading(1000, 0.0, 0.0));
        // Uptime reported 6x faster than wall time for 10 minutes.
        let mut reported = 0.0;
        for _ in 0..120 {
            reported += 30.0;
            s.observe(&reading(1000, reported, 0.0));
        }
        // At most the full allowance (60s at 2/s) plus 2/s for 600s, against
        // 3_600 reported.
        let accepted = s.offset.last_raw.uptime_seconds;
        assert!(accepted <= 120.0 + 1_200.0, "{accepted}");
        assert!(accepted >= 1_200.0, "{accepted}");
    }

    #[test]
    fn persisted_totals_survive_an_indexer_restart_unchanged() {
        // Offsets loaded from Postgres: no in-process reading yet.
        let mut offset = NodeOffset {
            last_node_start_time: 1000,
            banked: CumulativeMetrics {
                packets_received: 4_000_000.0,
                ..Default::default()
            },
            last_raw: CumulativeMetrics {
                packets_received: 300_000.0,
                uptime_seconds: 600_000.0,
                ..Default::default()
            },
            ..NodeOffset::default()
        };
        let before = network_totals([&offset]);

        // The node kept running through the indexer deploy.
        let seen = offset.observe(&reading(1000, 600_090.0, 300_050.0), Instant::now());
        assert_eq!(seen, Observation::default());
        let after = network_totals([&offset]);
        assert_eq!(after.packets_received, before.packets_received + 50.0);
        assert_eq!(after.uptime_seconds, before.uptime_seconds + 90.0);
    }
}
