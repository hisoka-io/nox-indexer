//! Retry, backoff and adaptive `eth_getLogs` chunk sizing.
//!
//! Free public RPCs rate-limit (HTTP/JSON-RPC 429) and cap `eth_getLogs` block
//! ranges at provider-specific sizes (10k, 50k, ...). A single failed request
//! must never abort a multi-million-block replay, so every chunk is retried with
//! exponential backoff plus jitter, and the chunk size shrinks on range or rate
//! errors and grows back after a run of successes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// How an RPC error string should influence the retry strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcErrorKind {
    /// The provider rejected the block range; `hint` is the limit it advertised.
    RangeTooLarge { hint: Option<u64> },
    /// 429 / "too many requests" / quota exhaustion.
    RateLimited,
    /// Anything else (timeouts, transport errors, 5xx, missing state).
    Transient,
}

/// Classify an RPC error message. Messages from several endpoints may be joined
/// together; a range complaint from any of them wins because shrinking the
/// chunk is the only remedy that helps with it.
pub fn classify_rpc_error(message: &str) -> RpcErrorKind {
    let lower = message.to_lowercase();
    const RANGE_MARKERS: [&str; 12] = [
        "block range",
        "ranges over",
        "range is too",
        "range too",
        "range exceeds",
        "exceed maximum",
        "limited to a",
        "too many blocks",
        "more than 10000 results",
        "query returned more than",
        "response size exceeded",
        "log response size",
    ];
    if RANGE_MARKERS.iter().any(|marker| lower.contains(marker)) {
        return RpcErrorKind::RangeTooLarge {
            hint: range_hint(&lower),
        };
    }
    const RATE_MARKERS: [&str; 7] = [
        "429",
        "too many requests",
        "rate limit",
        "rate-limit",
        "ratelimit",
        "exceeded the quota",
        "capacity exceeded",
    ];
    if RATE_MARKERS.iter().any(|marker| lower.contains(marker)) {
        return RpcErrorKind::RateLimited;
    }
    RpcErrorKind::Transient
}

/// Extract the block-range limit a provider advertises, e.g.
/// `exceed maximum block range: 50000` or `ranges over 10000 blocks`.
fn range_hint(lower: &str) -> Option<u64> {
    const PREFIXES: [&str; 4] = [
        "block range: ",
        "ranges over ",
        "limited to a ",
        "range of ",
    ];
    PREFIXES.iter().find_map(|prefix| {
        let start = lower.find(prefix)? + prefix.len();
        let digits: String = lower[start..]
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == ',')
            .filter(char::is_ascii_digit)
            .collect();
        digits.parse::<u64>().ok().filter(|n| *n > 0)
    })
}

/// Exponential backoff with "equal jitter": the delay for `attempt` (1-based) is
/// `min(cap, base * 2^(attempt-1))`, of which the upper half is randomised.
/// `entropy` supplies the randomness so the function stays deterministic in tests.
pub fn backoff_delay(attempt: u32, base: Duration, cap: Duration, entropy: u64) -> Duration {
    let exponent = attempt.saturating_sub(1).min(20);
    let ceiling = base.checked_mul(1_u32 << exponent).unwrap_or(cap).min(cap);
    let half = ceiling / 2;
    let span_ms = u64::try_from(half.as_millis()).unwrap_or(u64::MAX);
    let jitter_ms = if span_ms == 0 {
        0
    } else {
        entropy % (span_ms + 1)
    };
    half + Duration::from_millis(jitter_ms)
}

/// Backoff with process-local jitter.
pub fn jittered_backoff(attempt: u32, base: Duration, cap: Duration) -> Duration {
    backoff_delay(attempt, base, cap, next_entropy())
}

/// Cheap non-cryptographic entropy for jitter (splitmix64 over the clock and a
/// counter), avoiding a dependency on a random-number crate.
fn next_entropy() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let mut z = COUNTER
        .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
        .wrapping_add(nanos);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Successes in a row required before the chunk size doubles. Two keeps the
/// size stable even when a provider rate-limits every third request (halve
/// once, double once), instead of ratcheting down to the floor.
const GROW_AFTER_SUCCESSES: u32 = 2;

/// Adaptive `eth_getLogs` block-range size.
///
/// Range errors lower a learned ceiling (the provider's real limit) so the size
/// does not oscillate back into the same error; rate-limit errors only halve the
/// current size, since the limit there is on request rate rather than span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveChunk {
    size: u64,
    min: u64,
    ceiling: u64,
    streak: u32,
}

impl AdaptiveChunk {
    pub fn new(initial: u64, min: u64, max: u64) -> Self {
        let min = min.max(1);
        let max = max.max(min);
        Self {
            size: initial.clamp(min, max),
            min,
            ceiling: max,
            streak: 0,
        }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn on_success(&mut self) {
        self.streak += 1;
        if self.streak >= GROW_AFTER_SUCCESSES && self.size < self.ceiling {
            self.size = self.size.saturating_mul(2).min(self.ceiling);
            self.streak = 0;
        }
    }

    /// `failed_span` is the number of blocks in the request that was rejected.
    pub fn on_range_error(&mut self, failed_span: u64, hint: Option<u64>) {
        let halved = failed_span / 2;
        let next = match hint {
            Some(limit) if limit < failed_span => limit,
            _ => halved,
        };
        self.size = next.max(self.min);
        self.ceiling = self.ceiling.min(self.size).max(self.min);
        self.streak = 0;
    }

    pub fn on_rate_limited(&mut self) {
        self.size = (self.size / 2).max(self.min);
        self.streak = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_provider_range_errors_with_hints() {
        assert_eq!(
            classify_rpc_error(
                "(code: -32701, message: exceed maximum block range: 50000, data: None)"
            ),
            RpcErrorKind::RangeTooLarge { hint: Some(50_000) }
        );
        assert_eq!(
            classify_rpc_error("ranges over 10000 blocks are not supported on free plan"),
            RpcErrorKind::RangeTooLarge { hint: Some(10_000) }
        );
        assert_eq!(
            classify_rpc_error("eth_getLogs is limited to a 10,000 range"),
            RpcErrorKind::RangeTooLarge { hint: Some(10_000) }
        );
        assert_eq!(
            classify_rpc_error("query returned more than 10000 results"),
            RpcErrorKind::RangeTooLarge { hint: None }
        );
    }

    #[test]
    fn classifies_rate_limits_and_transient_errors() {
        assert_eq!(
            classify_rpc_error(r#"{"code":429,"message":"Too Many Requests"}"#),
            RpcErrorKind::RateLimited
        );
        assert_eq!(
            classify_rpc_error("error sending request: operation timed out"),
            RpcErrorKind::Transient
        );
        assert_eq!(
            classify_rpc_error("missing trie node / historical state not available"),
            RpcErrorKind::Transient
        );
    }

    #[test]
    fn range_error_wins_when_endpoints_disagree() {
        let joined =
            "[a.example: 429 Too Many Requests] [b.example: exceed maximum block range: 50000]";
        assert_eq!(
            classify_rpc_error(joined),
            RpcErrorKind::RangeTooLarge { hint: Some(50_000) }
        );
    }

    #[test]
    fn backoff_grows_exponentially_and_respects_the_cap() {
        let base = Duration::from_millis(500);
        let cap = Duration::from_secs(30);
        for attempt in 1..=12 {
            let ceiling = (base * (1 << (attempt - 1))).min(cap);
            let low = backoff_delay(attempt, base, cap, 0);
            let high = backoff_delay(attempt, base, cap, u64::MAX);
            assert_eq!(low, ceiling / 2, "attempt {attempt} lower bound");
            assert!(high <= ceiling, "attempt {attempt} upper bound");
            assert!(high >= low);
        }
        assert_eq!(backoff_delay(1_000, base, cap, 0), cap / 2);
    }

    #[test]
    fn jitter_stays_within_the_window() {
        let base = Duration::from_secs(1);
        let cap = Duration::from_secs(60);
        for _ in 0..200 {
            let delay = jittered_backoff(4, base, cap);
            assert!(delay >= Duration::from_secs(4));
            assert!(delay <= Duration::from_secs(8));
        }
    }

    #[test]
    fn chunk_shrinks_to_the_advertised_limit_and_never_regrows_past_it() {
        let mut chunk = AdaptiveChunk::new(1_000_000, 500, 1_000_000);
        chunk.on_range_error(1_000_000, Some(50_000));
        assert_eq!(chunk.size(), 50_000);
        for _ in 0..30 {
            chunk.on_success();
        }
        assert_eq!(chunk.size(), 50_000, "learned ceiling must hold");
    }

    #[test]
    fn chunk_halves_without_a_hint_and_is_floored() {
        let mut chunk = AdaptiveChunk::new(10_000, 1_000, 1_000_000);
        chunk.on_range_error(10_000, Some(10_000));
        assert_eq!(
            chunk.size(),
            5_000,
            "a hint equal to the failed span must still shrink"
        );
        chunk.on_range_error(5_000, None);
        chunk.on_range_error(2_500, None);
        chunk.on_range_error(1_250, None);
        assert_eq!(chunk.size(), 1_000);
        chunk.on_range_error(1_000, None);
        assert_eq!(chunk.size(), 1_000, "never below the floor");
    }

    #[test]
    fn chunk_holds_steady_when_every_third_request_is_rate_limited() {
        let mut chunk = AdaptiveChunk::new(20_000, 500, 20_000);
        for _ in 0..50 {
            chunk.on_success();
            chunk.on_success();
            chunk.on_rate_limited();
        }
        assert_eq!(chunk.size(), 10_000);
    }

    #[test]
    fn chunk_grows_back_after_rate_limiting() {
        let mut chunk = AdaptiveChunk::new(80_000, 1_000, 1_000_000);
        chunk.on_rate_limited();
        assert_eq!(chunk.size(), 40_000);
        for _ in 0..GROW_AFTER_SUCCESSES {
            chunk.on_success();
        }
        assert_eq!(
            chunk.size(),
            80_000,
            "rate limits must not lower the ceiling"
        );
        for _ in 0..(GROW_AFTER_SUCCESSES * 10) {
            chunk.on_success();
        }
        assert_eq!(chunk.size(), 1_000_000);
    }
}
