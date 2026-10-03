//! Which node addresses the indexer may contact.
//!
//! Node URLs come from permissionless registrations. Outside a local testnet
//! the indexer only polls publicly routable unicast addresses, and its HTTP
//! clients never follow redirects, so a registration cannot point the
//! indexer's pollers at its own private network.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

static ALLOW_PRIVATE_TARGETS: AtomicBool = AtomicBool::new(false);

/// Allow loopback and private node addresses (local testnets). Set once at boot.
pub fn set_private_targets_allowed(allow: bool) {
    ALLOW_PRIVATE_TARGETS.store(allow, Ordering::Relaxed);
}

pub fn private_targets_allowed() -> bool {
    ALLOW_PRIVATE_TARGETS.load(Ordering::Relaxed)
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || a == 0
        // 100.64.0.0/10 shared address space (carrier-grade NAT)
        || (a == 100 && (b & 0xc0) == 64)
        // 192.0.0.0/24 protocol assignments
        || (a == 192 && b == 0 && c == 0)
        // 198.18.0.0/15 benchmarking
        || (a == 198 && (b & 0xfe) == 18)
        // 240.0.0.0/4 reserved
        || a >= 240)
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_public_v4(v4);
    }
    let s = ip.segments();
    // Global unicast is 2000::/3. Inside it, exclude documentation and the
    // transition prefixes that embed an arbitrary IPv4 address.
    (s[0] & 0xe000) == 0x2000
        && !(s[0] == 0x2001 && s[1] == 0x0db8)
        && !(s[0] == 0x2001 && s[1] == 0x0000)
        && s[0] != 0x2002
}

/// Whether `ip` is a publicly routable unicast address.
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => is_public_v6(v6),
    }
}

pub fn ip_allowed_with(ip: IpAddr, allow_private: bool) -> bool {
    allow_private || is_public_ip(ip)
}

/// Whether the indexer may send requests to `url` (a node's admin base URL).
/// Without private targets only `http(s)://<public IP>` URLs pass; host names
/// are refused because they can resolve anywhere.
pub fn url_allowed_with(url: &str, allow_private: bool) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    if allow_private {
        return true;
    }
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
        .is_ok_and(is_public_ip)
}

pub fn url_allowed(url: &str) -> bool {
    url_allowed_with(url, private_targets_allowed())
}

/// HTTP client for node endpoints: never follows redirects. `timeout` bounds
/// the whole request; streaming clients pass `None` and only bound the connect.
pub fn node_http_client(timeout: Option<Duration>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5));
    if let Some(timeout) = timeout {
        builder = builder.timeout(timeout);
    }
    builder.build().unwrap_or_else(|error| {
        tracing::error!(
            "Failed to build node HTTP client ({error}); using defaults without redirects"
        );
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn public(ip: &str) -> bool {
        is_public_ip(ip.parse().unwrap())
    }

    #[test]
    fn only_public_unicast_addresses_are_public() {
        for ip in [
            "3.226.251.110",
            "34.237.170.1",
            "2600:1f18::1",
            "::ffff:3.86.75.6",
        ] {
            assert!(public(ip), "{ip}");
        }
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "255.255.255.255",
            "224.0.0.1",
            "198.18.0.1",
            "240.0.0.1",
            "::1",
            "::",
            "fd00::1",
            "fe80::1",
            "ff02::1",
            "2001:db8::1",
            "2002:a00:1::1",
            "::ffff:10.0.0.1",
            "::ffff:169.254.169.254",
        ] {
            assert!(!public(ip), "{ip}");
        }
    }

    #[test]
    fn private_and_named_urls_are_refused_outside_local_testnets() {
        assert!(url_allowed_with("http://3.226.251.110:15001", false));
        assert!(url_allowed_with("http://[2600:1f18::1]:15001", false));
        assert!(!url_allowed_with("http://169.254.169.254:80", false));
        assert!(!url_allowed_with("http://[fd00::1]:15001", false));
        assert!(!url_allowed_with("http://127.0.0.1:15001", false));
        assert!(!url_allowed_with(
            "http://postgres.railway.internal:5432",
            false
        ));
        assert!(!url_allowed_with("file:///etc/passwd", false));
        assert!(!url_allowed_with("", false));

        assert!(url_allowed_with("http://127.0.0.1:9001", true));
        assert!(url_allowed_with("http://node-1:9001", true));
        assert!(!url_allowed_with("ftp://127.0.0.1:9001", true));
    }
}
