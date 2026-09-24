use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "indexer",
    about = "NOX mixnet indexer — discovers nodes from NoxRegistry"
)]
pub struct Args {
    /// Network: localtestnet, testnet, or mainnet
    #[arg(long, env = "NETWORK", default_value = "localtestnet")]
    pub network: String,

    /// `NoxRegistry` contract address (0x-prefixed hex)
    #[arg(long, env = "REGISTRY_ADDRESS")]
    pub registry_address: String,

    /// Ethereum JSON-RPC URL(s), comma-separated for failover (overrides network default)
    #[arg(long, env = "ETH_RPC_URL")]
    pub rpc_url: Option<String>,

    /// Refuse to sync unless the RPC reports this chain id (e.g. 421614)
    #[arg(long, env = "EXPECTED_CHAIN_ID")]
    pub expected_chain_id: Option<u64>,

    /// Blocks behind head treated as final (default: 0 on localtestnet, 20 otherwise)
    #[arg(long, env = "CHAIN_CONFIRMATIONS")]
    pub confirmations: Option<u64>,

    /// Initial eth_getLogs block range; adapts to provider limits at runtime
    #[arg(long, env = "LOG_CHUNK_SIZE", default_value = "50000")]
    pub log_chunk_size: u64,

    /// Smallest eth_getLogs block range the adaptive sizing may shrink to
    #[arg(long, env = "LOG_CHUNK_MIN", default_value = "500")]
    pub log_chunk_min: u64,

    /// Largest eth_getLogs block range the adaptive sizing may grow to
    #[arg(long, env = "LOG_CHUNK_MAX", default_value = "1000000")]
    pub log_chunk_max: u64,

    /// Block number to start scanning events from (contract deployment block)
    #[arg(long, env = "FROM_BLOCK", default_value = "0")]
    pub from_block: u64,

    /// Indexer HTTP server port
    #[arg(long, env = "PORT", default_value = "4000")]
    pub port: u16,

    /// `PostgreSQL` connection string
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://localhost/indexer"
    )]
    pub database_url: String,

    /// Interval in seconds between uptime reputation checks (default: 60)
    #[arg(long, env = "UPTIME_CHECK_INTERVAL", default_value = "60")]
    pub uptime_check_interval: u64,

    /// Path to MaxMind GeoLite2-City.mmdb file for IP geolocation
    #[arg(
        long,
        env = "GEOIP_DB_PATH",
        default_value = "./assets/GeoLite2-City.mmdb"
    )]
    pub geoip_db_path: String,
}

pub struct NetworkConfig {
    pub rpc_urls: Vec<String>,
    pub poll_interval_secs: u64,
    pub from_block: u64,
    pub confirmations: u64,
}

impl NetworkConfig {
    pub fn resolve(args: &Args) -> Result<Self, String> {
        if args.from_block == 0 {
            return Err(
                "FROM_BLOCK must be the exact NoxRegistry deployment block; refusing an unbounded replay"
                    .to_string(),
            );
        }
        let (default_rpc, poll_secs, default_confirmations) = match args.network.as_str() {
            "localtestnet" => (Some("http://127.0.0.1:8545".to_string()), 6u64, 0u64),
            "testnet" | "mainnet" => (None, 12u64, 20u64),
            other => {
                return Err(format!(
                    "Unknown network: {other}. Use localtestnet, testnet, or mainnet"
                ))
            }
        };

        let rpc_urls = args
            .rpc_url
            .as_deref()
            .map(crate::chain::rpc::parse_rpc_urls)
            .filter(|urls| !urls.is_empty())
            .or_else(|| default_rpc.map(|url| vec![url]))
            .ok_or_else(|| format!("--rpc-url is required for network '{}'", args.network))?;

        if args.log_chunk_min == 0 || args.log_chunk_min > args.log_chunk_max {
            return Err("LOG_CHUNK_MIN must be positive and not above LOG_CHUNK_MAX".to_string());
        }

        Ok(Self {
            rpc_urls,
            poll_interval_secs: poll_secs,
            from_block: args.from_block,
            confirmations: args.confirmations.unwrap_or(default_confirmations),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(network: &str, rpc_url: Option<&str>, from_block: u64) -> Args {
        Args {
            network: network.to_string(),
            registry_address: "0x1111111111111111111111111111111111111111".to_string(),
            rpc_url: rpc_url.map(str::to_string),
            expected_chain_id: None,
            confirmations: None,
            log_chunk_size: 50_000,
            log_chunk_min: 500,
            log_chunk_max: 1_000_000,
            from_block,
            port: 4_000,
            database_url: "postgres://localhost/indexer".to_string(),
            uptime_check_interval: 60,
            geoip_db_path: "./assets/GeoLite2-City.mmdb".to_string(),
        }
    }

    #[test]
    fn zero_start_block_is_rejected_before_an_unbounded_replay() {
        assert!(matches!(
            NetworkConfig::resolve(&args("localtestnet", None, 0)),
            Err(message) if message.contains("FROM_BLOCK")
        ));
    }

    #[test]
    fn comma_separated_rpc_urls_become_a_failover_list() {
        let config = NetworkConfig::resolve(&args(
            "testnet",
            Some("https://a.example/rpc, https://b.example/rpc"),
            10,
        ))
        .unwrap();
        assert_eq!(
            config.rpc_urls,
            vec!["https://a.example/rpc", "https://b.example/rpc"]
        );
        assert_eq!(config.confirmations, 20);
    }

    #[test]
    fn testnet_requires_an_rpc_url() {
        assert!(NetworkConfig::resolve(&args("testnet", Some(" , "), 10)).is_err());
        let local = NetworkConfig::resolve(&args("localtestnet", None, 10)).unwrap();
        assert_eq!(local.rpc_urls, vec!["http://127.0.0.1:8545"]);
        assert_eq!(local.confirmations, 0);
    }
}
