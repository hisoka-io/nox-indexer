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

    /// Ethereum JSON-RPC URL (overrides network default)
    #[arg(long, env = "ETH_RPC_URL")]
    pub rpc_url: Option<String>,

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
    pub rpc_url: String,
    pub poll_interval_secs: u64,
    pub from_block: u64,
}

impl NetworkConfig {
    pub fn resolve(args: &Args) -> Result<Self, String> {
        let (default_rpc, poll_secs) = match args.network.as_str() {
            "localtestnet" => (Some("http://127.0.0.1:8545".to_string()), 6u64),
            "testnet" | "mainnet" => (None, 12u64),
            other => {
                return Err(format!(
                    "Unknown network: {other}. Use localtestnet, testnet, or mainnet"
                ))
            }
        };

        let rpc_url = args
            .rpc_url
            .clone()
            .or(default_rpc)
            .ok_or_else(|| format!("--rpc-url is required for network '{}'", args.network))?;

        Ok(Self {
            rpc_url,
            poll_interval_secs: poll_secs,
            from_block: args.from_block,
        })
    }
}
