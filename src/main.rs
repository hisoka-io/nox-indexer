mod api;
mod broadcast;
mod chain;
mod config;
mod db;
mod geo;
mod node;
mod state;

use clap::Parser;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;

use config::{Args, NetworkConfig};
use state::AppState;

/// How often banked lifetime metric offsets are flushed to Postgres. Restart
/// detection persists immediately; this bounds what an indexer crash can lose.
const METRIC_OFFSET_FLUSH_SECS: u64 = 60;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info,indexer=debug")
        .init();

    let args = Args::parse();

    let net_config = NetworkConfig::resolve(&args).unwrap_or_else(|e| {
        eprintln!("Config error: {e}");
        std::process::exit(1);
    });

    let chain_config = Arc::new(
        chain::ChainConfig::new(
            &net_config.rpc_urls,
            &args.registry_address,
            net_config.poll_interval_secs,
            net_config.from_block,
            net_config.confirmations,
            args.expected_chain_id,
            chain::LogChunkConfig {
                initial: args.log_chunk_size,
                min: args.log_chunk_min,
                max: args.log_chunk_max,
            },
        )
        .unwrap_or_else(|e| {
            eprintln!("Chain config error: {e}");
            std::process::exit(1);
        }),
    );

    let state = init_state(&args, chain_config.clone()).await;

    let handles = spawn_background_tasks(&state, &chain_config, args.uptime_check_interval);

    install_shutdown_handler(state.shutdown.clone());

    serve_http(state, args.port, &net_config, &args).await;

    tracing::info!("Waiting for background tasks to finish...");
    for handle in handles {
        let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
    }
    tracing::info!("Shutdown complete");
}

async fn init_state(args: &Args, chain: Arc<chain::ChainConfig>) -> AppState {
    let db = db::Db::connect(&args.database_url)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to connect to database: {e}");
            std::process::exit(1);
        });

    let geo = match geo::GeoIp::open(&args.geoip_db_path) {
        Ok(g) => {
            tracing::info!("GeoIP database loaded from {}", args.geoip_db_path);
            Some(g)
        }
        Err(e) => {
            tracing::warn!("GeoIP database failed to load: {e} — coordinates will be unavailable");
            None
        }
    };

    // Serve the configured registry's last known members until the first chain
    // sync of this process reconciles them. Rows from other registries are not
    // loaded: after a registry change they must not appear registered.
    let persisted_nodes = db
        .load_registry_nodes(&chain.registry_hex)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Failed to load nodes from DB: {e}");
            HashMap::new()
        });
    tracing::info!(
        "Loaded {} nodes of registry {} from DB",
        persisted_nodes.len(),
        chain.registry_hex
    );

    let check_interval_ms =
        i64::try_from(args.uptime_check_interval.saturating_mul(1_000)).unwrap_or(i64::MAX);
    let network_genesis_ms = match db.ensure_network_genesis(check_interval_ms).await {
        Ok(genesis) => genesis,
        Err(e) => {
            tracing::warn!("Failed to load network genesis: {e}");
            None
        }
    };

    let (tx, _rx) = tokio::sync::broadcast::channel(256);

    let metric_offsets = match db.load_metric_offsets().await {
        Ok(offsets) => {
            if !offsets.is_empty() {
                tracing::info!("Loaded lifetime metric offsets for {} nodes", offsets.len());
            }
            offsets
        }
        Err(e) => {
            tracing::warn!("Failed to load metric offsets, starting empty: {e}");
            HashMap::new()
        }
    };

    let registry_address = chain.registry_hex.clone();
    AppState {
        chain,
        nodes: Arc::new(RwLock::new(persisted_nodes)),
        metrics: Arc::new(RwLock::new(HashMap::new())),
        recent_events: Arc::new(RwLock::new(VecDeque::with_capacity(
            state::MAX_RECENT_EVENTS + 10,
        ))),
        topo_dedup: Arc::new(parking_lot::Mutex::new(state::TopoDedup::new())),
        tx,
        db,
        geo,
        shutdown: CancellationToken::new(),
        metric_offsets: Arc::new(RwLock::new(metric_offsets)),
        sync: Arc::new(RwLock::new(state::SyncStatus {
            registry_address,
            ..Default::default()
        })),
        topology_version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        seed_cache: Arc::new(tokio::sync::Mutex::new(None)),
        probe_now: Arc::new(tokio::sync::Notify::new()),
        network_genesis_ms: Arc::new(RwLock::new(network_genesis_ms)),
    }
}

fn spawn_background_tasks(
    state: &AppState,
    chain_config: &Arc<chain::ChainConfig>,
    uptime_interval: u64,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();

    // Run the chain sync inside the spawned task rather than awaiting it here.
    // A replay can cover tens of millions of blocks, and blocking startup on it
    // meant the HTTP server did not bind until it finished, so the platform
    // healthcheck timed out and killed the deploy before it could ever serve.
    // The supervisor retries failed syncs forever with backoff.
    let s = state.clone();
    let c = chain_config.clone();
    handles.push(tokio::spawn(
        async move { chain::run_chain_sync(s, c).await },
    ));

    let s = state.clone();
    handles.push(tokio::spawn(async move {
        node::subscriber::manage_subscriptions(s).await
    }));

    let s = state.clone();
    handles.push(tokio::spawn(async move {
        node::subscriber::uptime_check_loop(s, uptime_interval).await
    }));

    let s = state.clone();
    handles.push(tokio::spawn(async move {
        node::subscriber::periodic_topology_sync(s).await
    }));

    let s = state.clone();
    handles.push(tokio::spawn(async move {
        node::subscriber::metric_offset_flush_loop(s, METRIC_OFFSET_FLUSH_SECS).await
    }));

    handles
}

fn install_shutdown_handler(shutdown: CancellationToken) {
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received shutdown signal");
        shutdown.cancel();
    });
}

async fn serve_http(state: AppState, port: u16, net_config: &NetworkConfig, args: &Args) {
    let chain = state.chain.clone();
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    let app = axum::Router::new()
        .route("/v1/state", axum::routing::get(api::handle_get_state))
        .route(
            "/v1/reputation",
            axum::routing::get(api::handle_get_reputation),
        )
        .route("/v1/live", axum::routing::get(api::handle_ws_upgrade))
        .route(
            "/seed/topology",
            axum::routing::get(api::handle_seed_topology),
        )
        .route("/healthz", axum::routing::get(api::handle_healthz))
        .layer(cors)
        .with_state(state.clone());

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind to {addr}: {e}");
            std::process::exit(1);
        });

    tracing::info!("Indexer listening on http://{addr}");
    tracing::info!("  Network: {}", args.network);
    tracing::info!("  RPC endpoints: {}", chain.endpoint_labels.join(", "));
    tracing::info!(
        "  Registry: {} (from block {})",
        chain.registry_hex,
        net_config.from_block
    );
    tracing::info!("  Confirmations: {}", net_config.confirmations);
    tracing::info!("  Uptime check interval: {}s", args.uptime_check_interval);

    let shutdown = state.shutdown.clone();
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            tracing::info!("HTTP server shutting down gracefully");
        })
        .await
    {
        eprintln!("Server error: {e}");
    }
}
