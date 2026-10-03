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
/// Backoff between database connection attempts at boot.
const DB_CONNECT_BACKOFF_BASE: Duration = Duration::from_secs(1);
const DB_CONNECT_BACKOFF_CAP: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() {
    // RUST_LOG overrides the default, e.g. RUST_LOG=info,indexer=debug.
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = Args::parse();

    let net_config = NetworkConfig::resolve(&args).unwrap_or_else(|e| {
        eprintln!("Config error: {e}");
        std::process::exit(1);
    });
    node::targets::set_private_targets_allowed(
        args.network == "localtestnet" || args.allow_private_node_addresses,
    );

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

    let settlement_config = chain::settlement::SettlementConfig::new(
        args.entry_point_address.as_deref(),
        args.reward_pool_address.as_deref(),
        args.settlement_from_block.unwrap_or(net_config.from_block),
    )
    .unwrap_or_else(|e| {
        eprintln!("Settlement config error: {e}");
        std::process::exit(1);
    });

    let shutdown = CancellationToken::new();
    install_shutdown_handler(shutdown.clone());

    let state = init_state(&args, chain_config.clone(), shutdown).await;

    let mut handles = spawn_background_tasks(&state, &chain_config, args.uptime_check_interval);
    if let Some(config) = settlement_config {
        let s = state.clone();
        let c = chain_config.clone();
        handles.push(tokio::spawn(async move {
            chain::settlement::run_settlement_sync(s, c, config).await
        }));
    }

    serve_http(state, args.port, &net_config, &args).await;

    tracing::info!("Waiting for background tasks to finish...");
    for handle in handles {
        let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
    }
    tracing::info!("Shutdown complete");
}

/// Connect to Postgres, retrying with backoff instead of exiting. The platform
/// restarts a crashed container only a few times, so a short database outage
/// at boot must not use those restarts up and leave the indexer down.
async fn connect_db(url: &str, shutdown: &CancellationToken) -> db::Db {
    let mut attempt = 0_u32;
    loop {
        match db::Db::connect(url).await {
            Ok(db) => {
                if attempt > 0 {
                    tracing::info!("Connected to database after {attempt} failed attempts");
                }
                return db;
            }
            Err(e) => {
                attempt = attempt.saturating_add(1);
                let delay = chain::retry::jittered_backoff(
                    attempt,
                    DB_CONNECT_BACKOFF_BASE,
                    DB_CONNECT_BACKOFF_CAP,
                );
                tracing::error!(
                    "Database connection attempt {attempt} failed, retrying in {delay:?}: {e}"
                );
                tokio::select! {
                    () = shutdown.cancelled() => {
                        tracing::info!("Shutdown requested before the database was reachable");
                        std::process::exit(0);
                    }
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

async fn init_state(
    args: &Args,
    chain: Arc<chain::ChainConfig>,
    shutdown: CancellationToken,
) -> AppState {
    let db = connect_db(&args.database_url, &shutdown).await;

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
        shutdown,
        metric_offsets: Arc::new(RwLock::new(metric_offsets)),
        sync: Arc::new(RwLock::new(state::SyncStatus {
            registry_address,
            ..Default::default()
        })),
        topology_version: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        seed_cache: Arc::new(tokio::sync::Mutex::new(None)),
        probe_now: Arc::new(tokio::sync::Notify::new()),
        network_genesis_ms: Arc::new(RwLock::new(network_genesis_ms)),
        max_sync_age_secs: args.health_max_sync_age_secs,
        settlements: Arc::new(RwLock::new(Default::default())),
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
        node::subscriber::metric_offset_flush_loop(s, METRIC_OFFSET_FLUSH_SECS).await
    }));

    handles
}

/// Cancel on SIGINT or SIGTERM. Railway (like Docker) stops containers with
/// SIGTERM, and the graceful path flushes metric offsets before exit.
fn install_shutdown_handler(shutdown: CancellationToken) {
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("Received shutdown signal");
        shutdown.cancel();
    });
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut terminate) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        }
        Err(error) => {
            tracing::warn!("Cannot listen for SIGTERM ({error}); only Ctrl-C stops gracefully");
            tokio::signal::ctrl_c().await.ok();
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
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
        .route(
            "/healthz/sync",
            axum::routing::get(api::handle_healthz_sync),
        )
        .route(
            "/v1/settlements",
            axum::routing::get(api::handle_get_settlements),
        )
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
