//! workerd is spawned by the control plane, one process per Sync. It
//! fetches its Sync's spec (and the source/target Connections it
//! references) from the control plane's REST API, then dispatches to the
//! matching pipeline module based on (source connection type, target
//! connection type) — kafka->postgres, kafka->lake, or lake->postgres —
//! reporting health back via the heartbeat client throughout.
//!
//! This is a from-scratch Rust rewrite that replaces the old Go `worker/`
//! module (now deleted), keeping the binary/image name and CLI contract
//! identical so controlplane/internal/workermgr needed no changes.

mod cpclient;
mod heartbeat;
mod kafkalake;
mod kafkapostgres;
mod lakepostgres;
mod mapping;
mod types;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{error, info};

use types::{ConnectionSpec, ConnectionType, SyncSpec};

#[derive(Parser)]
struct Args {
    #[arg(long = "sync-id")]
    sync_id: String,
    #[arg(long = "controlplane-addr", default_value = "localhost:9090")]
    controlplane_addr: String,
    #[arg(long = "controlplane-http-addr", default_value = "http://localhost:8080")]
    controlplane_http_addr: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    // Registers the "s3://" scheme with delta-rs's object-store factory
    // registry. Without this, any deltalake::open_table*/CreateBuilder call
    // against an s3:// URI fails with "Cannot infer storage location from:
    // Unknown scheme: s3" — the s3 Cargo feature alone only pulls in the
    // deltalake-aws crate, it doesn't self-register.
    deltalake::aws::register_handlers(None);
    let args = Args::parse();

    if let Err(e) = run(args).await {
        // {:?} (anyhow's Debug impl) prints the full "Caused by:" chain,
        // not just the top-level .context() message — essential here since
        // the top-level message alone (e.g. "creating delta table") hides
        // the actual root cause.
        error!(error = ?e, "worker stopped");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    let mut hb = heartbeat::Client::dial(&args.controlplane_addr)
        .await
        .context("failed to dial control plane")?;

    let pid = std::process::id() as i64;
    hb.register(&args.sync_id, pid)
        .await
        .context("failed to register with control plane")?;
    info!(sync = %args.sync_id, "worker registered");

    match resolve_sync(&args.controlplane_http_addr, &args.sync_id).await {
        Ok((sync, source, target)) => {
            let dispatch_result = dispatch(&args.sync_id, sync, source, target, hb.clone()).await;
            if let Err(e) = &dispatch_result {
                let _ = hb.report_error(&args.sync_id, &e.to_string(), true).await;
            }
            dispatch_result
        }
        Err(e) => {
            let _ = hb.report_error(&args.sync_id, &e.to_string(), true).await;
            Err(e)
        }
    }
}

async fn resolve_sync(
    controlplane_http_addr: &str,
    sync_id: &str,
) -> Result<(SyncSpec, ConnectionSpec, ConnectionSpec)> {
    let sync = cpclient::get_sync(controlplane_http_addr, sync_id)
        .await
        .context("fetching sync")?;
    let source = cpclient::get_connection(controlplane_http_addr, &sync.source.connection_ref)
        .await
        .context("fetching source connection")?;
    let target = cpclient::get_connection(controlplane_http_addr, &sync.target.connection_ref)
        .await
        .context("fetching target connection")?;
    Ok((sync, source, target))
}

async fn dispatch(
    sync_id: &str,
    sync: SyncSpec,
    source: ConnectionSpec,
    target: ConnectionSpec,
    hb: heartbeat::Client,
) -> Result<()> {
    match (source.typ, target.typ) {
        (ConnectionType::Kafka, ConnectionType::Postgres) => {
            kafkapostgres::run(kafkapostgres::Config {
                sync_name: sync_id.to_string(),
                sync,
                source,
                target,
                heartbeat: hb,
            })
            .await
        }
        (ConnectionType::Kafka, ConnectionType::Datalake) => {
            kafkalake::run(kafkalake::Config {
                sync_name: sync_id.to_string(),
                sync,
                source,
                target,
                heartbeat: hb,
            })
            .await
        }
        (ConnectionType::Datalake, ConnectionType::Postgres) => {
            lakepostgres::run(lakepostgres::Config {
                sync_name: sync_id.to_string(),
                sync,
                source,
                target,
                heartbeat: hb,
            })
            .await
        }
        (s, t) => anyhow::bail!("unsupported sync pair (source={:?}, target={:?})", s, t),
    }
}
