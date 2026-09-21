//! kafka -> postgres pipeline: a direct port of the old
//! worker/internal/kafkapg.go. Consumes Config.sync.source.topic and
//! upserts into Config.sync.target.table, either continuously (Mode:
//! streaming) or once per cron tick (Mode: scheduled). Only
//! Replication=delta is supported.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use cron::Schedule;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::time::{sleep, timeout};
use tokio_postgres::NoTls;
use tracing::{error, warn};

use crate::heartbeat::Client as HeartbeatClient;
use crate::mapping::{build_upsert_sql, extract_values, quote_ident};
use crate::types::{ConnectionSpec, ConnectionType, SyncMode, SyncReplication, SyncRetention, SyncSpec};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const DRAIN_IDLE_TIMEOUT: Duration = Duration::from_secs(3);
const RETENTION_PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub struct Config {
    pub sync_name: String,
    pub sync: SyncSpec,
    pub source: ConnectionSpec,
    pub target: ConnectionSpec,
    pub heartbeat: HeartbeatClient,
}

pub async fn run(cfg: Config) -> Result<()> {
    if cfg.source.typ != ConnectionType::Kafka || cfg.source.kafka.is_none() {
        bail!("source connection has type {:?}, want kafka", cfg.source.typ);
    }
    if cfg.target.typ != ConnectionType::Postgres || cfg.target.postgres.is_none() {
        bail!("target connection has type {:?}, want postgres", cfg.target.typ);
    }
    if cfg.sync.replication != SyncReplication::Delta {
        bail!("replication {:?} is not supported (only delta)", cfg.sync.replication);
    }

    let schedule = match cfg.sync.mode {
        SyncMode::Scheduled => Some(
            Schedule::from_str(&cfg.sync.schedule)
                .with_context(|| format!("parsing schedule {:?}", cfg.sync.schedule))?,
        ),
        SyncMode::Streaming => None,
    };

    let dsn = cfg.target.postgres.as_ref().unwrap().dsn.clone();
    let (pg, pg_conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connecting to target postgres")?;
    tokio::spawn(async move {
        if let Err(e) = pg_conn.await {
            error!(error = %e, "postgres connection closed with error");
        }
    });
    let pg = std::sync::Arc::new(pg);

    if let Some(retention) = cfg.sync.retention.clone() {
        let pg = pg.clone();
        let table = cfg.sync.target.table.clone();
        let sync_name = cfg.sync_name.clone();
        tokio::spawn(async move {
            prune_loop(pg, &table, &retention, &sync_name).await;
        });
    }

    let group_id = if cfg.sync.consumer_group.is_empty() {
        format!("dal-worker-{}", cfg.sync_name)
    } else {
        cfg.sync.consumer_group.clone()
    };

    let brokers = cfg.source.kafka.as_ref().unwrap().brokers.join(",");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("creating kafka consumer")?;
    consumer
        .subscribe(&[cfg.sync.source.topic.as_str()])
        .context("subscribing to topic")?;

    let upsert_sql = build_upsert_sql(&cfg.sync.target.table, &cfg.sync.mapping);

    let mut hb = cfg.heartbeat.clone();
    let sync_name = cfg.sync_name.clone();
    tokio::spawn(async move {
        loop {
            sleep(HEARTBEAT_INTERVAL).await;
            let ts = Utc::now().timestamp();
            // rdkafka doesn't expose consumer-group lag synchronously the
            // way kafka-go's reader.Stats().Lag does; report 0 here rather
            // than adding a separate offset-lookup round trip per tick.
            if let Err(e) = hb.heartbeat(&sync_name, "running", 0, "", ts).await {
                warn!(error = %e, sync = %sync_name, "heartbeat failed");
            }
        }
    });

    if let Some(schedule) = schedule {
        run_scheduled(&consumer, &pg, &upsert_sql, &cfg, schedule).await
    } else {
        run_streaming(&consumer, &pg, &upsert_sql, &cfg).await
    }
}

async fn run_streaming(
    consumer: &StreamConsumer,
    pg: &tokio_postgres::Client,
    upsert_sql: &str,
    cfg: &Config,
) -> Result<()> {
    loop {
        let msg = consumer.recv().await.context("fetching message")?;
        upsert_one(consumer, pg, upsert_sql, cfg, &msg).await?;
    }
}

async fn run_scheduled(
    consumer: &StreamConsumer,
    pg: &tokio_postgres::Client,
    upsert_sql: &str,
    cfg: &Config,
    schedule: Schedule,
) -> Result<()> {
    loop {
        let next = schedule
            .upcoming(Utc)
            .next()
            .context("schedule produced no upcoming tick")?;
        let now = Utc::now();
        let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
        sleep(wait).await;
        drain_cycle(consumer, pg, upsert_sql, cfg).await?;
    }
}

async fn drain_cycle(
    consumer: &StreamConsumer,
    pg: &tokio_postgres::Client,
    upsert_sql: &str,
    cfg: &Config,
) -> Result<()> {
    loop {
        match timeout(DRAIN_IDLE_TIMEOUT, consumer.recv()).await {
            Ok(Ok(msg)) => upsert_one(consumer, pg, upsert_sql, cfg, &msg).await?,
            Ok(Err(e)) => return Err(e).context("fetching message"),
            Err(_) => return Ok(()), // idle timeout: drain cycle done until next tick
        }
    }
}

/// Periodically deletes rows older than retention.days from table, keyed on
/// retention.timestamp_column. Runs independently of streaming/scheduled
/// consume mode — pruning is a target-side concern, not a source-consumption
/// concern. Errors are logged and retried on the next tick rather than
/// killing the whole worker, since a transient prune failure shouldn't take
/// down ingestion.
async fn prune_loop(
    pg: std::sync::Arc<tokio_postgres::Client>,
    table: &str,
    retention: &SyncRetention,
    sync_name: &str,
) {
    let sql = format!(
        "DELETE FROM {} WHERE {} < now() - INTERVAL '{} days'",
        quote_ident(table),
        quote_ident(&retention.timestamp_column),
        retention.days,
    );
    loop {
        match pg.execute(sql.as_str(), &[]).await {
            Ok(deleted) => {
                if deleted > 0 {
                    tracing::info!(sync = %sync_name, table, deleted, "pruned rows past retention window");
                }
            }
            Err(e) => error!(sync = %sync_name, table, error = %e, "retention prune failed"),
        }
        sleep(RETENTION_PRUNE_INTERVAL).await;
    }
}

async fn upsert_one(
    consumer: &StreamConsumer,
    pg: &tokio_postgres::Client,
    upsert_sql: &str,
    cfg: &Config,
    msg: &rdkafka::message::BorrowedMessage<'_>,
) -> Result<()> {
    let payload = msg.payload().unwrap_or_default();
    match extract_values(payload, &cfg.sync.mapping) {
        Ok((values, _key)) => {
            let params: Vec<_> = values.iter().map(|v| v.to_sql_param()).collect();
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                params.iter().map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
            pg.execute(upsert_sql, &param_refs)
                .await
                .with_context(|| format!("upserting row (offset {})", msg.offset()))?;
            consumer
                .commit_message(msg, CommitMode::Sync)
                .context("committing offset")?;
        }
        Err(e) => {
            warn!(sync = %cfg.sync_name, offset = msg.offset(), error = %e, "skipping malformed message");
            consumer
                .commit_message(msg, CommitMode::Sync)
                .context("commit failed after skip")?;
        }
    }
    Ok(())
}

