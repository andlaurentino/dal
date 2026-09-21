//! kafka -> lake pipeline: consumes Config.sync.source.topic and appends
//! each message as a row in a Delta table at
//! s3://{bucket}/{sync.target.path}, creating the table with
//! delta.enableChangeDataFeed=true on first run if it doesn't exist yet.
//! Delta writes are file-oriented, so — unlike kafkapostgres.rs's
//! per-message commit — messages are batched by count/time window and
//! offsets are only committed after a batch's write succeeds (same
//! at-least-once semantics as the postgres path, coarser granularity).
//!
//! NOTE: delta-rs's Rust API (CreateBuilder/DeltaOps/WriteBuilder shapes)
//! has churned across versions faster than this dependency set's other
//! crates — verify these calls against the exact `deltalake` version
//! pinned in Cargo.toml the first time this builds against a real MinIO.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use chrono::Utc;
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::operations::DeltaOps;
use deltalake::DeltaTable;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::time::timeout;
use tracing::{error, warn};

use crate::heartbeat::Client as HeartbeatClient;
use crate::mapping::{extract_values, ColumnValue};
use crate::types::{ConnectionSpec, ConnectionType, SyncFieldMapping, SyncMode, SyncReplication, SyncSpec};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const BATCH_MAX_ROWS: usize = 500;
const BATCH_MAX_WAIT: Duration = Duration::from_secs(5);

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
    if cfg.target.typ != ConnectionType::Datalake || cfg.target.datalake.is_none() {
        bail!("target connection has type {:?}, want datalake", cfg.target.typ);
    }
    if cfg.sync.replication != SyncReplication::Delta {
        bail!("replication {:?} is not supported (only delta)", cfg.sync.replication);
    }
    if cfg.sync.mode != SyncMode::Streaming {
        bail!("kafka->lake only supports streaming mode currently");
    }

    let lake = cfg.target.datalake.as_ref().unwrap();
    let table_uri = format!("s3://{}/{}", lake.bucket, cfg.sync.target.path.trim_matches('/'));
    let storage_options = storage_options_for(&lake.endpoint);

    let mut table = open_or_create_table(&table_uri, &storage_options, &cfg.sync.mapping.schema).await?;

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

    let arrow_schema = arrow_schema_for(&cfg.sync.mapping.schema);

    let mut hb = cfg.heartbeat.clone();
    let sync_name = cfg.sync_name.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(HEARTBEAT_INTERVAL).await;
            let ts = Utc::now().timestamp();
            if let Err(e) = hb.heartbeat(&sync_name, "running", 0, "", ts).await {
                warn!(error = %e, sync = %sync_name, "heartbeat failed");
            }
        }
    });

    loop {
        let (rows, kafka_msgs) = collect_batch(&consumer, &cfg).await?;
        if rows.is_empty() {
            continue;
        }
        let batch = build_record_batch(&arrow_schema, &cfg.sync.mapping.schema, &rows)?;
        table = DeltaOps(table)
            .write(vec![batch])
            .await
            .context("writing batch to delta table")?;
        for msg in &kafka_msgs {
            if let Err(e) = consumer.commit_message(msg, CommitMode::Async) {
                error!(error = %e, "failed committing kafka offset after delta write");
            }
        }
    }
}

fn storage_options_for(endpoint: &str) -> HashMap<String, String> {
    let mut opts = HashMap::new();
    opts.insert("AWS_ENDPOINT_URL".to_string(), endpoint.to_string());
    // MinIO defaults used by infra/k8s/minio.yaml; override via the
    // Connection's credentialsRef mechanism once a secrets backend exists.
    opts.insert("AWS_ACCESS_KEY_ID".to_string(), "dalminio".to_string());
    opts.insert("AWS_SECRET_ACCESS_KEY".to_string(), "dalminio123".to_string());
    opts.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
    opts.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
    // Without an explicit region, object_store's S3 client falls back to
    // its default AWS credential/region discovery chain, which includes an
    // EC2 instance-metadata-service (IMDS) lookup that hangs/times out
    // off-AWS (e.g. against MinIO). MinIO ignores the region value itself,
    // but object_store still needs one set to skip that lookup entirely.
    opts.insert("AWS_REGION".to_string(), "us-east-1".to_string());
    opts
}

fn delta_type_for(typ: &str) -> DeltaDataType {
    match typ {
        "int" | "bigint" => DeltaDataType::Primitive(PrimitiveType::Long),
        "float" | "double" => DeltaDataType::Primitive(PrimitiveType::Double),
        "boolean" | "bool" => DeltaDataType::Primitive(PrimitiveType::Boolean),
        "timestamptz" | "timestamp" => DeltaDataType::Primitive(PrimitiveType::Timestamp),
        _ => DeltaDataType::Primitive(PrimitiveType::String),
    }
}

fn arrow_type_for(typ: &str) -> ArrowDataType {
    match typ {
        "int" | "bigint" => ArrowDataType::Int64,
        "float" | "double" => ArrowDataType::Float64,
        "boolean" | "bool" => ArrowDataType::Boolean,
        _ => ArrowDataType::Utf8,
    }
}

fn arrow_schema_for(fields: &[SyncFieldMapping]) -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(
        fields
            .iter()
            .map(|f| Field::new(&f.column, arrow_type_for(&f.typ), true))
            .collect::<Vec<_>>(),
    ))
}

async fn open_or_create_table(
    table_uri: &str,
    storage_options: &HashMap<String, String>,
    fields: &[SyncFieldMapping],
) -> Result<DeltaTable> {
    match deltalake::open_table_with_storage_options(table_uri, storage_options.clone()).await {
        Ok(table) => Ok(table),
        Err(_) => {
            let columns: Vec<StructField> = fields
                .iter()
                .map(|f| StructField::new(f.column.clone(), delta_type_for(&f.typ), true))
                .collect();

            let mut config = HashMap::new();
            config.insert("delta.enableChangeDataFeed".to_string(), Some("true".to_string()));

            CreateBuilder::new()
                .with_location(table_uri)
                .with_storage_options(storage_options.clone())
                .with_columns(columns)
                .with_configuration(config)
                .await
                .context("creating delta table")
        }
    }
}

/// Pulls messages off the consumer until BATCH_MAX_ROWS is reached or
/// BATCH_MAX_WAIT elapses since the first message in this batch. Malformed
/// messages are logged and committed immediately (same skip-and-commit
/// behavior as kafkapostgres.rs) rather than included in the batch.
async fn collect_batch<'a>(
    consumer: &'a StreamConsumer,
    cfg: &Config,
) -> Result<(Vec<(Vec<ColumnValue>, String)>, Vec<rdkafka::message::BorrowedMessage<'a>>)> {
    let mut rows = Vec::new();
    let mut msgs = Vec::new();
    let deadline = tokio::time::Instant::now() + BATCH_MAX_WAIT;

    while rows.len() < BATCH_MAX_ROWS {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => {
                let payload = msg.payload().unwrap_or_default();
                match extract_values(payload, &cfg.sync.mapping) {
                    Ok((values, key)) => {
                        rows.push((values, key));
                        msgs.push(msg);
                    }
                    Err(e) => {
                        warn!(sync = %cfg.sync_name, offset = msg.offset(), error = %e, "skipping malformed message");
                        consumer
                            .commit_message(&msg, CommitMode::Async)
                            .context("commit failed after skip")?;
                    }
                }
            }
            Ok(Err(e)) => return Err(e).context("fetching message"),
            Err(_) => break, // batch wait elapsed
        }
    }
    Ok((rows, msgs))
}

fn build_record_batch(
    schema: &Arc<ArrowSchema>,
    fields: &[SyncFieldMapping],
    rows: &[(Vec<ColumnValue>, String)],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for (col_idx, f) in fields.iter().enumerate() {
        let column: ArrayRef = match f.typ.as_str() {
            "int" | "bigint" => Arc::new(Int64Array::from_iter(rows.iter().map(|(v, _)| match &v[col_idx] {
                ColumnValue::Int(i) => Some(*i),
                _ => None,
            }))),
            "float" | "double" => Arc::new(Float64Array::from_iter(rows.iter().map(|(v, _)| match &v[col_idx] {
                ColumnValue::Float(x) => Some(*x),
                _ => None,
            }))),
            "boolean" | "bool" => Arc::new(BooleanArray::from_iter(rows.iter().map(|(v, _)| match &v[col_idx] {
                ColumnValue::Bool(b) => Some(*b),
                _ => None,
            }))),
            _ => Arc::new(StringArray::from_iter(rows.iter().map(|(v, _)| {
                Some(v[col_idx].as_key_string())
            }))),
        };
        columns.push(column);
    }
    RecordBatch::try_new(schema.clone(), columns).context("building arrow record batch")
}
