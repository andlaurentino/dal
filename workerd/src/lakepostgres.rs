//! lake -> postgres pipeline: polls the Delta table at
//! s3://{bucket}/{sync.source.path} for new commits, reads its Change Data
//! Feed since the last processed version, and applies each row to
//! Config.sync.target.table with full CDF fidelity — insert/update rows are
//! upserted, delete rows are deleted (unlike kafkapostgres.rs, which has no
//! delete path since Kafka messages are append-only). The read cursor (last
//! processed Delta version) is persisted as a small marker object under
//! sync.source.checkpointPath in the same bucket, since — unlike a Kafka
//! consumer group — Delta versions aren't tracked by an external broker.
//!
//! NOTE: delta-rs's CDF read API (`DeltaOps::load_cdf`) has churned across
//! versions — verify this call against the exact `deltalake` version pinned
//! in Cargo.toml the first time this builds against a real MinIO.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use bytes::Bytes;
use chrono::Utc;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use deltalake::operations::DeltaOps;
use futures_util::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use tokio_postgres::NoTls;
use tracing::{error, warn};

use crate::heartbeat::Client as HeartbeatClient;
use crate::mapping::{build_delete_sql, build_upsert_sql, ColumnValue};
use crate::types::{ConnectionSpec, ConnectionType, SyncReplication, SyncSpec};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct Config {
    pub sync_name: String,
    pub sync: SyncSpec,
    pub source: ConnectionSpec,
    pub target: ConnectionSpec,
    pub heartbeat: HeartbeatClient,
}

pub async fn run(cfg: Config) -> Result<()> {
    if cfg.source.typ != ConnectionType::Datalake || cfg.source.datalake.is_none() {
        bail!("source connection has type {:?}, want datalake", cfg.source.typ);
    }
    if cfg.target.typ != ConnectionType::Postgres || cfg.target.postgres.is_none() {
        bail!("target connection has type {:?}, want postgres", cfg.target.typ);
    }
    if cfg.sync.replication == SyncReplication::Snapshot {
        bail!("replication snapshot is not yet supported for lake->postgres");
    }
    if cfg.sync.source.checkpoint_path.is_empty() {
        bail!("spec.source.checkpointPath is required for lake->postgres");
    }

    let lake = cfg.source.datalake.as_ref().unwrap();
    let table_uri = format!("s3://{}/{}", lake.bucket, cfg.sync.source.path.trim_matches('/'));
    let storage_options = storage_options_for(&lake.endpoint);

    let checkpoint_store = AmazonS3Builder::new()
        .with_endpoint(&lake.endpoint)
        .with_bucket_name(&lake.bucket)
        .with_access_key_id("dalminio")
        .with_secret_access_key("dalminio123")
        .with_allow_http(true)
        // Without an explicit region, object_store falls back to its
        // default AWS credential/region discovery chain, which includes an
        // EC2 instance-metadata-service (IMDS) lookup that hangs/times out
        // off-AWS (e.g. against MinIO).
        .with_region("us-east-1")
        .build()
        .context("building checkpoint object store client")?;
    let checkpoint_path = ObjPath::from(format!(
        "{}/version",
        cfg.sync.source.checkpoint_path.trim_matches('/')
    ));

    let mut last_processed = read_checkpoint(&checkpoint_store, &checkpoint_path).await?;

    let dsn = cfg.target.postgres.as_ref().unwrap().dsn.clone();
    let (pg, pg_conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connecting to target postgres")?;
    tokio::spawn(async move {
        if let Err(e) = pg_conn.await {
            error!(error = %e, "postgres connection closed with error");
        }
    });

    let upsert_sql = build_upsert_sql(&cfg.sync.target.table, &cfg.sync.mapping);
    let delete_sql = build_delete_sql(&cfg.sync.target.table, &cfg.sync.mapping);

    let mut hb = cfg.heartbeat.clone();
    let sync_name = cfg.sync_name.clone();
    let hb_watermark = Arc::new(tokio::sync::Mutex::new(last_processed));
    {
        let hb_watermark = hb_watermark.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(HEARTBEAT_INTERVAL).await;
                let ts = Utc::now().timestamp();
                let wm = *hb_watermark.lock().await;
                if let Err(e) = hb.heartbeat(&sync_name, "running", 0, &wm.to_string(), ts).await {
                    warn!(error = %e, sync = %sync_name, "heartbeat failed");
                }
            }
        });
    }

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let table = deltalake::open_table_with_storage_options(&table_uri, storage_options.clone())
            .await
            .context("opening delta table")?;
        let current_version = table.version();

        if current_version <= last_processed {
            continue;
        }

        let scan = DeltaOps(table)
            .load_cdf()
            .with_starting_version(last_processed + 1)
            .with_ending_version(current_version)
            .build()
            .await
            .context("loading change data feed")?;

        // DeltaCdfScan is a DataFusion ExecutionPlan, not a Stream directly
        // — execute its single partition to get a SendableRecordBatchStream.
        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = scan
            .execute(0, task_ctx)
            .context("executing cdf scan")?;

        while let Some(batch) = stream.next().await {
            let batch = batch.context("reading cdf record batch")?;
            apply_cdf_batch(&pg, &upsert_sql, &delete_sql, &cfg.sync, &batch).await?;
        }

        write_checkpoint(&checkpoint_store, &checkpoint_path, current_version).await?;
        last_processed = current_version;
        *hb_watermark.lock().await = current_version;
    }
}

fn storage_options_for(endpoint: &str) -> HashMap<String, String> {
    let mut opts = HashMap::new();
    opts.insert("AWS_ENDPOINT_URL".to_string(), endpoint.to_string());
    opts.insert("AWS_ACCESS_KEY_ID".to_string(), "dalminio".to_string());
    opts.insert("AWS_SECRET_ACCESS_KEY".to_string(), "dalminio123".to_string());
    opts.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
    // See the matching comment in kafkalake.rs::storage_options_for: without
    // this, object_store falls back to an IMDS lookup that hangs off-AWS.
    opts.insert("AWS_REGION".to_string(), "us-east-1".to_string());
    opts
}

async fn read_checkpoint(store: &impl ObjectStore, path: &ObjPath) -> Result<i64> {
    match store.get(path).await {
        Ok(result) => {
            let bytes = result.bytes().await.context("reading checkpoint bytes")?;
            let text = String::from_utf8_lossy(&bytes);
            text.trim()
                .parse::<i64>()
                .context("parsing checkpoint version")
        }
        Err(object_store::Error::NotFound { .. }) => Ok(-1), // no checkpoint yet: read from version 0
        Err(e) => Err(e).context("reading checkpoint object"),
    }
}

async fn write_checkpoint(store: &impl ObjectStore, path: &ObjPath, version: i64) -> Result<()> {
    store
        .put(path, Bytes::from(version.to_string()).into())
        .await
        .context("writing checkpoint object")?;
    Ok(())
}

/// Applies one CDF RecordBatch to postgres: insert/update_postimage rows are
/// upserted, delete rows are deleted, update_preimage rows are ignored (the
/// paired update_postimage row carries the final state).
async fn apply_cdf_batch(
    pg: &tokio_postgres::Client,
    upsert_sql: &str,
    delete_sql: &str,
    sync: &SyncSpec,
    batch: &RecordBatch,
) -> Result<()> {
    let change_type_col = batch
        .column_by_name("_change_type")
        .context("cdf batch missing _change_type column")?
        .as_any()
        .downcast_ref::<StringArray>()
        .context("_change_type column has unexpected type")?;

    let mut mapped_columns = Vec::with_capacity(sync.mapping.schema.len());
    for f in &sync.mapping.schema {
        let col = batch
            .column_by_name(&f.column)
            .with_context(|| format!("cdf batch missing column {:?}", f.column))?;
        mapped_columns.push((col, f.typ.as_str()));
    }

    for row in 0..batch.num_rows() {
        let change_type = change_type_col.value(row);
        if change_type == "update_preimage" {
            continue;
        }

        let values: Vec<ColumnValue> = mapped_columns
            .iter()
            .map(|(col, typ)| arrow_value_at(col, row, typ))
            .collect::<Result<_>>()?;

        match change_type {
            "insert" | "update_postimage" => {
                let params: Vec<_> = values.iter().map(|v| v.to_sql_param()).collect();
                let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                    .iter()
                    .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();
                pg.execute(upsert_sql, &param_refs)
                    .await
                    .context("upserting cdf row")?;
            }
            "delete" => {
                let key_idx = sync
                    .mapping
                    .schema
                    .iter()
                    .position(|f| f.column == sync.mapping.key_field)
                    .context("keyField not found in mapping schema")?;
                let key_param = values[key_idx].to_sql_param();
                pg.execute(delete_sql, &[key_param.as_ref()])
                    .await
                    .context("deleting cdf row")?;
            }
            other => warn!(change_type = other, "unexpected _change_type value, skipping row"),
        }
    }
    Ok(())
}

fn arrow_value_at(col: &arrow_array::ArrayRef, row: usize, typ: &str) -> Result<ColumnValue> {
    if col.is_null(row) {
        return Ok(ColumnValue::Text(String::new()));
    }
    Ok(match typ {
        "int" | "bigint" => ColumnValue::Int(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .context("expected int64 array")?
                .value(row),
        ),
        "float" | "double" => ColumnValue::Float(
            col.as_any()
                .downcast_ref::<Float64Array>()
                .context("expected float64 array")?
                .value(row),
        ),
        "boolean" | "bool" => ColumnValue::Bool(
            col.as_any()
                .downcast_ref::<BooleanArray>()
                .context("expected boolean array")?
                .value(row),
        ),
        "timestamptz" | "timestamp" => {
            let s = col
                .as_any()
                .downcast_ref::<StringArray>()
                .context("expected utf8 array")?
                .value(row);
            let t = chrono::DateTime::parse_from_rfc3339(s)
                .with_context(|| format!("parsing {s:?} as RFC3339"))?;
            ColumnValue::Timestamp(t.with_timezone(&chrono::Utc))
        }
        _ => ColumnValue::Text(
            col.as_any()
                .downcast_ref::<StringArray>()
                .context("expected utf8 array")?
                .value(row)
                .to_string(),
        ),
    })
}
