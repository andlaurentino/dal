//! Field extraction/coercion and SQL rendering shared across pipelines,
//! ported from the old worker/internal/kafkapg's extractValues/coerce/
//! buildUpsertSQL.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use tokio_postgres::types::ToSql;

use crate::types::SyncMapping;

/// A coerced column value, typed per the mapping's declared column `type`.
/// Kept as an enum (rather than a trait object) so both the postgres-upsert
/// path and the Arrow-array-append path (kafka->lake) can pattern-match on
/// it without redoing JSON coercion twice.
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Text(String),
    // Kept distinct from Text: tokio-postgres's extended query protocol
    // infers each placeholder's Postgres type from the target column (here,
    // timestamptz/timestamp), and a bare String doesn't implement ToSql for
    // that type — only chrono::DateTime<Utc> does (via tokio-postgres's
    // with-chrono-0_4 feature). Storage in the lake stays string-based
    // (RFC3339, via as_key_string()) — this variant only matters for
    // postgres parameter binding.
    Timestamp(DateTime<Utc>),
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl ColumnValue {
    pub fn as_key_string(&self) -> String {
        match self {
            ColumnValue::Text(s) => s.clone(),
            ColumnValue::Timestamp(t) => t.to_rfc3339(),
            ColumnValue::Int(i) => i.to_string(),
            ColumnValue::Float(f) => f.to_string(),
            ColumnValue::Bool(b) => b.to_string(),
        }
    }

    /// Boxes this value as a tokio-postgres bind parameter.
    pub fn to_sql_param(&self) -> Box<dyn ToSql + Sync + Send> {
        match self {
            ColumnValue::Text(s) => Box::new(s.clone()),
            ColumnValue::Timestamp(t) => Box::new(*t),
            ColumnValue::Int(i) => Box::new(*i),
            ColumnValue::Float(f) => Box::new(*f),
            ColumnValue::Bool(b) => Box::new(*b),
        }
    }
}

/// Parses an RFC3339 timestamp string (the format tools/producer and
/// workerd's own lake writes both use) into a Timestamp value.
fn coerce_timestamp(v: &JsonValue, typ: &str) -> Result<ColumnValue> {
    let s = match v {
        JsonValue::String(s) => s.as_str(),
        _ => bail!("cannot coerce {v} to {typ}: expected a string"),
    };
    DateTime::parse_from_rfc3339(s)
        .map(|t| ColumnValue::Timestamp(t.with_timezone(&Utc)))
        .with_context(|| format!("cannot coerce {s:?} to {typ}: expected RFC3339"))
}

/// Maps a SyncFieldMapping's declared `type` string to how the value should
/// be coerced, mirroring kafkapg.go's coerce().
fn coerce(v: &JsonValue, typ: &str) -> Result<ColumnValue> {
    match typ {
        "text" | "json" => Ok(ColumnValue::Text(json_to_string(v))),
        "timestamptz" | "timestamp" => coerce_timestamp(v, typ),
        "int" | "bigint" => match v {
            JsonValue::Number(n) => n
                .as_i64()
                .map(ColumnValue::Int)
                .ok_or_else(|| anyhow!("cannot coerce {v} to {typ}")),
            JsonValue::String(s) => s
                .parse::<i64>()
                .map(ColumnValue::Int)
                .with_context(|| format!("cannot coerce {s:?} to {typ}")),
            _ => bail!("cannot coerce {v} to {typ}"),
        },
        "float" | "double" => match v {
            JsonValue::Number(n) => n
                .as_f64()
                .map(ColumnValue::Float)
                .ok_or_else(|| anyhow!("cannot coerce {v} to {typ}")),
            _ => bail!("cannot coerce {v} to {typ}"),
        },
        "boolean" | "bool" => match v {
            JsonValue::Bool(b) => Ok(ColumnValue::Bool(*b)),
            _ => bail!("cannot coerce {v} to {typ}"),
        },
        _ => Ok(ColumnValue::Text(json_to_string(v))),
    }
}

fn json_to_string(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Walks a decoded JSON document per each field's flat/dotted JSONPath
/// (e.g. "$.event_id" or "$.a.b" — no array indexing), coercing each value
/// per its declared type. Returns the coerced row plus the key field's
/// stringified value, for use as a heartbeat watermark or upsert/delete key.
pub fn extract_values(raw: &[u8], mapping: &SyncMapping) -> Result<(Vec<ColumnValue>, String)> {
    let doc: JsonValue = serde_json::from_slice(raw).context("decoding message JSON")?;

    let mut values = Vec::with_capacity(mapping.schema.len());
    let mut key = String::new();
    for f in &mapping.schema {
        let v = lookup_path(&doc, &f.json_path)
            .ok_or_else(|| anyhow!("jsonPath {:?} not found", f.json_path))?;
        let coerced = coerce(v, &f.typ).with_context(|| format!("column {:?}", f.column))?;
        if f.column == mapping.key_field {
            key = coerced.as_key_string();
        }
        values.push(coerced);
    }
    Ok((values, key))
}

fn lookup_path<'a>(doc: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    let path = path.strip_prefix("$.").unwrap_or(path);
    let mut cur = doc;
    for part in path.split('.') {
        cur = cur.as_object()?.get(part)?;
    }
    Some(cur)
}

pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Renders `INSERT ... ON CONFLICT (keyField) DO UPDATE`, mirroring
/// kafkapg.go's buildUpsertSQL. Built once per run since the column set is
/// fixed for the life of the worker.
pub fn build_upsert_sql(table: &str, mapping: &SyncMapping) -> String {
    let ident = quote_ident(table);
    let cols: Vec<String> = mapping.schema.iter().map(|f| quote_ident(&f.column)).collect();
    let placeholders: Vec<String> = (1..=mapping.schema.len()).map(|i| format!("${i}")).collect();
    let updates: Vec<String> = mapping
        .schema
        .iter()
        .filter(|f| f.column != mapping.key_field)
        .map(|f| {
            let col = quote_ident(&f.column);
            format!("{col} = EXCLUDED.{col}")
        })
        .collect();

    let key_ident = quote_ident(&mapping.key_field);
    if updates.is_empty() {
        format!(
            "INSERT INTO {ident} ({}) VALUES ({}) ON CONFLICT ({key_ident}) DO NOTHING",
            cols.join(", "),
            placeholders.join(", "),
        )
    } else {
        format!(
            "INSERT INTO {ident} ({}) VALUES ({}) ON CONFLICT ({key_ident}) DO UPDATE SET {}",
            cols.join(", "),
            placeholders.join(", "),
            updates.join(", "),
        )
    }
}

/// Renders `DELETE FROM <table> WHERE <keyField> = $1`, used by the
/// lake->postgres pipeline to propagate Delta CDF delete rows (full CDF
/// fidelity — kafka->postgres has no delete path since Kafka messages are
/// append-only).
pub fn build_delete_sql(table: &str, mapping: &SyncMapping) -> String {
    format!(
        "DELETE FROM {} WHERE {} = $1",
        quote_ident(table),
        quote_ident(&mapping.key_field)
    )
}
