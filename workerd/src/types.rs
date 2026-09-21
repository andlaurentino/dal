//! Mirrors the JSON shape of `core/api/v1alpha1`'s Go structs, as returned
//! by controlplane's REST API (the `spec` field of its envelope response).
//! Field names/casing must stay in sync with the Go `json:"..."` tags.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct SyncSpec {
    pub source: SyncSource,
    pub target: SyncTarget,
    pub mode: SyncMode,
    pub replication: SyncReplication,
    #[serde(default)]
    pub schedule: String,
    pub mapping: SyncMapping,
    #[serde(default, rename = "consumerGroup")]
    pub consumer_group: String,
    #[serde(default)]
    pub retention: Option<SyncRetention>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncRetention {
    pub days: i64,
    #[serde(rename = "timestampColumn")]
    pub timestamp_column: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncSource {
    #[serde(rename = "connectionRef")]
    pub connection_ref: String,
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub path: String,
    #[serde(default, rename = "checkpointPath")]
    pub checkpoint_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncTarget {
    #[serde(rename = "connectionRef")]
    pub connection_ref: String,
    #[serde(default)]
    pub table: String,
    #[serde(default)]
    pub path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    Streaming,
    Scheduled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncReplication {
    Delta,
    Snapshot,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncMapping {
    #[serde(rename = "keyField")]
    pub key_field: String,
    pub schema: Vec<SyncFieldMapping>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncFieldMapping {
    #[serde(rename = "jsonPath")]
    pub json_path: String,
    pub column: String,
    #[serde(rename = "type")]
    pub typ: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectionSpec {
    #[serde(rename = "type")]
    pub typ: ConnectionType,
    #[serde(default)]
    pub kafka: Option<KafkaConnection>,
    #[serde(default)]
    pub postgres: Option<PostgresConnection>,
    #[serde(default)]
    pub datalake: Option<DatalakeConnection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionType {
    Kafka,
    Postgres,
    Datalake,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConnection {
    pub brokers: Vec<String>,
    // Reserved for a future SASL wiring in kafkapostgres/kafkalake's
    // ClientConfig, same "declared but unused (inline-only) in this slice"
    // status as CredentialsRef on the Go-era ConnectionSpec types.
    #[serde(default, rename = "saslMechanism")]
    #[allow(dead_code)]
    pub sasl_mechanism: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConnection {
    pub dsn: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatalakeConnection {
    pub endpoint: String,
    pub bucket: String,
}
