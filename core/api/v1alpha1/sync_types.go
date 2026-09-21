package v1alpha1

// SyncMode indicates whether the sync runs continuously or on a schedule.
type SyncMode string

const (
	SyncModeStreaming SyncMode = "streaming"
	SyncModeScheduled SyncMode = "scheduled"
)

// SyncReplication indicates whether each run replicates only changes
// (delta) or the full source dataset (snapshot).
type SyncReplication string

const (
	SyncReplicationDelta    SyncReplication = "delta"
	SyncReplicationSnapshot SyncReplication = "snapshot"
)

// Sync represents one data replication job between a source and a target
// Connection.
type Sync struct {
	TypeMeta `yaml:",inline"`
	Metadata ObjectMeta `json:"metadata" yaml:"metadata"`
	Spec     SyncSpec   `json:"spec" yaml:"spec"`
}

type SyncSpec struct {
	Source      SyncSource      `json:"source" yaml:"source"`
	Target      SyncTarget      `json:"target" yaml:"target"`
	Mode        SyncMode        `json:"mode" yaml:"mode"`
	Replication SyncReplication `json:"replication" yaml:"replication"`
	// Schedule is a cron expression, required when Mode == SyncModeScheduled.
	Schedule string      `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	Mapping  SyncMapping `json:"mapping" yaml:"mapping"`
	// ConsumerGroup defaults to "dal-worker-{metadata.name}" when empty.
	ConsumerGroup string `json:"consumerGroup,omitempty" yaml:"consumerGroup,omitempty"`
	// Retention prunes rows older than Days from a postgres target,
	// enforced by workerd via periodic DELETEs. Only valid when the target
	// connection is postgres. A Projection's spec.tiering reuses this same
	// window as its recent/historical cutover rather than duplicating it.
	Retention *SyncRetention `json:"retention,omitempty" yaml:"retention,omitempty"`
}

type SyncRetention struct {
	Days int64 `json:"days" yaml:"days"`
	// TimestampColumn must name a column in Mapping.Schema of type
	// timestamptz/timestamp.
	TimestampColumn string `json:"timestampColumn" yaml:"timestampColumn"`
}

// SyncSource identifies the readable side of a Sync. Exactly one of Topic
// (kafka connections) or Path (datalake connections) applies, depending on
// the referenced Connection's type. CheckpointPath is required alongside
// Path: it's the folder in the same datalake bucket where the worker
// persists its Change Data Feed read cursor (the last-processed Delta table
// version), since — unlike a Kafka consumer group — Delta versions aren't
// tracked by an external broker.
type SyncSource struct {
	ConnectionRef  string `json:"connectionRef" yaml:"connectionRef"`
	Topic          string `json:"topic,omitempty" yaml:"topic,omitempty"`
	Path           string `json:"path,omitempty" yaml:"path,omitempty"`
	CheckpointPath string `json:"checkpointPath,omitempty" yaml:"checkpointPath,omitempty"`
}

// SyncTarget identifies the writable side of a Sync. Exactly one of Table
// (postgres connections) or Path (datalake connections) applies, depending
// on the referenced Connection's type.
type SyncTarget struct {
	ConnectionRef string `json:"connectionRef" yaml:"connectionRef"`
	Table         string `json:"table,omitempty" yaml:"table,omitempty"`
	Path          string `json:"path,omitempty" yaml:"path,omitempty"`
}

type SyncMapping struct {
	// KeyField is the JSON field (by column name after mapping) used as the
	// upsert primary key.
	KeyField string             `json:"keyField" yaml:"keyField"`
	Schema   []SyncFieldMapping `json:"schema" yaml:"schema"`
}

type SyncFieldMapping struct {
	JSONPath string `json:"jsonPath" yaml:"jsonPath"`
	Column   string `json:"column" yaml:"column"`
	Type     string `json:"type" yaml:"type"`
}
