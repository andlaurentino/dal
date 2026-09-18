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
}

type SyncSource struct {
	ConnectionRef string `json:"connectionRef" yaml:"connectionRef"`
	Topic         string `json:"topic" yaml:"topic"`
}

type SyncTarget struct {
	ConnectionRef string `json:"connectionRef" yaml:"connectionRef"`
	Table         string `json:"table" yaml:"table"`
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
