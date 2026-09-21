package v1alpha1

import (
	"fmt"

	"github.com/robfig/cron/v3"
)

// This file covers schema-level and semantic validation only (required
// fields, enum values, intra-resource consistency). Referential validation
// (do referenced Connections/Syncs exist, do types match) is cross-resource
// and lives in the control plane's internal/validate package, which has
// access to the full resource store.

func validateTypeMeta(t TypeMeta, wantKind string) error {
	if t.APIVersion != APIVersion {
		return fmt.Errorf("apiVersion: expected %q, got %q", APIVersion, t.APIVersion)
	}
	if t.Kind != wantKind {
		return fmt.Errorf("kind: expected %q, got %q", wantKind, t.Kind)
	}
	return nil
}

func ValidateConnection(c *Connection) error {
	if err := validateTypeMeta(c.TypeMeta, KindConnection); err != nil {
		return err
	}
	if c.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}

	populated := 0
	if c.Spec.Kafka != nil {
		populated++
		if len(c.Spec.Kafka.Brokers) == 0 {
			return fmt.Errorf("spec.kafka.brokers must have at least one entry")
		}
	}
	if c.Spec.Postgres != nil {
		populated++
		if c.Spec.Postgres.DSN == "" {
			return fmt.Errorf("spec.postgres.dsn is required")
		}
	}
	if c.Spec.Datalake != nil {
		populated++
		if c.Spec.Datalake.Endpoint == "" || c.Spec.Datalake.Bucket == "" {
			return fmt.Errorf("spec.datalake.endpoint and spec.datalake.bucket are required")
		}
	}

	switch c.Spec.Type {
	case ConnectionTypeKafka:
		if c.Spec.Kafka == nil {
			return fmt.Errorf("spec.type is %q but spec.kafka is not set", c.Spec.Type)
		}
	case ConnectionTypePostgres:
		if c.Spec.Postgres == nil {
			return fmt.Errorf("spec.type is %q but spec.postgres is not set", c.Spec.Type)
		}
	case ConnectionTypeDatalake:
		if c.Spec.Datalake == nil {
			return fmt.Errorf("spec.type is %q but spec.datalake is not set", c.Spec.Type)
		}
	default:
		return fmt.Errorf("spec.type: unknown connection type %q", c.Spec.Type)
	}

	if populated != 1 {
		return fmt.Errorf("exactly one of spec.kafka, spec.postgres, spec.datalake must be set (got %d)", populated)
	}

	return nil
}

func ValidateSync(s *Sync) error {
	if err := validateTypeMeta(s.TypeMeta, KindSync); err != nil {
		return err
	}
	if s.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}
	if s.Spec.Source.ConnectionRef == "" {
		return fmt.Errorf("spec.source.connectionRef is required")
	}
	if (s.Spec.Source.Topic == "") == (s.Spec.Source.Path == "") {
		return fmt.Errorf("spec.source: exactly one of topic or path is required")
	}
	if s.Spec.Source.Path != "" && s.Spec.Source.CheckpointPath == "" {
		return fmt.Errorf("spec.source.checkpointPath is required when spec.source.path is set")
	}
	if s.Spec.Target.ConnectionRef == "" {
		return fmt.Errorf("spec.target.connectionRef is required")
	}
	if (s.Spec.Target.Table == "") == (s.Spec.Target.Path == "") {
		return fmt.Errorf("spec.target: exactly one of table or path is required")
	}

	switch s.Spec.Mode {
	case SyncModeStreaming:
	case SyncModeScheduled:
		if s.Spec.Schedule == "" {
			return fmt.Errorf("spec.schedule is required when spec.mode is %q", SyncModeScheduled)
		}
		if _, err := cron.ParseStandard(s.Spec.Schedule); err != nil {
			return fmt.Errorf("spec.schedule: invalid cron expression %q: %w", s.Spec.Schedule, err)
		}
	default:
		return fmt.Errorf("spec.mode: unknown mode %q", s.Spec.Mode)
	}

	switch s.Spec.Replication {
	case SyncReplicationDelta, SyncReplicationSnapshot:
	default:
		return fmt.Errorf("spec.replication: unknown value %q", s.Spec.Replication)
	}

	if s.Spec.Mapping.KeyField == "" {
		return fmt.Errorf("spec.mapping.keyField is required")
	}
	if len(s.Spec.Mapping.Schema) == 0 {
		return fmt.Errorf("spec.mapping.schema must have at least one field")
	}
	for i, f := range s.Spec.Mapping.Schema {
		if f.JSONPath == "" || f.Column == "" || f.Type == "" {
			return fmt.Errorf("spec.mapping.schema[%d]: jsonPath, column, and type are all required", i)
		}
	}

	if r := s.Spec.Retention; r != nil {
		if r.Days <= 0 {
			return fmt.Errorf("spec.retention.days must be positive")
		}
		if r.TimestampColumn == "" {
			return fmt.Errorf("spec.retention.timestampColumn is required")
		}
		var found bool
		for _, f := range s.Spec.Mapping.Schema {
			if f.Column == r.TimestampColumn {
				found = true
				if f.Type != "timestamptz" && f.Type != "timestamp" {
					return fmt.Errorf("spec.retention.timestampColumn %q must map to a timestamptz/timestamp column, got %q", r.TimestampColumn, f.Type)
				}
				break
			}
		}
		if !found {
			return fmt.Errorf("spec.retention.timestampColumn %q does not match any spec.mapping.schema[].column", r.TimestampColumn)
		}
	}

	return nil
}

func ValidateProjection(p *Projection) error {
	if err := validateTypeMeta(p.TypeMeta, KindProjection); err != nil {
		return err
	}
	if p.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}
	if len(p.Spec.Sources) == 0 {
		return fmt.Errorf("spec.sources must have at least one entry")
	}
	for i, src := range p.Spec.Sources {
		if src.SyncRef == "" {
			return fmt.Errorf("spec.sources[%d].syncRef is required", i)
		}
	}
	if p.Spec.Queryable.Table == "" {
		return fmt.Errorf("spec.queryable.table is required")
	}

	if t := p.Spec.Tiering; t != nil {
		if t.RecentSyncRef == "" || t.HistoricalSyncRef == "" {
			return fmt.Errorf("spec.tiering.recentSyncRef and spec.tiering.historicalSyncRef are both required")
		}
		if t.RecentSyncRef == t.HistoricalSyncRef {
			return fmt.Errorf("spec.tiering.recentSyncRef and spec.tiering.historicalSyncRef must be different syncs")
		}
		var haveRecent, haveHistorical bool
		for _, src := range p.Spec.Sources {
			if src.SyncRef == t.RecentSyncRef {
				haveRecent = true
			}
			if src.SyncRef == t.HistoricalSyncRef {
				haveHistorical = true
			}
		}
		if !haveRecent {
			return fmt.Errorf("spec.tiering.recentSyncRef %q must also appear in spec.sources", t.RecentSyncRef)
		}
		if !haveHistorical {
			return fmt.Errorf("spec.tiering.historicalSyncRef %q must also appear in spec.sources", t.HistoricalSyncRef)
		}
	}

	return nil
}
