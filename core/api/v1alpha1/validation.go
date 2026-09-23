package v1alpha1

import (
	"fmt"
	"slices"
	"sort"
	"time"

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

	seenSyncRefs := make(map[string]bool, len(p.Spec.Sources))
	windows := make([]routingWindow, 0, len(p.Spec.Sources))

	for i, src := range p.Spec.Sources {
		if src.SyncRef == "" {
			return fmt.Errorf("spec.sources[%d].syncRef is required", i)
		}
		if seenSyncRefs[src.SyncRef] {
			return fmt.Errorf("spec.sources[%d].syncRef %q is a duplicate", i, src.SyncRef)
		}
		seenSyncRefs[src.SyncRef] = true

		if src.View.Table == "" {
			return fmt.Errorf("spec.sources[%d].view.table is required", i)
		}
		seenCols := make(map[string]bool, len(src.View.Columns))
		for j, c := range src.View.Columns {
			if c.Source == "" {
				return fmt.Errorf("spec.sources[%d].view.columns[%d].source is required", i, j)
			}
			name := c.As
			if name == "" {
				name = c.Source
			}
			if seenCols[name] {
				return fmt.Errorf("spec.sources[%d].view.columns[%d]: column %q is a duplicate (after rename)", i, j, name)
			}
			seenCols[name] = true
		}

		if len(p.Spec.Sources) == 1 {
			if src.Routing != nil {
				return fmt.Errorf("spec.sources[%d].routing must be unset when spec.sources has only one entry", i)
			}
			continue
		}

		r := src.Routing
		if r == nil {
			return fmt.Errorf("spec.sources[%d].routing is required when spec.sources has more than one entry", i)
		}
		if r.Type != SourceRoutingTypeTimeRange {
			return fmt.Errorf("spec.sources[%d].routing.type: unsupported type %q (only %q is implemented)", i, r.Type, SourceRoutingTypeTimeRange)
		}
		if r.TimestampColumn == "" {
			return fmt.Errorf("spec.sources[%d].routing.timestampColumn is required", i)
		}
		w := routingWindow{i: i}
		if r.MinAge != "" {
			d, err := time.ParseDuration(r.MinAge)
			if err != nil || d < 0 {
				return fmt.Errorf("spec.sources[%d].routing.minAge: invalid non-negative duration %q", i, r.MinAge)
			}
			w.min, w.hasMin = d, true
		}
		if r.MaxAge != "" {
			d, err := time.ParseDuration(r.MaxAge)
			if err != nil || d < 0 {
				return fmt.Errorf("spec.sources[%d].routing.maxAge: invalid non-negative duration %q", i, r.MaxAge)
			}
			w.max, w.hasMax = d, true
		}
		if w.hasMin && w.hasMax && w.min >= w.max {
			return fmt.Errorf("spec.sources[%d].routing: minAge (%s) must be less than maxAge (%s)", i, r.MinAge, r.MaxAge)
		}
		windows = append(windows, w)
	}

	if len(p.Spec.Sources) > 1 {
		if err := validateContiguousWindows(windows); err != nil {
			return err
		}
	}

	// Every source's post-rename column set must match exactly, so a
	// stitched multi-source read returns one consistent shape. Passthrough
	// sources (no view.columns) can't be checked here — their column list
	// only becomes known once the referenced Sync's mapping is resolved,
	// which requires store access; that check lives in
	// controlplane/internal/validate alongside the other referential
	// checks. Here we only catch the case where explicit column lists
	// disagree with each other.
	if len(p.Spec.Sources) > 1 {
		var want []string
		for i, src := range p.Spec.Sources {
			if len(src.View.Columns) == 0 {
				continue // passthrough; checked cross-resource
			}
			got := make([]string, len(src.View.Columns))
			for j, c := range src.View.Columns {
				name := c.As
				if name == "" {
					name = c.Source
				}
				got[j] = name
			}
			if want == nil {
				want = got
				continue
			}
			if !slices.Equal(want, got) {
				return fmt.Errorf("spec.sources[%d].view.columns: column set %v does not match an earlier source's %v — every source of a multi-source projection must expose the same columns, in the same order", i, got, want)
			}
		}
	}

	return nil
}

// routingWindow is one source's parsed [min,max) age bound, used only to
// check that a multi-source Projection's windows partition time exactly.
type routingWindow struct {
	i              int
	min, max       time.Duration
	hasMin, hasMax bool
}

// validateContiguousWindows checks that a multi-source Projection's
// routing windows partition time exactly: sorted by age (youngest first),
// each source's max age must equal the next source's min age, the
// youngest source must have no lower bound, and the oldest must have no
// upper bound.
func validateContiguousWindows(windows []routingWindow) error {
	sorted := make([]int, len(windows))
	for i := range sorted {
		sorted[i] = i
	}
	sort.Slice(sorted, func(a, b int) bool { return windows[sorted[a]].min < windows[sorted[b]].min })

	if windows[sorted[0]].hasMin {
		return fmt.Errorf("spec.sources[%d].routing.minAge must be unset — this source has the smallest minAge, so it must have no lower bound", windows[sorted[0]].i)
	}
	last := len(sorted) - 1
	if windows[sorted[last]].hasMax {
		return fmt.Errorf("spec.sources[%d].routing.maxAge must be unset — this source has the largest minAge, so it must have no upper bound", windows[sorted[last]].i)
	}
	for k := 0; k < last; k++ {
		cur, next := windows[sorted[k]], windows[sorted[k+1]]
		if !cur.hasMax || !next.hasMin || cur.max != next.min {
			return fmt.Errorf(
				"spec.sources[%d].routing.maxAge must equal spec.sources[%d].routing.minAge — routing windows must be contiguous with no gaps or overlaps",
				cur.i, next.i,
			)
		}
	}
	return nil
}
