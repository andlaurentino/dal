package v1alpha1

// Projection exposes a queryable logical view backed by one or more Syncs.
// Projections reference Syncs (not raw Connections) so the query planner
// can key off each source's own mapped schema.
type Projection struct {
	TypeMeta `yaml:",inline"`
	Metadata ObjectMeta     `json:"metadata" yaml:"metadata"`
	Spec     ProjectionSpec `json:"spec" yaml:"spec"`
}

type ProjectionSpec struct {
	Sources   []ProjectionSource  `json:"sources" yaml:"sources"`
	Queryable ProjectionQueryable `json:"queryable,omitempty" yaml:"queryable,omitempty"`
}

// ProjectionSource is one Sync this Projection reads from, with its own
// view of that Sync's data (View) and, when the Projection has more than
// one source, how queries route across them (Routing).
//
// This replaces the earlier design where exactly two sources were
// special-cased via spec.tiering{recentSyncRef,historicalSyncRef} and the
// cutover between them was borrowed from the recent Sync's own
// spec.retention. That coupled a query-routing decision (which source
// answers which page) to an ingest-time concern (how long workerd retains
// rows) — and had no answer at all for a Sync populated by some other
// means with no spec.retention set. Routing now belongs entirely to the
// Projection, and any number of sources can participate.
type ProjectionSource struct {
	SyncRef string `json:"syncRef" yaml:"syncRef"`
	// Routing decides which page of a query this source answers, relative
	// to the Projection's other sources. Required when spec.sources has
	// more than one entry; must be unset when there's exactly one (nothing
	// to route between).
	Routing *SourceRouting `json:"routing,omitempty" yaml:"routing,omitempty"`
	// View is this source's own queryable shape: which target table/path
	// to read and, optionally, which columns to expose (and under what
	// name). Every source names its own View.Table independently — there
	// is no single Projection-wide table override.
	View SourceView `json:"view" yaml:"view"`
}

// SourceRoutingType discriminates how a multi-source Projection routes
// queries across its sources. Only "timeRange" is implemented today;
// the type is a discriminated union from the start so a future
// "partition"-based routing type doesn't require another schema break.
type SourceRoutingType string

const (
	SourceRoutingTypeTimeRange SourceRoutingType = "timeRange"
)

// SourceRouting bounds a source's age window: it answers rows whose
// TimestampColumn value is between MinAge and MaxAge old, relative to
// query time. MinAge empty means "no lower bound" (the newest tier);
// MaxAge empty means "no upper bound" (the oldest tier). Across a
// Projection's sources these windows must partition time exactly — no
// gaps, no overlaps — validated at apply time.
type SourceRouting struct {
	Type            SourceRoutingType `json:"type" yaml:"type"`
	TimestampColumn string            `json:"timestampColumn" yaml:"timestampColumn"`
	// MinAge/MaxAge are Go duration strings (e.g. "168h"), always
	// non-negative.
	MinAge string `json:"minAge,omitempty" yaml:"minAge,omitempty"`
	MaxAge string `json:"maxAge,omitempty" yaml:"maxAge,omitempty"`
}

// SourceView is one source's queryable shape.
type SourceView struct {
	// Table is the target table name (postgres) or lake path (datalake)
	// to read — this source's own name, independent of any other
	// source's.
	Table string `json:"table" yaml:"table"`
	// Columns optionally selects and renames a subset of this source's
	// mapped columns. Omit for passthrough (every column the underlying
	// Sync's mapping produces, in its own order). When a Projection has
	// more than one source, every source's post-rename column set
	// (whether from Columns or passthrough) must match exactly, so a
	// stitched read returns one consistent shape.
	Columns []SourceColumn `json:"columns,omitempty" yaml:"columns,omitempty"`
}

// SourceColumn selects one column from a Sync's mapped output (Source,
// which must match a core/api/v1alpha1.SyncFieldMapping.Column in that
// Sync) and optionally renames it (As). Renaming only — no computed
// expressions in this slice.
type SourceColumn struct {
	Source string `json:"source" yaml:"source"`
	As     string `json:"as,omitempty" yaml:"as,omitempty"`
}

type ProjectionQueryable struct {
	DefaultLimit int64 `json:"defaultLimit,omitempty" yaml:"defaultLimit,omitempty"`
}
