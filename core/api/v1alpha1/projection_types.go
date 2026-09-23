package v1alpha1

// Projection exposes a queryable logical view backed by one or more
// Connections, read directly — independent of whatever Sync (if any) wrote
// to them.
type Projection struct {
	TypeMeta `yaml:",inline"`
	Metadata ObjectMeta     `json:"metadata" yaml:"metadata"`
	Spec     ProjectionSpec `json:"spec" yaml:"spec"`
}

type ProjectionSpec struct {
	Sources []ProjectionSource `json:"sources" yaml:"sources"`
}

// ProjectionSource is one Connection this Projection reads from directly,
// with its own view of that Connection's data (View) and, when the
// Projection has more than one source, how queries route across them
// (Routing).
//
// This replaces the earlier design where exactly two sources were
// special-cased via spec.tiering{recentSyncRef,historicalSyncRef} and the
// cutover between them was borrowed from the recent Sync's own
// spec.retention. That coupled a query-routing decision (which source
// answers which page) to an ingest-time concern (how long workerd retains
// rows) — and had no answer at all for a Sync populated by some other
// means with no spec.retention set. Routing now belongs entirely to the
// Projection, and any number of sources can participate.
//
// Sources name a Connection, not a Sync: a Projection's query-time shape
// shouldn't depend on which ingestion job (if any) populated the
// underlying table/path, and a Connection may be written to by more than
// one Sync, or by nothing at all (e.g. a hand-seeded table).
type ProjectionSource struct {
	ConnectionRef string `json:"connectionRef" yaml:"connectionRef"`
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
	// MinAge/MaxAge are duration strings, always non-negative: either a Go
	// duration (e.g. "168h", "30m") or a day count suffixed with "d" (e.g.
	// "7d"), parsed by ParseAge.
	MinAge string `json:"minAge,omitempty" yaml:"minAge,omitempty"`
	MaxAge string `json:"maxAge,omitempty" yaml:"maxAge,omitempty"`
}

// SourceView is one source's queryable shape.
type SourceView struct {
	// Table is the target table name (postgres) or lake path (datalake)
	// to read — this source's own name, independent of any other
	// source's.
	Table string `json:"table" yaml:"table"`
	// Columns selects and optionally renames the columns this source
	// exposes — required, at least one entry. There is no passthrough:
	// a Connection carries no schema to pass through from, so every
	// column a Projection reads must be named explicitly. When a
	// Projection has more than one source, every source's post-rename
	// column set must match exactly, so a stitched read returns one
	// consistent shape.
	Columns []SourceColumn `json:"columns" yaml:"columns"`
}

// SourceColumn selects one column from the underlying table/path (Source)
// and optionally renames it (As). Renaming only — no computed expressions
// in this slice.
type SourceColumn struct {
	Source string `json:"source" yaml:"source"`
	As     string `json:"as,omitempty" yaml:"as,omitempty"`
}
