package v1alpha1

// Projection exposes a queryable logical view backed by one or more Syncs.
// Projections reference Syncs (not raw Connections) so the query planner
// can key off each source's replication freshness/retention.
type Projection struct {
	TypeMeta `yaml:",inline"`
	Metadata ObjectMeta     `json:"metadata" yaml:"metadata"`
	Spec     ProjectionSpec `json:"spec" yaml:"spec"`
}

type ProjectionSpec struct {
	Sources   []ProjectionSource  `json:"sources" yaml:"sources"`
	Queryable ProjectionQueryable `json:"queryable" yaml:"queryable"`
}

type ProjectionSource struct {
	SyncRef string `json:"syncRef" yaml:"syncRef"`
	// MaxStalenessSeconds is informational in this slice: the planner does
	// not yet evaluate it, but the shape is real so future tiering logic
	// can key off it without a schema change.
	MaxStalenessSeconds int64 `json:"maxStalenessSeconds,omitempty" yaml:"maxStalenessSeconds,omitempty"`
}

type ProjectionQueryable struct {
	Table        string `json:"table" yaml:"table"`
	DefaultLimit int64  `json:"defaultLimit,omitempty" yaml:"defaultLimit,omitempty"`
}
