package store

// Kind strings used as the Redis key-schema "kind" segment (lowercase,
// distinct from v1alpha1.Kind{Connection,Sync,Projection} which are the
// YAML `kind:` field values).
const (
	KindConnection = "connection"
	KindSync       = "sync"
	KindProjection = "projection"
)
