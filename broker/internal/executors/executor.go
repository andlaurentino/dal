// Package executors defines the port broker's store-specific query
// implementations (postgres, lake) sit behind. Handler dispatches to
// whichever Executor is registered for a StorePlan's store type — adding a
// new target store means registering a new adapter that satisfies this
// interface, not adding a branch inside broker's query-handling logic.
package executors

import (
	"context"
	"time"
)

// Column describes one column of a query result, independent of which
// store produced it — postgres and lake results share this shape so
// callers (and JSON encoding) don't need to know which executor ran.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Column selects one output column: read Source from the store, expose it
// to callers as As. Both executors treat an empty Columns slice on
// QueryRequest as passthrough (every column, in the store's/mapping's own
// order, As equal to Source).
type ColumnSpec struct {
	Source string
	As     string
}

// QueryRequest carries every parameter any Executor might need. Fields
// irrelevant to a given store type are simply ignored by that executor
// (e.g. DSN is meaningless to the lake executor, Endpoint/Bucket/
// AccessKeyID/SecretAccessKey are meaningless to the postgres one) — this
// is the cost of one shared signature behind a single port, traded for not
// branching on store type outside the executors themselves.
type QueryRequest struct {
	// Table names the target table (postgres) or lake path (datalake).
	Table string

	// DSN is the postgres connection string. Postgres executor only.
	DSN string

	// Endpoint/Bucket/AccessKeyID/SecretAccessKey identify and
	// authenticate the S3-compatible datalake connection. Lake executor
	// only.
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string

	// Columns selects and renames a subset of this source's columns.
	// Empty means passthrough — every column, unrenamed.
	Columns []ColumnSpec

	// OrderBy, when non-empty, names a column to sort by descending
	// before pagination, and is what After/Before filter against — set
	// whenever this request is one of several sources in a multi-source
	// projection (the planner always pairs OrderBy with at least one of
	// After/Before in that case).
	OrderBy string

	// After/Before bound OrderBy's value: a row is included only if
	// (After == nil || value > After) && (Before == nil || value <= Before).
	// Both executors honor these — a source in the middle of a routing
	// chain can be bounded on both sides, not just the historical end.
	After  *time.Time
	Before *time.Time

	Limit  int
	Offset int
}

// QueryResult is a page of results plus the total row count matching the
// request (ignoring Limit/Offset), so callers can report pagination
// totals without a second round-trip.
type QueryResult struct {
	Columns []Column
	Rows    [][]any
	Total   int64
}

// Executor runs a paginated read against one store. postgres.Executor and
// lake.Executor both implement this.
type Executor interface {
	Query(ctx context.Context, req QueryRequest) (QueryResult, error)
}
