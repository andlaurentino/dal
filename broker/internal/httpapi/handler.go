// Package httpapi exposes the broker's public query endpoint. It resolves a
// query plan via the control plane's QueryPlanner, then queries each
// resolved source in order (youngest to oldest), stitching pages across
// them so pagination looks seamless regardless of how many sources answer
// a Projection or where a page's boundary falls relative to their routing
// windows. A single-source Projection is just the one-source case of the
// same loop — no separate code path.
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/andersonlaurentino/dal-broker/internal/executors"
	"github.com/andersonlaurentino/dal-broker/internal/executors/lake"
	"github.com/andersonlaurentino/dal-broker/internal/executors/postgres"
	"github.com/andersonlaurentino/dal-broker/internal/grpcclient"
	controlplanev1 "github.com/andersonlaurentino/dal-core/proto/gen/controlplane/v1"
)

const (
	defaultLimit = 20
	maxLimit     = 500
)

type Handler struct {
	planner       *grpcclient.Client
	stores        map[string]executors.Executor
	s3AccessKeyID string
	s3SecretKey   string
	log           *slog.Logger
}

func New(planner *grpcclient.Client, log *slog.Logger) (*Handler, error) {
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("S3_ACCESS_KEY and S3_SECRET_KEY must both be set")
	}
	return &Handler{
		planner: planner,
		stores: map[string]executors.Executor{
			"postgres": postgres.New(),
			"datalake": lake.New(),
		},
		s3AccessKeyID: accessKey,
		s3SecretKey:   secretKey,
		log:           log,
	}, nil
}

func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("GET /query/{projection}", h.handleQuery)
}

func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	projection := r.PathValue("projection")

	limit := queryInt(r, "limit", defaultLimit)
	if limit <= 0 || limit > maxLimit {
		limit = defaultLimit
	}
	offset := queryInt(r, "offset", 0)
	if offset < 0 {
		offset = 0
	}

	plan, err := h.planner.ResolvePlan(r.Context(), projection)
	if err != nil {
		h.log.Error("resolve plan failed", "projection", projection, "err", err)
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	sources := plan.GetSources()
	if len(sources) == 0 {
		h.log.Error("resolve plan returned no sources", "projection", projection)
		writeError(w, http.StatusBadGateway, "projection has no resolvable sources")
		return
	}

	// Sources are already ordered youngest to oldest by the planner. Query
	// each in turn, carrying the remaining limit/offset forward — a source
	// fully skipped by offset (or fully consumed by limit) is still
	// queried (with limit possibly 0) so its Total contributes to the
	// grand total: without that, early pages would under-report `total`
	// and callers couldn't tell there's more data in a later source. A
	// limit-0 query still runs the underlying COUNT(*) but fetches no
	// rows, so this costs a query without pulling data we'd discard.
	now := time.Now()
	remainingLimit, remainingOffset := limit, offset
	var rows [][]any
	var columns []executors.Column
	var total int64

	for _, sp := range sources {
		after, before := ageBounds(sp, now)

		srcLimit := max(remainingLimit, 0)
		srcOffset := max(remainingOffset, 0)

		result, err := h.queryStore(r.Context(), sp, after, before, srcLimit, srcOffset)
		if err != nil {
			h.log.Error("query failed", "projection", projection, "target", sp.GetTarget(), "err", err)
			writeError(w, http.StatusBadGateway, "failed to query projection: "+err.Error())
			return
		}

		total += result.Total
		if columns == nil {
			columns = result.Columns
		}
		rows = append(rows, result.Rows...)

		remainingOffset -= int(result.Total)
		remainingLimit = limit - len(rows)
	}

	writeResult(w, columns, rows, total, limit, offset, len(sources) > 1)
}

// ageBounds converts a StorePlan's age window into absolute time bounds
// for executors.QueryRequest.After/Before, relative to now. See
// executors.QueryRequest's doc comment for the exact (after, before]
// semantics; the derivation: a row is in this source's window when
// MinAge <= age(row) < MaxAge, and age(row) = now - timestamp, which
// rearranges to timestamp in (now-MaxAge, now-MinAge].
func ageBounds(sp *controlplanev1.StorePlan, now time.Time) (after, before *time.Time) {
	if sp.GetHasMaxAge() {
		t := now.Add(-time.Duration(sp.GetMaxAgeSeconds()) * time.Second)
		after = &t
	}
	if sp.GetHasMinAge() {
		t := now.Add(-time.Duration(sp.GetMinAgeSeconds()) * time.Second)
		before = &t
	}
	return after, before
}

// queryStore dispatches to whichever Executor is registered for sp's store
// type — the only place broker branches on store type at all, and it's a
// map lookup, not a switch: adding a new target store means registering a
// new executors.Executor adapter in New, not editing this function.
func (h *Handler) queryStore(ctx context.Context, sp *controlplanev1.StorePlan, after, before *time.Time, limit, offset int) (executors.QueryResult, error) {
	ex, ok := h.stores[sp.GetStoreType()]
	if !ok {
		return executors.QueryResult{}, errUnsupportedStoreType(sp.GetStoreType())
	}

	cols := sp.GetColumns()
	colSpec := make([]executors.ColumnSpec, len(cols))
	for i, c := range cols {
		colSpec[i] = executors.ColumnSpec{Source: c.GetSource(), As: c.GetAs()}
	}

	return ex.Query(ctx, executors.QueryRequest{
		DSN:             sp.GetDsn(),
		Table:           sp.GetTarget(),
		Endpoint:        sp.GetEndpoint(),
		Bucket:          sp.GetBucket(),
		AccessKeyID:     h.s3AccessKeyID,
		SecretAccessKey: h.s3SecretKey,
		Columns:         colSpec,
		OrderBy:         sp.GetOrderBy(),
		After:           after,
		Before:          before,
		Limit:           limit,
		Offset:          offset,
	})
}

type errUnsupportedStoreType string

func (e errUnsupportedStoreType) Error() string { return "unsupported store type: " + string(e) }

func writeResult(w http.ResponseWriter, columns any, rows [][]any, total int64, limit, offset int, tiered bool) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"columns": columns,
		"rows":    rows,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
		"tiered":  tiered,
	})
}

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
