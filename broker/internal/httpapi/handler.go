// Package httpapi exposes the broker's public query endpoint. It resolves a
// query plan via the control plane's QueryPlanner, then dispatches the read
// to the store-specific executor(s) — a single postgres or lake executor
// for a plain projection, or both (stitched together per page) for a
// tiered one.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/andersonlaurentino/dal-broker/internal/executors/lake"
	"github.com/andersonlaurentino/dal-broker/internal/executors/postgres"
	"github.com/andersonlaurentino/dal-broker/internal/grpcclient"
	controlplanev1 "github.com/andersonlaurentino/dal-core/proto/gen/controlplane/v1"
)

const (
	defaultLimit = 20
	maxLimit     = 500

	minioAccessKeyID     = "dalminio"
	minioSecretAccessKey = "dalminio123"
)

type Handler struct {
	planner  *grpcclient.Client
	postgres *postgres.Executor
	lake     *lake.Executor
	log      *slog.Logger
}

func New(planner *grpcclient.Client, log *slog.Logger) *Handler {
	return &Handler{planner: planner, postgres: postgres.New(), lake: lake.New(), log: log}
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

	if plan.GetHistorical() != nil {
		h.handleTieredQuery(w, r.Context(), projection, plan, limit, offset)
		return
	}

	primary := plan.GetPrimary()
	columns, rows, total, err := h.queryStore(r.Context(), primary, "", limit, offset)
	if err != nil {
		h.log.Error("query failed", "projection", projection, "target", primary.GetTarget(), "err", err)
		writeError(w, http.StatusBadGateway, "failed to query projection: "+err.Error())
		return
	}

	writeResult(w, columns, rows, total, limit, offset, false)
}

// handleTieredQuery always fills the page to `limit` rows (when available)
// by stitching the tail of the postgres "recent" tier with the head of the
// lake "historical" tier's continuation, so pagination looks seamless to
// the caller regardless of where the retention cutover falls within a page.
func (h *Handler) handleTieredQuery(w http.ResponseWriter, ctx context.Context, projection string, plan *controlplanev1.ResolvePlanResponse, limit, offset int) {
	primary := plan.GetPrimary()
	historical := plan.GetHistorical()
	cutoverColumn := plan.GetCutoverColumn()
	cutoverBefore := time.Now().AddDate(0, 0, -int(plan.GetRetentionDays()))

	pgColumns, pgRows, pgTotal, err := h.postgres.Query(ctx, primary.GetDsn(), primary.GetTarget(), cutoverColumn, limit, offset)
	if err != nil {
		h.log.Error("tiered query: postgres tier failed", "projection", projection, "target", primary.GetTarget(), "err", err)
		writeError(w, http.StatusBadGateway, "failed to query projection: "+err.Error())
		return
	}

	rows := pgRows
	var columns any = pgColumns
	remaining := limit - len(pgRows)
	if remaining < 0 {
		remaining = 0
	}

	// Always query the lake tier, even for a page postgres alone fully
	// answers (remaining == 0): its total (rows strictly older than the
	// cutover) is needed to report the true combined total, not just what
	// postgres itself holds — otherwise early pages under-report `total`
	// and callers can't tell there's more data beyond postgres's window.
	// A limit of 0 still runs the lake executor's COUNT(*) but fetches no
	// rows, so this costs a query without pulling data we'd discard.
	lakeOffset := offset - int(pgTotal)
	if lakeOffset < 0 {
		lakeOffset = 0
	}
	lakeCfg := lake.Config{
		Endpoint:        historical.GetEndpoint(),
		Bucket:          historical.GetBucket(),
		AccessKeyID:     minioAccessKeyID,
		SecretAccessKey: minioSecretAccessKey,
	}
	lakeColumns, lakeRows, lakeOlderTotal, err := h.lake.Query(ctx, lakeCfg, historical.GetTarget(), cutoverColumn, &cutoverBefore, remaining, lakeOffset)
	if err != nil {
		h.log.Error("tiered query: lake tier failed", "projection", projection, "target", historical.GetTarget(), "err", err)
		writeError(w, http.StatusBadGateway, "failed to query projection: "+err.Error())
		return
	}
	rows = append(rows, lakeRows...)
	if len(pgColumns) == 0 {
		columns = lakeColumns
	}

	total := pgTotal + lakeOlderTotal
	writeResult(w, columns, rows, total, limit, offset, true)
}

// queryStore dispatches to the store-specific executor for a plain
// (untiered) projection. columns is returned as `any` since callers only
// need to JSON-encode it — postgres.Column and lake.Column already share
// the same {name,type} shape.
func (h *Handler) queryStore(ctx context.Context, sp *controlplanev1.StorePlan, orderBy string, limit, offset int) (any, [][]any, int64, error) {
	switch sp.GetStoreType() {
	case "postgres":
		return h.postgres.Query(ctx, sp.GetDsn(), sp.GetTarget(), orderBy, limit, offset)
	case "datalake":
		cfg := lake.Config{
			Endpoint:        sp.GetEndpoint(),
			Bucket:          sp.GetBucket(),
			AccessKeyID:     minioAccessKeyID,
			SecretAccessKey: minioSecretAccessKey,
		}
		return h.lake.Query(ctx, cfg, sp.GetTarget(), orderBy, nil, limit, offset)
	default:
		return nil, nil, 0, errUnsupportedStoreType(sp.GetStoreType())
	}
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
