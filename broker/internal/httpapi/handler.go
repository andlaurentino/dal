// Package httpapi exposes the broker's public query endpoint. It resolves a
// query plan via the control plane's QueryPlanner, then dispatches the read
// to the store-specific executor.
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/andersonlaurentino/dal-broker/internal/executors/postgres"
	"github.com/andersonlaurentino/dal-broker/internal/grpcclient"
)

const (
	defaultLimit = 50
	maxLimit     = 500
)

type Handler struct {
	planner  *grpcclient.Client
	postgres *postgres.Executor
	log      *slog.Logger
}

func New(planner *grpcclient.Client, log *slog.Logger) *Handler {
	return &Handler{planner: planner, postgres: postgres.New(), log: log}
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

	if plan.GetStoreType() != "postgres" {
		writeError(w, http.StatusBadRequest, "unsupported store type: "+plan.GetStoreType())
		return
	}

	columns, rows, total, err := h.postgres.Query(r.Context(), plan.GetDsn(), plan.GetTarget(), limit, offset)
	if err != nil {
		h.log.Error("query failed", "projection", projection, "target", plan.GetTarget(), "err", err)
		writeError(w, http.StatusBadGateway, "failed to query projection: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"columns": columns,
		"rows":    rows,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
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
