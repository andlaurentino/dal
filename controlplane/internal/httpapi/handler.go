// Package httpapi is the control plane's external REST API: kubectl-apply
// style resource management for Connection/Sync/Projection, backed by
// internal/store (Redis) and internal/validate (referential checks).
package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/andersonlaurentino/dal-controlplane/internal/store"
	"github.com/andersonlaurentino/dal-controlplane/internal/validate"
	"github.com/andersonlaurentino/dal-controlplane/internal/workermgr"
	v1alpha1 "github.com/andersonlaurentino/dal-core/api/v1alpha1"
)

type Handler struct {
	store     *store.Store
	validator *validate.Validator
	workers   *workermgr.Manager
	log       *slog.Logger
}

func New(s *store.Store, v *validate.Validator, w *workermgr.Manager, log *slog.Logger) *Handler {
	return &Handler{store: s, validator: v, workers: w, log: log}
}

func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1alpha1/apply", h.handleApply)
	for path, cfg := range kindConfigs {
		mux.HandleFunc("GET /api/v1alpha1/"+path, h.handleList(cfg))
		mux.HandleFunc("GET /api/v1alpha1/"+path+"/{name}", h.handleGet(cfg))
		mux.HandleFunc("DELETE /api/v1alpha1/"+path+"/{name}", h.handleDelete(cfg))
	}
	mux.HandleFunc("GET /api/v1alpha1/workers", h.handleListWorkers)
}

// kindConfig describes how to decode a resource kind, extract its name and
// spec (for the response envelope), and which Redis "kind" bucket it lives
// in. Registering handlers through this table, rather than writing three
// near-identical sets of handlers, keeps the per-kind differences to just
// this data.
type kindConfig struct {
	yamlKind  string // v1alpha1.Kind{Connection,Sync,Projection}
	storeKind string // store.Kind{Connection,Sync,Projection}
	decode    func(raw []byte) (name string, spec any, err error)
	// validateRefs runs referential validation after schema decode succeeds.
	// nil for kinds with nothing to check (Connection).
	validateRefs func(ctx context.Context, v *validate.Validator, raw []byte) error
}

var kindConfigs = map[string]kindConfig{
	"connections": {
		yamlKind:  v1alpha1.KindConnection,
		storeKind: store.KindConnection,
		decode: func(raw []byte) (string, any, error) {
			c, err := v1alpha1.DecodeConnection(raw)
			if err != nil {
				return "", nil, err
			}
			return c.Metadata.Name, c.Spec, nil
		},
	},
	"syncs": {
		yamlKind:  v1alpha1.KindSync,
		storeKind: store.KindSync,
		decode: func(raw []byte) (string, any, error) {
			s, err := v1alpha1.DecodeSync(raw)
			if err != nil {
				return "", nil, err
			}
			return s.Metadata.Name, s.Spec, nil
		},
		validateRefs: func(ctx context.Context, v *validate.Validator, raw []byte) error {
			s, err := v1alpha1.DecodeSync(raw)
			if err != nil {
				return err
			}
			return v.ValidateSync(ctx, s)
		},
	},
	"projections": {
		yamlKind:  v1alpha1.KindProjection,
		storeKind: store.KindProjection,
		decode: func(raw []byte) (string, any, error) {
			p, err := v1alpha1.DecodeProjection(raw)
			if err != nil {
				return "", nil, err
			}
			return p.Metadata.Name, p.Spec, nil
		},
		validateRefs: func(ctx context.Context, v *validate.Validator, raw []byte) error {
			p, err := v1alpha1.DecodeProjection(raw)
			if err != nil {
				return err
			}
			return v.ValidateProjection(ctx, p)
		},
	},
}

func (h *Handler) handleApply(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, codeSchemaInvalid, "failed to read request body")
		return
	}

	yamlKind, err := v1alpha1.ParseKind(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, codeSchemaInvalid, err.Error())
		return
	}

	cfg, ok := kindConfigForYAMLKind(yamlKind)
	if !ok {
		writeError(w, http.StatusBadRequest, codeSchemaInvalid, "unknown kind: "+yamlKind)
		return
	}

	name, spec, err := cfg.decode(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, codeSchemaInvalid, err.Error())
		return
	}

	if cfg.validateRefs != nil {
		if err := cfg.validateRefs(r.Context(), h.validator, raw); err != nil {
			writeError(w, http.StatusBadRequest, codeReferenceInvalid, err.Error())
			return
		}
	}

	if err := h.store.Put(r.Context(), cfg.storeKind, name, raw); err != nil {
		h.log.Error("store put failed", "kind", yamlKind, "name", name, "err", err)
		writeError(w, http.StatusInternalServerError, codeInternal, "failed to store resource")
		return
	}

	if cfg.storeKind == store.KindSync {
		h.workers.Notify(name)
	}

	writeJSON(w, http.StatusOK, okResponse{Kind: yamlKind, Name: name, Spec: spec, Raw: string(raw)})
}

func (h *Handler) handleList(cfg kindConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		names, err := h.store.List(r.Context(), cfg.storeKind)
		if err != nil {
			h.log.Error("store list failed", "kind", cfg.storeKind, "err", err)
			writeError(w, http.StatusInternalServerError, codeInternal, "failed to list resources")
			return
		}

		items := make([]okResponse, 0, len(names))
		for _, name := range names {
			raw, err := h.store.Get(r.Context(), cfg.storeKind, name)
			if err != nil {
				h.log.Error("store get failed during list", "kind", cfg.storeKind, "name", name, "err", err)
				continue
			}
			_, spec, err := cfg.decode(raw)
			if err != nil {
				h.log.Error("decode failed during list", "kind", cfg.storeKind, "name", name, "err", err)
				continue
			}
			items = append(items, okResponse{Kind: cfg.yamlKind, Name: name, Spec: spec, Raw: string(raw)})
		}

		writeJSON(w, http.StatusOK, listResponse{Items: items})
	}
}

func (h *Handler) handleGet(cfg kindConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		raw, err := h.store.Get(r.Context(), cfg.storeKind, name)
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, codeNotFound, "resource not found")
			return
		}
		if err != nil {
			h.log.Error("store get failed", "kind", cfg.storeKind, "name", name, "err", err)
			writeError(w, http.StatusInternalServerError, codeInternal, "failed to fetch resource")
			return
		}

		_, spec, err := cfg.decode(raw)
		if err != nil {
			h.log.Error("decode failed", "kind", cfg.storeKind, "name", name, "err", err)
			writeError(w, http.StatusInternalServerError, codeInternal, "stored resource failed to decode")
			return
		}

		writeJSON(w, http.StatusOK, okResponse{Kind: cfg.yamlKind, Name: name, Spec: spec, Raw: string(raw)})
	}
}

func (h *Handler) handleDelete(cfg kindConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if err := h.store.Delete(r.Context(), cfg.storeKind, name); err != nil {
			h.log.Error("store delete failed", "kind", cfg.storeKind, "name", name, "err", err)
			writeError(w, http.StatusInternalServerError, codeInternal, "failed to delete resource")
			return
		}
		if cfg.storeKind == store.KindSync {
			h.workers.Notify(name)
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h *Handler) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	statuses, err := h.workers.ListWorkerStatus(r.Context())
	if err != nil {
		h.log.Error("listing worker status failed", "err", err)
		writeError(w, http.StatusInternalServerError, codeInternal, "failed to list workers")
		return
	}

	items := make([]workerStatusResponse, len(statuses))
	for i, ws := range statuses {
		items[i] = workerStatusResponse{
			SyncName:        ws.SyncName,
			PodPhase:        ws.PodPhase,
			Ready:           ws.Ready,
			Restarts:        ws.Restarts,
			Alive:           ws.Alive,
			ObservedPhase:   ws.ObservedPhase,
			LastHeartbeatAt: ws.LastHeartbeatAt,
			ConsumerLag:     ws.ConsumerLag,
			Watermark:       ws.Watermark,
			LastError:       ws.LastError,
		}
	}
	writeJSON(w, http.StatusOK, workerStatusListResponse{Items: items})
}

func kindConfigForYAMLKind(yamlKind string) (kindConfig, bool) {
	for _, cfg := range kindConfigs {
		if cfg.yamlKind == yamlKind {
			return cfg, true
		}
	}
	return kindConfig{}, false
}
