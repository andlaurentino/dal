// Package planner resolves which store/connection/table(s) should answer a
// query against a named Projection, by walking Projection -> Connection
// through internal/store. A single-source projection resolves to one
// StorePlan. A multi-source projection resolves to one StorePlan per
// source, ordered youngest-to-oldest by its routing window, plus the age
// bounds and column view broker needs to route and stitch pages across
// them.
package planner

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/andersonlaurentino/dal-controlplane/internal/store"
	v1alpha1 "github.com/andersonlaurentino/dal-core/api/v1alpha1"
)

// ColumnPlan is one resolved output column: read Source from the
// underlying store, expose it to callers as As (equal to Source for
// passthrough).
type ColumnPlan struct {
	Source string
	As     string
}

// StorePlan is everything broker needs to execute a read against one
// resolved store.
type StorePlan struct {
	StoreType     string
	ConnectionRef string
	Target        string
	DSN           string
	Endpoint      string
	Bucket        string

	// OrderBy/MinAge/MaxAge are only set when this StorePlan is one of
	// several in a multi-source Plan — they're how broker filters and
	// orders this source's rows relative to the others. OrderBy names the
	// timestamp column; MinAge/MaxAge are nil when that side is
	// unbounded (the youngest source has no MinAge, the oldest has no
	// MaxAge).
	OrderBy string
	MinAge  *time.Duration
	MaxAge  *time.Duration

	// Columns is this source's resolved view, in order.
	Columns []ColumnPlan
}

// Plan is what broker needs to execute a read on behalf of a Projection
// query. Sources has exactly one entry for a single-source projection
// (no age bounds), or one entry per source — ordered youngest to
// oldest — for a multi-source one.
type Plan struct {
	Sources []StorePlan
}

func Resolve(ctx context.Context, st *store.Store, projectionName string) (*Plan, error) {
	p, err := getProjection(ctx, st, projectionName)
	if err != nil {
		return nil, err
	}
	if len(p.Spec.Sources) == 0 {
		return nil, fmt.Errorf("projection %q has no sources", projectionName)
	}

	sources := make([]StorePlan, len(p.Spec.Sources))
	for i, src := range p.Spec.Sources {
		sp, err := storePlanFor(ctx, st, src)
		if err != nil {
			return nil, err
		}
		sources[i] = *sp
	}

	if len(sources) > 1 {
		sort.Slice(sources, func(a, b int) bool {
			return age(sources[a].MinAge) < age(sources[b].MinAge)
		})
	}

	return &Plan{Sources: sources}, nil
}

func age(d *time.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return *d
}

// storePlanFor resolves one Projection source's Connection + view/routing
// into a StorePlan.
func storePlanFor(ctx context.Context, st *store.Store, src v1alpha1.ProjectionSource) (*StorePlan, error) {
	c, err := getConnection(ctx, st, src.ConnectionRef)
	if err != nil {
		return nil, err
	}

	sp := &StorePlan{
		StoreType:     string(c.Spec.Type),
		ConnectionRef: src.ConnectionRef,
		Target:        src.View.Table,
		Columns:       columnsFor(src),
	}

	switch c.Spec.Type {
	case v1alpha1.ConnectionTypePostgres:
		if c.Spec.Postgres == nil {
			return nil, fmt.Errorf("connection %q has type postgres but spec.postgres is not set", src.ConnectionRef)
		}
		sp.DSN = c.Spec.Postgres.DSN
	case v1alpha1.ConnectionTypeDatalake:
		if c.Spec.Datalake == nil {
			return nil, fmt.Errorf("connection %q has type datalake but spec.datalake is not set", src.ConnectionRef)
		}
		sp.Endpoint = c.Spec.Datalake.Endpoint
		sp.Bucket = c.Spec.Datalake.Bucket
	default:
		return nil, fmt.Errorf("connection %q has type %q, which is not queryable", src.ConnectionRef, c.Spec.Type)
	}

	if r := src.Routing; r != nil {
		sp.OrderBy = r.TimestampColumn
		if r.MinAge != "" {
			d, err := v1alpha1.ParseAge(r.MinAge)
			if err != nil {
				return nil, fmt.Errorf("source %q: invalid routing.minAge %q: %w", src.ConnectionRef, r.MinAge, err)
			}
			sp.MinAge = &d
		}
		if r.MaxAge != "" {
			d, err := v1alpha1.ParseAge(r.MaxAge)
			if err != nil {
				return nil, fmt.Errorf("source %q: invalid routing.maxAge %q: %w", src.ConnectionRef, r.MaxAge, err)
			}
			sp.MaxAge = &d
		}
	}

	return sp, nil
}

// columnsFor resolves a source's final Source->As column list from its
// own view.columns — always set, since a Connection carries no schema to
// pass through from.
func columnsFor(src v1alpha1.ProjectionSource) []ColumnPlan {
	cols := make([]ColumnPlan, len(src.View.Columns))
	for i, c := range src.View.Columns {
		as := c.As
		if as == "" {
			as = c.Source
		}
		cols[i] = ColumnPlan{Source: c.Source, As: as}
	}
	return cols
}

func getProjection(ctx context.Context, st *store.Store, name string) (*v1alpha1.Projection, error) {
	raw, err := st.Get(ctx, store.KindProjection, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("projection %q does not exist", name)
	}
	if err != nil {
		return nil, err
	}
	return v1alpha1.DecodeProjection(raw)
}

func getConnection(ctx context.Context, st *store.Store, name string) (*v1alpha1.Connection, error) {
	raw, err := st.Get(ctx, store.KindConnection, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("connection %q does not exist", name)
	}
	if err != nil {
		return nil, err
	}
	return v1alpha1.DecodeConnection(raw)
}
