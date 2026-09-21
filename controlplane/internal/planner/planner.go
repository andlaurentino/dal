// Package planner resolves which store/connection/table should answer a
// query against a named Projection, by walking Projection -> Sync ->
// Connection through internal/store. A plain projection resolves to a
// single store (postgres or datalake). A tiered projection (spec.tiering)
// resolves to two: a retention-limited postgres "primary" and a
// full-history datalake "historical", plus the retention window broker
// needs to route pages between them.
package planner

import (
	"context"
	"errors"
	"fmt"

	"github.com/andersonlaurentino/dal-controlplane/internal/store"
	v1alpha1 "github.com/andersonlaurentino/dal-core/api/v1alpha1"
)

// StorePlan is everything broker needs to execute a read against one
// resolved store.
type StorePlan struct {
	StoreType     string
	ConnectionRef string
	Target        string
	DSN           string
	Endpoint      string
	Bucket        string
}

// Plan is what the broker needs to execute a read on behalf of a
// Projection query. Historical/CutoverColumn/RetentionDays are only set
// for a tiered projection.
type Plan struct {
	Primary          StorePlan
	Historical       *StorePlan
	CutoverColumn    string
	RetentionDays    int64
	StalenessSeconds int64
}

func Resolve(ctx context.Context, st *store.Store, projectionName string) (*Plan, error) {
	p, err := getProjection(ctx, st, projectionName)
	if err != nil {
		return nil, err
	}
	if len(p.Spec.Sources) == 0 {
		return nil, fmt.Errorf("projection %q has no sources", projectionName)
	}

	if p.Spec.Tiering != nil {
		return resolveTiered(ctx, st, p)
	}
	return resolveSingle(ctx, st, p)
}

func resolveSingle(ctx context.Context, st *store.Store, p *v1alpha1.Projection) (*Plan, error) {
	syncRef := p.Spec.Sources[0].SyncRef

	s, err := getSync(ctx, st, syncRef)
	if err != nil {
		return nil, err
	}

	primary, err := storePlanFor(ctx, st, s, p.Spec.Queryable.Table)
	if err != nil {
		return nil, err
	}

	return &Plan{Primary: *primary}, nil
}

func resolveTiered(ctx context.Context, st *store.Store, p *v1alpha1.Projection) (*Plan, error) {
	t := p.Spec.Tiering

	recentSync, err := getSync(ctx, st, t.RecentSyncRef)
	if err != nil {
		return nil, err
	}
	if recentSync.Spec.Retention == nil {
		return nil, fmt.Errorf("sync %q (tiering.recentSyncRef) has no spec.retention", t.RecentSyncRef)
	}

	historicalSync, err := getSync(ctx, st, t.HistoricalSyncRef)
	if err != nil {
		return nil, err
	}

	primary, err := storePlanFor(ctx, st, recentSync, p.Spec.Queryable.Table)
	if err != nil {
		return nil, err
	}
	historical, err := storePlanFor(ctx, st, historicalSync, "")
	if err != nil {
		return nil, err
	}

	return &Plan{
		Primary:       *primary,
		Historical:    historical,
		CutoverColumn: recentSync.Spec.Retention.TimestampColumn,
		RetentionDays: recentSync.Spec.Retention.Days,
	}, nil
}

// storePlanFor resolves one Sync's target Connection into a StorePlan.
// queryableTable overrides the target's own table name when set (mirrors
// the pre-tiering behavior of Spec.Queryable.Table taking precedence).
func storePlanFor(ctx context.Context, st *store.Store, s *v1alpha1.Sync, queryableTable string) (*StorePlan, error) {
	c, err := getConnection(ctx, st, s.Spec.Target.ConnectionRef)
	if err != nil {
		return nil, err
	}

	switch c.Spec.Type {
	case v1alpha1.ConnectionTypePostgres:
		if c.Spec.Postgres == nil {
			return nil, fmt.Errorf("connection %q has type postgres but spec.postgres is not set", s.Spec.Target.ConnectionRef)
		}
		target := queryableTable
		if target == "" {
			target = s.Spec.Target.Table
		}
		return &StorePlan{
			StoreType:     string(c.Spec.Type),
			ConnectionRef: s.Spec.Target.ConnectionRef,
			Target:        target,
			DSN:           c.Spec.Postgres.DSN,
		}, nil
	case v1alpha1.ConnectionTypeDatalake:
		if c.Spec.Datalake == nil {
			return nil, fmt.Errorf("connection %q has type datalake but spec.datalake is not set", s.Spec.Target.ConnectionRef)
		}
		target := queryableTable
		if target == "" {
			target = s.Spec.Target.Path
		}
		return &StorePlan{
			StoreType:     string(c.Spec.Type),
			ConnectionRef: s.Spec.Target.ConnectionRef,
			Target:        target,
			Endpoint:      c.Spec.Datalake.Endpoint,
			Bucket:        c.Spec.Datalake.Bucket,
		}, nil
	default:
		return nil, fmt.Errorf("connection %q has type %q, which is not queryable", s.Spec.Target.ConnectionRef, c.Spec.Type)
	}
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

func getSync(ctx context.Context, st *store.Store, name string) (*v1alpha1.Sync, error) {
	raw, err := st.Get(ctx, store.KindSync, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("sync %q does not exist", name)
	}
	if err != nil {
		return nil, err
	}
	return v1alpha1.DecodeSync(raw)
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
