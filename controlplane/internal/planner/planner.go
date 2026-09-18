// Package planner resolves which store/connection/table should answer a
// query against a named Projection, by walking Projection -> Sync ->
// Connection through internal/store. This slice always resolves to the
// single Postgres target of a Kafka->Postgres sync.
package planner

import (
	"context"
	"errors"
	"fmt"

	"github.com/andersonlaurentino/dal-controlplane/internal/store"
	v1alpha1 "github.com/andersonlaurentino/dal-core/api/v1alpha1"
)

// Plan is what the broker needs to execute a read against the resolved
// store on behalf of a Projection query.
type Plan struct {
	StoreType        string
	ConnectionRef    string
	Target           string
	DSN              string
	StalenessSeconds int64
}

func Resolve(ctx context.Context, st *store.Store, projectionName string) (*Plan, error) {
	praw, err := st.Get(ctx, store.KindProjection, projectionName)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("projection %q does not exist", projectionName)
	}
	if err != nil {
		return nil, err
	}
	p, err := v1alpha1.DecodeProjection(praw)
	if err != nil {
		return nil, err
	}
	if len(p.Spec.Sources) == 0 {
		return nil, fmt.Errorf("projection %q has no sources", projectionName)
	}
	syncRef := p.Spec.Sources[0].SyncRef

	sraw, err := st.Get(ctx, store.KindSync, syncRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("sync %q does not exist", syncRef)
	}
	if err != nil {
		return nil, err
	}
	s, err := v1alpha1.DecodeSync(sraw)
	if err != nil {
		return nil, err
	}

	craw, err := st.Get(ctx, store.KindConnection, s.Spec.Target.ConnectionRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("connection %q does not exist", s.Spec.Target.ConnectionRef)
	}
	if err != nil {
		return nil, err
	}
	c, err := v1alpha1.DecodeConnection(craw)
	if err != nil {
		return nil, err
	}

	if c.Spec.Type != v1alpha1.ConnectionTypePostgres || c.Spec.Postgres == nil {
		return nil, fmt.Errorf("connection %q has type %q, which is not queryable", s.Spec.Target.ConnectionRef, c.Spec.Type)
	}

	target := p.Spec.Queryable.Table
	if target == "" {
		target = s.Spec.Target.Table
	}

	return &Plan{
		StoreType:     string(c.Spec.Type),
		ConnectionRef: s.Spec.Target.ConnectionRef,
		Target:        target,
		DSN:           c.Spec.Postgres.DSN,
	}, nil
}
