// Package validate performs cross-resource referential checks that
// core/api/v1alpha1's schema validation can't do on its own (it has no
// access to the resource store). These checks run only on apply
// (create/update) — deletes never check for incoming references, so
// dangling refs are allowed by design.
package validate

import (
	"context"
	"errors"
	"fmt"

	"github.com/andersonlaurentino/dal-controlplane/internal/store"
	v1alpha1 "github.com/andersonlaurentino/dal-core/api/v1alpha1"
)

type Validator struct {
	store *store.Store
}

func New(s *store.Store) *Validator {
	return &Validator{store: s}
}

func (v *Validator) getConnection(ctx context.Context, name string) (*v1alpha1.Connection, error) {
	raw, err := v.store.Get(ctx, store.KindConnection, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("connection %q does not exist", name)
	}
	if err != nil {
		return nil, err
	}
	return v1alpha1.DecodeConnection(raw)
}

// ValidateSync checks that both the source and target connectionRefs exist,
// and that each is a plausible role for its side (source readable, target
// writable). The role check is deliberately coarse — there's no formal
// "role" field on ConnectionType — it just catches the obvious mistake of
// wiring a Sync to the wrong kind of Connection.
func (v *Validator) ValidateSync(ctx context.Context, s *v1alpha1.Sync) error {
	var errs []error

	src, err := v.getConnection(ctx, s.Spec.Source.ConnectionRef)
	if err != nil {
		errs = append(errs, fmt.Errorf("spec.source.connectionRef: %w", err))
	} else if !isReadableSource(src.Spec.Type) {
		errs = append(errs, fmt.Errorf("spec.source.connectionRef: connection %q has type %q, which is not a valid sync source", s.Spec.Source.ConnectionRef, src.Spec.Type))
	}

	tgt, err := v.getConnection(ctx, s.Spec.Target.ConnectionRef)
	if err != nil {
		errs = append(errs, fmt.Errorf("spec.target.connectionRef: %w", err))
	} else if !isWritableTarget(tgt.Spec.Type) {
		errs = append(errs, fmt.Errorf("spec.target.connectionRef: connection %q has type %q, which is not a valid sync target", s.Spec.Target.ConnectionRef, tgt.Spec.Type))
	}

	return errors.Join(errs...)
}

// ValidateProjection checks that every referenced Sync exists.
func (v *Validator) ValidateProjection(ctx context.Context, p *v1alpha1.Projection) error {
	var errs []error
	for i, src := range p.Spec.Sources {
		ok, err := v.store.Exists(ctx, store.KindSync, src.SyncRef)
		if err != nil {
			errs = append(errs, fmt.Errorf("spec.sources[%d].syncRef: %w", i, err))
			continue
		}
		if !ok {
			errs = append(errs, fmt.Errorf("spec.sources[%d].syncRef: sync %q does not exist", i, src.SyncRef))
		}
	}
	return errors.Join(errs...)
}

func isReadableSource(t v1alpha1.ConnectionType) bool {
	return t == v1alpha1.ConnectionTypeKafka || t == v1alpha1.ConnectionTypeDatalake
}

func isWritableTarget(t v1alpha1.ConnectionType) bool {
	return t == v1alpha1.ConnectionTypePostgres || t == v1alpha1.ConnectionTypeDatalake
}
