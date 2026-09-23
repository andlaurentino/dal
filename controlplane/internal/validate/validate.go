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

// validPairs enumerates the only (source type, target type) combinations
// workerd knows how to run. Keep in sync with workerd's own dispatch
// (workerd/src/main.rs) — a pair accepted here but not implemented there
// will pass validation and then fail at runtime via ReportError.
var validPairs = map[[2]v1alpha1.ConnectionType]bool{
	{v1alpha1.ConnectionTypeKafka, v1alpha1.ConnectionTypePostgres}:    true, // kafka -> postgres
	{v1alpha1.ConnectionTypeKafka, v1alpha1.ConnectionTypeDatalake}:    true, // kafka -> lake
	{v1alpha1.ConnectionTypeDatalake, v1alpha1.ConnectionTypePostgres}: true, // lake -> postgres
}

// ValidateSync checks that both the source and target connectionRefs exist,
// that their (source type, target type) pair is one workerd implements, and
// that each side sets the field appropriate for its resolved connection type
// (topic/path on the source, table/path on the target) — core's schema
// validation can only check that exactly one of each pair is set, not which
// one is correct, since it has no access to the referenced Connections.
func (v *Validator) ValidateSync(ctx context.Context, s *v1alpha1.Sync) error {
	var errs []error

	src, srcErr := v.getConnection(ctx, s.Spec.Source.ConnectionRef)
	if srcErr != nil {
		errs = append(errs, fmt.Errorf("spec.source.connectionRef: %w", srcErr))
	}

	tgt, tgtErr := v.getConnection(ctx, s.Spec.Target.ConnectionRef)
	if tgtErr != nil {
		errs = append(errs, fmt.Errorf("spec.target.connectionRef: %w", tgtErr))
	}

	if srcErr == nil && tgtErr == nil {
		if !validPairs[[2]v1alpha1.ConnectionType{src.Spec.Type, tgt.Spec.Type}] {
			errs = append(errs, fmt.Errorf(
				"spec: connection pair (source=%q, target=%q) is not a supported sync — valid pairs are kafka->postgres, kafka->lake (datalake), lake->postgres",
				src.Spec.Type, tgt.Spec.Type,
			))
		}

		if err := validateRoleFields("source", src.Spec.Type, s.Spec.Source.Topic, s.Spec.Source.Path); err != nil {
			errs = append(errs, err)
		}
		if err := validateRoleFields("target", tgt.Spec.Type, s.Spec.Target.Table, s.Spec.Target.Path); err != nil {
			errs = append(errs, err)
		}

		if src.Spec.Type == v1alpha1.ConnectionTypeDatalake && tgt.Spec.Type == v1alpha1.ConnectionTypePostgres &&
			s.Spec.Replication == v1alpha1.SyncReplicationSnapshot {
			errs = append(errs, fmt.Errorf("spec.replication: %q is not yet supported for lake->postgres syncs", v1alpha1.SyncReplicationSnapshot))
		}

		if s.Spec.Retention != nil && tgt.Spec.Type != v1alpha1.ConnectionTypePostgres {
			errs = append(errs, fmt.Errorf("spec.retention: only supported when spec.target is a postgres connection, got %q", tgt.Spec.Type))
		}
	}

	return errors.Join(errs...)
}

// validateRoleFields checks that the topic/table-shaped field is set for
// kafka/postgres connections and the path-shaped field is set for datalake
// connections, on whichever side (source or target) is being checked.
func validateRoleFields(side string, connType v1alpha1.ConnectionType, topicOrTable, path string) error {
	switch connType {
	case v1alpha1.ConnectionTypeKafka, v1alpha1.ConnectionTypePostgres:
		if topicOrTable == "" || path != "" {
			field := "topic"
			if connType == v1alpha1.ConnectionTypePostgres {
				field = "table"
			}
			return fmt.Errorf("spec.%s: connection type %q requires %s to be set and path to be empty", side, connType, field)
		}
	case v1alpha1.ConnectionTypeDatalake:
		if path == "" || topicOrTable != "" {
			return fmt.Errorf("spec.%s: connection type %q requires path to be set and topic/table to be empty", side, connType)
		}
	}
	return nil
}

// ValidateProjection checks that every referenced Connection exists and is
// queryable (postgres or datalake, not kafka) — core/api/v1alpha1's schema
// validation already checked that spec.sources[].view.columns is non-empty
// and, for multi-source projections, that every source's resolved column
// set matches exactly; neither needs store access, so both live there
// instead of here.
func (v *Validator) ValidateProjection(ctx context.Context, p *v1alpha1.Projection) error {
	var errs []error
	for i, src := range p.Spec.Sources {
		c, err := v.getConnection(ctx, src.ConnectionRef)
		if err != nil {
			errs = append(errs, fmt.Errorf("spec.sources[%d].connectionRef: %w", i, err))
			continue
		}
		switch c.Spec.Type {
		case v1alpha1.ConnectionTypePostgres, v1alpha1.ConnectionTypeDatalake:
		default:
			errs = append(errs, fmt.Errorf(
				"spec.sources[%d].connectionRef: connection %q has type %q, which is not queryable",
				i, src.ConnectionRef, c.Spec.Type,
			))
		}
	}
	return errors.Join(errs...)
}
