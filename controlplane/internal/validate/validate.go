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

func (v *Validator) getSync(ctx context.Context, name string) (*v1alpha1.Sync, error) {
	raw, err := v.store.Get(ctx, store.KindSync, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("sync %q does not exist", name)
	}
	if err != nil {
		return nil, err
	}
	return v1alpha1.DecodeSync(raw)
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

// ValidateProjection checks that every referenced Sync exists, and — for a
// tiered projection — that the recent/historical pair is actually shaped
// the way tiering requires (postgres-with-retention feeding the recent
// side, datalake feeding the historical side).
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
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	if t := p.Spec.Tiering; t != nil {
		recent, err := v.getSync(ctx, t.RecentSyncRef)
		if err != nil {
			errs = append(errs, fmt.Errorf("spec.tiering.recentSyncRef: %w", err))
		}
		historical, histErr := v.getSync(ctx, t.HistoricalSyncRef)
		if histErr != nil {
			errs = append(errs, fmt.Errorf("spec.tiering.historicalSyncRef: %w", histErr))
		}

		if err == nil && histErr == nil {
			if recent.Spec.Retention == nil {
				errs = append(errs, fmt.Errorf("spec.tiering.recentSyncRef %q must declare spec.retention", t.RecentSyncRef))
			}

			recentTgt, rErr := v.getConnection(ctx, recent.Spec.Target.ConnectionRef)
			if rErr != nil {
				errs = append(errs, fmt.Errorf("spec.tiering.recentSyncRef: %w", rErr))
			} else if recentTgt.Spec.Type != v1alpha1.ConnectionTypePostgres {
				errs = append(errs, fmt.Errorf("spec.tiering.recentSyncRef %q must target a postgres connection, got %q", t.RecentSyncRef, recentTgt.Spec.Type))
			}

			histTgt, hErr := v.getConnection(ctx, historical.Spec.Target.ConnectionRef)
			if hErr != nil {
				errs = append(errs, fmt.Errorf("spec.tiering.historicalSyncRef: %w", hErr))
			} else if histTgt.Spec.Type != v1alpha1.ConnectionTypeDatalake {
				errs = append(errs, fmt.Errorf("spec.tiering.historicalSyncRef %q must target a datalake connection, got %q", t.HistoricalSyncRef, histTgt.Spec.Type))
			}

			if recent.Spec.Retention != nil {
				var haveCutoverCol bool
				for _, f := range historical.Spec.Mapping.Schema {
					if f.Column == recent.Spec.Retention.TimestampColumn {
						haveCutoverCol = true
						break
					}
				}
				if !haveCutoverCol {
					errs = append(errs, fmt.Errorf(
						"spec.tiering.historicalSyncRef %q has no mapping column matching recentSyncRef %q's retention.timestampColumn %q",
						t.HistoricalSyncRef, t.RecentSyncRef, recent.Spec.Retention.TimestampColumn,
					))
				}
			}
		}
	}

	return errors.Join(errs...)
}
