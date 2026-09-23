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
	"slices"

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

// ValidateProjection checks that every referenced Sync exists, that every
// spec.sources[].view.columns[].source (and spec.sources[].routing's
// timestampColumn) actually names a column that Sync's own mapping
// produces, and — for a multi-source projection — that every source's
// resolved (post-rename, passthrough-or-not) column set matches exactly,
// so a stitched read returns one consistent shape regardless of which
// source(s) answered a given page.
func (v *Validator) ValidateProjection(ctx context.Context, p *v1alpha1.Projection) error {
	var errs []error
	syncs := make([]*v1alpha1.Sync, len(p.Spec.Sources))
	for i, src := range p.Spec.Sources {
		s, err := v.getSync(ctx, src.SyncRef)
		if err != nil {
			errs = append(errs, fmt.Errorf("spec.sources[%d].syncRef: %w", i, err))
			continue
		}
		syncs[i] = s

		mapped := make(map[string]bool, len(s.Spec.Mapping.Schema))
		for _, f := range s.Spec.Mapping.Schema {
			mapped[f.Column] = true
		}

		for j, c := range src.View.Columns {
			if !mapped[c.Source] {
				errs = append(errs, fmt.Errorf(
					"spec.sources[%d].view.columns[%d].source: sync %q's mapping has no column %q",
					i, j, src.SyncRef, c.Source,
				))
			}
		}

		if r := src.Routing; r != nil && !mapped[r.TimestampColumn] {
			errs = append(errs, fmt.Errorf(
				"spec.sources[%d].routing.timestampColumn: sync %q's mapping has no column %q",
				i, src.SyncRef, r.TimestampColumn,
			))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	if len(p.Spec.Sources) < 2 {
		return nil
	}

	var want []string
	for i, src := range p.Spec.Sources {
		got := resolvedColumns(src, syncs[i])
		if want == nil {
			want = got
			continue
		}
		if !slices.Equal(want, got) {
			errs = append(errs, fmt.Errorf(
				"spec.sources[%d]: resolved column set %v does not match an earlier source's %v — every source of a multi-source projection must expose the same columns, in the same order (passthrough sources use their sync's mapping order)",
				i, got, want,
			))
		}
	}

	return errors.Join(errs...)
}

// resolvedColumns returns a source's final advertised column names, in
// order: its own view.columns (renamed via `as`) if set, or — for
// passthrough — every column the referenced Sync's mapping produces, in
// mapping order.
func resolvedColumns(src v1alpha1.ProjectionSource, s *v1alpha1.Sync) []string {
	if len(src.View.Columns) > 0 {
		names := make([]string, len(src.View.Columns))
		for i, c := range src.View.Columns {
			if c.As != "" {
				names[i] = c.As
			} else {
				names[i] = c.Source
			}
		}
		return names
	}
	names := make([]string, len(s.Spec.Mapping.Schema))
	for i, f := range s.Spec.Mapping.Schema {
		names[i] = f.Column
	}
	return names
}
