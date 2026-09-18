// Package kafkapg implements the sync-types this slice supports: a
// delta-replicated Kafka topic upserted into a Postgres table per a Sync's
// field mapping, either consumed continuously (Mode: streaming) or drained
// once per cron tick (Mode: scheduled).
package kafkapg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/robfig/cron/v3"
	"github.com/segmentio/kafka-go"

	v1alpha1 "github.com/andersonlaurentino/dal-core/api/v1alpha1"
	"github.com/andersonlaurentino/dal-worker/internal/heartbeat"
)

const heartbeatInterval = 10 * time.Second

// Config bundles everything Run needs: the Sync's own spec plus the fully
// resolved source and target Connections it references.
type Config struct {
	SyncName string
	Sync     v1alpha1.SyncSpec
	Source   v1alpha1.ConnectionSpec
	Target   v1alpha1.ConnectionSpec

	Heartbeat *heartbeat.Client
	Log       *slog.Logger
}

// runner holds everything shared between the streaming and scheduled
// execution strategies: the open DB/Kafka handles, the precomputed upsert
// statement, and the last-processed watermark reported via heartbeats.
type runner struct {
	cfg       Config
	db        *sql.DB
	reader    *kafka.Reader
	upsertSQL string
	log       *slog.Logger

	watermark string
}

// Run blocks, consuming Config.Sync.Source.Topic and upserting into
// Config.Sync.Target.Table until ctx is cancelled or an unrecoverable error
// occurs. Only a kafka source, a postgres target, and Replication=delta are
// supported; Mode selects streaming (continuous consume) or scheduled
// (cron-driven drain cycles).
func Run(ctx context.Context, cfg Config) error {
	if cfg.Source.Type != v1alpha1.ConnectionTypeKafka || cfg.Source.Kafka == nil {
		return fmt.Errorf("source connection has type %q, want kafka", cfg.Source.Type)
	}
	if cfg.Target.Type != v1alpha1.ConnectionTypePostgres || cfg.Target.Postgres == nil {
		return fmt.Errorf("target connection has type %q, want postgres", cfg.Target.Type)
	}
	if cfg.Sync.Replication != v1alpha1.SyncReplicationDelta {
		return fmt.Errorf("replication %q is not supported (only %q)", cfg.Sync.Replication, v1alpha1.SyncReplicationDelta)
	}

	var schedule cron.Schedule
	if cfg.Sync.Mode == v1alpha1.SyncModeScheduled {
		var err error
		schedule, err = cron.ParseStandard(cfg.Sync.Schedule)
		if err != nil {
			return fmt.Errorf("parsing schedule %q: %w", cfg.Sync.Schedule, err)
		}
	} else if cfg.Sync.Mode != v1alpha1.SyncModeStreaming {
		return fmt.Errorf("sync mode %q is not supported", cfg.Sync.Mode)
	}

	db, err := sql.Open("pgx", cfg.Target.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("connecting to target postgres: %w", err)
	}
	defer db.Close()

	groupID := cfg.Sync.ConsumerGroup
	if groupID == "" {
		groupID = "dal-worker-" + cfg.SyncName
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Source.Kafka.Brokers,
		GroupID: groupID,
		Topic:   cfg.Sync.Source.Topic,
	})
	defer reader.Close()

	upsertSQL, err := buildUpsertSQL(cfg.Sync.Target.Table, cfg.Sync.Mapping)
	if err != nil {
		return err
	}

	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}

	r := &runner{cfg: cfg, db: db, reader: reader, upsertSQL: upsertSQL, log: log}

	hbTicker := time.NewTicker(heartbeatInterval)
	defer hbTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hbTicker.C:
				lag := reader.Stats().Lag
				if err := cfg.Heartbeat.Heartbeat(ctx, cfg.SyncName, "running", lag, r.watermark, time.Now().Unix()); err != nil {
					log.Error("heartbeat failed", "sync", cfg.SyncName, "err", err)
				}
			}
		}
	}()

	if schedule != nil {
		return r.runScheduled(ctx, schedule)
	}
	return r.runStreaming(ctx)
}

// runStreaming consumes messages continuously, upserting and committing
// each one as it arrives.
func (r *runner) runStreaming(ctx context.Context) error {
	for {
		msg, err := r.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetching message: %w", err)
		}

		if err := r.upsertOne(ctx, msg); err != nil {
			return err
		}
	}
}

// runScheduled fires one drain cycle per cron tick: it fetches messages
// until a fetch goes quiet (no new message within drainIdleTimeout), then
// goes idle until the next tick. Messages published mid-cycle after the
// drain has already gone quiet are picked up on the next tick, not this
// one. Reader.ReadLag isn't usable here — kafka-go returns an error for it
// once a GroupID is set, since a group-managed reader doesn't own a fixed
// partition set to compute lag against locally.
func (r *runner) runScheduled(ctx context.Context, schedule cron.Schedule) error {
	c := cron.New()
	errCh := make(chan error, 1)
	c.Schedule(schedule, cron.FuncJob(func() {
		if err := r.drainCycle(ctx); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}))
	c.Start()
	defer c.Stop()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

const drainIdleTimeout = 3 * time.Second

func (r *runner) drainCycle(ctx context.Context) error {
	for {
		fetchCtx, cancel := context.WithTimeout(ctx, drainIdleTimeout)
		msg, err := r.reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("fetching message: %w", err)
		}
		if err := r.upsertOne(ctx, msg); err != nil {
			return err
		}
	}
}

// upsertOne extracts, upserts, and commits a single Kafka message. A
// malformed message is logged and committed anyway so it doesn't block the
// partition forever; a DB error is treated as unrecoverable for this run.
func (r *runner) upsertOne(ctx context.Context, msg kafka.Message) error {
	values, key, err := extractValues(msg.Value, r.cfg.Sync.Mapping)
	if err != nil {
		r.log.Error("skipping malformed message", "sync", r.cfg.SyncName, "offset", msg.Offset, "err", err)
		if err := r.reader.CommitMessages(ctx, msg); err != nil {
			r.log.Error("commit failed after skip", "sync", r.cfg.SyncName, "err", err)
		}
		return nil
	}

	if _, err := r.db.ExecContext(ctx, r.upsertSQL, values...); err != nil {
		return fmt.Errorf("upserting row (offset %d): %w", msg.Offset, err)
	}

	if err := r.reader.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("committing offset: %w", err)
	}
	r.watermark = key
	return nil
}

// buildUpsertSQL renders an INSERT ... ON CONFLICT (keyField) DO UPDATE
// statement once per Run, since the column set is fixed for the life of the
// worker.
func buildUpsertSQL(table string, mapping v1alpha1.SyncMapping) (string, error) {
	ident := pgx.Identifier{table}.Sanitize()

	cols := make([]string, len(mapping.Schema))
	placeholders := make([]string, len(mapping.Schema))
	var updates []string
	for i, f := range mapping.Schema {
		col := pgx.Identifier{f.Column}.Sanitize()
		cols[i] = col
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		if f.Column != mapping.KeyField {
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}

	keyIdent := pgx.Identifier{mapping.KeyField}.Sanitize()
	if len(updates) == 0 {
		return fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
			ident, strings.Join(cols, ", "), strings.Join(placeholders, ", "), keyIdent,
		), nil
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		ident, strings.Join(cols, ", "), strings.Join(placeholders, ", "), keyIdent, strings.Join(updates, ", "),
	), nil
}

// extractValues walks the decoded message per mapping.Schema[].JSONPath
// (flat/dotted paths only, e.g. "$.event_id" or "$.a.b" — no array
// indexing) and coerces each value per its declared type. It also returns
// the key field's value for use as the heartbeat watermark.
func extractValues(raw []byte, mapping v1alpha1.SyncMapping) ([]any, string, error) {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, "", fmt.Errorf("decoding message JSON: %w", err)
	}

	values := make([]any, len(mapping.Schema))
	var key string
	for i, f := range mapping.Schema {
		v, ok := lookupPath(doc, f.JSONPath)
		if !ok {
			return nil, "", fmt.Errorf("jsonPath %q not found", f.JSONPath)
		}
		coerced, err := coerce(v, f.Type)
		if err != nil {
			return nil, "", fmt.Errorf("column %q: %w", f.Column, err)
		}
		values[i] = coerced
		if f.Column == mapping.KeyField {
			key = fmt.Sprintf("%v", coerced)
		}
	}
	return values, key, nil
}

func lookupPath(doc map[string]any, path string) (any, bool) {
	path = strings.TrimPrefix(path, "$.")
	cur := any(doc)
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func coerce(v any, typ string) (any, error) {
	switch typ {
	case "text", "timestamptz", "timestamp", "json":
		return fmt.Sprintf("%v", v), nil
	case "int", "bigint":
		switch n := v.(type) {
		case float64:
			return int64(n), nil
		case string:
			i, err := strconv.ParseInt(n, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot coerce %q to %s: %w", n, typ, err)
			}
			return i, nil
		default:
			return nil, fmt.Errorf("cannot coerce %T to %s", v, typ)
		}
	case "float", "double":
		switch n := v.(type) {
		case float64:
			return n, nil
		default:
			return nil, fmt.Errorf("cannot coerce %T to %s", v, typ)
		}
	case "boolean", "bool":
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("cannot coerce %T to %s", v, typ)
		}
		return b, nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}
