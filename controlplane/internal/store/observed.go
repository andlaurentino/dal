package store

import (
	"context"
	"strconv"
	"time"

	"github.com/andersonlaurentino/dal-core/redisutil"
	"github.com/redis/go-redis/v9"
)

// heartbeatTTL bounds how long a worker's SyncHeartbeatKey survives without
// a refresh before it's considered dead. workerd heartbeats every 10s
// (HEARTBEAT_INTERVAL in each of its pipeline modules); 3x that gives
// margin for one or two missed beats before flagging it as not alive.
const heartbeatTTL = 30 * time.Second

// ObservedSync mirrors SyncObservedKey's hash: everything workerd has
// reported about one Sync's runtime state, independent of whether its
// heartbeat is currently alive (see GetSyncObserved).
type ObservedSync struct {
	Phase           string
	LastHeartbeatAt time.Time
	LastError       string
	ConsumerLag     int64
	Watermark       string
}

// PutSyncHeartbeat records a Heartbeat RPC: updates the observed-state hash
// and refreshes the short-TTL liveness key in one round trip.
func (s *Store) PutSyncHeartbeat(ctx context.Context, syncName, phase string, consumerLag int64, watermark string, at time.Time) error {
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, redisutil.SyncObservedKey(syncName), map[string]any{
			"phase":           phase,
			"lastHeartbeatAt": at.Format(time.RFC3339),
			"consumerLag":     consumerLag,
			"watermark":       watermark,
		})
		pipe.Set(ctx, redisutil.SyncHeartbeatKey(syncName), "1", heartbeatTTL)
		return nil
	})
	return err
}

// PutSyncError records a ReportError RPC. A fatal error also sets phase to
// "error" so it's visible without waiting for the next heartbeat (which,
// for a fatal error, likely never comes).
func (s *Store) PutSyncError(ctx context.Context, syncName, message string, fatal bool) error {
	fields := map[string]any{"lastError": message}
	if fatal {
		fields["phase"] = "error"
	}
	return s.rdb.HSet(ctx, redisutil.SyncObservedKey(syncName), fields).Err()
}

// GetSyncObserved returns a Sync's observed state (nil if nothing's ever
// been reported for it) and whether its heartbeat is currently alive
// (heartbeated within heartbeatTTL) — the two are read separately since a
// worker that died still leaves its last-known observed state behind, and
// callers need to distinguish "stale info from a dead worker" from "worker
// currently healthy".
func (s *Store) GetSyncObserved(ctx context.Context, syncName string) (*ObservedSync, bool, error) {
	vals, err := s.rdb.HGetAll(ctx, redisutil.SyncObservedKey(syncName)).Result()
	if err != nil {
		return nil, false, err
	}
	aliveCount, err := s.rdb.Exists(ctx, redisutil.SyncHeartbeatKey(syncName)).Result()
	if err != nil {
		return nil, false, err
	}
	alive := aliveCount > 0

	if len(vals) == 0 {
		return nil, alive, nil
	}
	o := &ObservedSync{
		Phase:     vals["phase"],
		LastError: vals["lastError"],
		Watermark: vals["watermark"],
	}
	if t, err := time.Parse(time.RFC3339, vals["lastHeartbeatAt"]); err == nil {
		o.LastHeartbeatAt = t
	}
	if lag, err := strconv.ParseInt(vals["consumerLag"], 10, 64); err == nil {
		o.ConsumerLag = lag
	}
	return o, alive, nil
}
