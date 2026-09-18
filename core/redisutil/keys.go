// Package redisutil centralizes the Redis key schema used by the control
// plane to store desired and observed resource state, so every reader/writer
// (control plane, worker heartbeats) agrees on the exact key shape.
package redisutil

import "fmt"

// DesiredKey is where a resource's desired-state JSON is stored, keyed by
// kind (e.g. "connection", "sync", "projection") and resource name.
func DesiredKey(kind, name string) string {
	return fmt.Sprintf("dal:%s:desired:%s", kind, name)
}

// DesiredIndexKey is the set of all resource names that exist for a kind.
func DesiredIndexKey(kind string) string {
	return fmt.Sprintf("dal:%s:desired:index", kind)
}

// SyncObservedKey is the hash holding a Sync's observed status: phase,
// lastHeartbeatAt, lastError, consumerLag, watermark.
func SyncObservedKey(syncName string) string {
	return fmt.Sprintf("dal:sync:observed:%s", syncName)
}

// SyncHeartbeatKey is a short-TTL key refreshed by the worker; its absence
// signals a dead worker without the control plane needing to poll.
func SyncHeartbeatKey(syncName string) string {
	return fmt.Sprintf("dal:sync:heartbeat:%s", syncName)
}

// SyncLockKey guards which control-plane replica owns spawning/supervising
// a given Sync. Reserved for future multi-replica support; unused while a
// single control-plane replica runs.
func SyncLockKey(syncName string) string {
	return fmt.Sprintf("dal:lock:sync:%s", syncName)
}
