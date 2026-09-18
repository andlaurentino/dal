// Package store persists Connection/Sync/Projection desired-state documents
// in Redis using the key schema defined in core/redisutil. The control
// plane itself is stateless — all desired state lives here.
package store

import (
	"context"
	"errors"

	"github.com/andersonlaurentino/dal-core/redisutil"
	"github.com/redis/go-redis/v9"
)

// ErrNotFound is returned by Get when no document exists for kind+name.
var ErrNotFound = errors.New("resource not found")

type Store struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

// Put stores raw (the raw YAML bytes as submitted, not re-serialized) under
// kind+name and adds name to that kind's index set. Both writes happen in a
// single pipeline so a caller never observes one without the other.
func (s *Store) Put(ctx context.Context, kind, name string, raw []byte) error {
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, redisutil.DesiredKey(kind, name), raw, 0)
		pipe.SAdd(ctx, redisutil.DesiredIndexKey(kind), name)
		return nil
	})
	return err
}

// Get returns the raw stored bytes for kind+name, or ErrNotFound.
func (s *Store) Get(ctx context.Context, kind, name string) ([]byte, error) {
	raw, err := s.rdb.Get(ctx, redisutil.DesiredKey(kind, name)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// List returns all resource names stored for kind.
func (s *Store) List(ctx context.Context, kind string) ([]string, error) {
	return s.rdb.SMembers(ctx, redisutil.DesiredIndexKey(kind)).Result()
}

// Delete removes kind+name. It does not check whether other resources
// still reference it — dangling references are allowed by design.
func (s *Store) Delete(ctx context.Context, kind, name string) error {
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, redisutil.DesiredKey(kind, name))
		pipe.SRem(ctx, redisutil.DesiredIndexKey(kind), name)
		return nil
	})
	return err
}

// Exists reports whether kind+name is present in the index set — a cheap
// existence check for referential validation, without fetching the body.
func (s *Store) Exists(ctx context.Context, kind, name string) (bool, error) {
	return s.rdb.SIsMember(ctx, redisutil.DesiredIndexKey(kind), name).Result()
}
