-- Target table for the Kafka -> Postgres thin-slice Sync (examples/kafka-to-postgres).
CREATE TABLE IF NOT EXISTS user_events (
  event_id    TEXT PRIMARY KEY,
  user_id     TEXT NOT NULL,
  event_type  TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  synced_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_events_occurred_at ON user_events (occurred_at DESC);
