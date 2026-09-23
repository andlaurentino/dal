# tiered-events

Worked example of DAL's time-range tiering: one logical dataset (`user_events`
from the `user-events` Kafka topic), replicated two ways, served through a
single Projection that hides the seam.

## Resources

- `connection-kafka.yaml`, `connection-postgres.yaml`, `connection-datalake.yaml` —
  the three backing stores. `datalake` points at any S3-compatible object
  store; this repo's local dev stack runs [RustFS](https://github.com/rustfs/rustfs)
  as the reference backend (see `infra/k8s/rustfs.yaml`), but MinIO or real
  AWS S3 work identically — DAL's `datalake` Connection type only knows about
  `endpoint`/`bucket`, never the backend implementation.
- `sync-postgres.yaml` (`events-to-postgres`) — kafka → postgres, pruned to
  the last 7 days (`spec.retention`). Fast, indexed, recent-activity queries.
- `sync-lake.yaml` (`events-to-lake`) — kafka → lake, no retention: a Delta
  table (with Change Data Feed enabled) holding the full history of every
  event ever produced.
- `projection-postgres.yaml` / `projection-lake.yaml` — single-tier views
  over each Sync individually, useful for testing each tier in isolation.
  Each source names its own `view.table` — there's no Projection-wide table
  override.
- `projection-combined.yaml` (`events-combined`) — the tiered Projection:
  two sources, each with its own `routing` (`type: timeRange`,
  `timestampColumn: occurred_at`, and a `minAge`/`maxAge` window) that
  together partition time with no gap or overlap — `events-to-postgres`
  answers rows younger than 168h (7 days), `events-to-lake` answers
  everything 168h or older. This routing lives entirely on the Projection;
  it doesn't require `events-to-postgres` to declare `spec.retention` at
  all (they happen to agree here, but a source populated by some other
  means with no retention concept still works). `broker` resolves this
  against `controlplane`'s query planner and stitches pages that straddle
  the boundary, so pagination looks seamless regardless of where in time a
  page falls — generalized to any number of sources, not just two.

## Try it

```
make apply-tiered-seed   # applies these resources, then seeds backdated + recent events
```

Then query `events-recent`, `events-history`, or `events-combined` through
`broker`'s query API (`broker-external`'s NodePort) and compare: postgres
only has the last 7 days, the lake has everything, and the combined view
transparently spans both.
