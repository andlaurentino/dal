# DAL — Architecture & Conventions

DAL (Data Abstraction Layer) is a set of microservices (Go for `controlplane`/`broker`/`web`, Rust for `workerd`), orchestrated on Kubernetes, that move data from source systems into query-optimized stores — including an intermediate Delta Lake tier on MinIO — and serve reads back through a stable, storage-agnostic query interface. This file documents the architecture and the conventions new work in this repo must follow.

## Overview

Users declare `Connection`, `Sync`, and `Projection` resources (YAML, CRD-style) against `controlplane`. `controlplane` validates and stores them, then reconciles a Kubernetes `ReplicaSet` per `Sync` so a dedicated `workerd` Pod moves that Sync's data continuously or on a schedule. Three Sync kinds exist, distinguished by the (source, target) Connection types: kafka→postgres, kafka→lake (Delta table on MinIO, Change Data Feed enabled), and lake→postgres (consumes that CDF). `broker` answers queries against named `Projection`s without callers ever knowing which physical store or table backs them. `web` is the UI over both.

## Components

### `controlplane` (`controlplaned`)
- Source of truth for `Connection`/`Sync`/`Projection` resources — stored in Redis (`internal/store`).
- Validates every resource on apply (`core/api/v1alpha1/validation.go`) before persisting; cross-resource checks — including which (source type, target type) pairs are supported — live in `internal/validate`.
- Exposes REST (`internal/httpapi`) for `web`/CLI apply and delete.
- Exposes gRPC `WorkerManager` and `QueryPlanner` servers (`internal/grpcserver`) for `workerd` and `broker`.
- Owns Kubernetes orchestration of workers (`internal/workermgr`) — reconciles one `ReplicaSet` per `Sync`, all running the same `workerd` image regardless of Sync kind.

### `workerd` (Rust)
- One process per `Sync`, run as a Kubernetes `ReplicaSet` Pod created by `controlplane/internal/workermgr`.
- A single binary implementing all three Sync kinds; on startup it resolves its Sync's source/target Connections and dispatches to the matching pipeline module (`src/kafkapostgres.rs`, `src/kafkalake.rs`, `src/lakepostgres.rs`) based on their `(type, type)` pair. `streaming` (continuous consume) vs `scheduled` (in-process cron-driven drain cycles) is decided the same way for kafka-sourced Syncs.
- Written in Rust specifically for Delta Lake support (the `deltalake` crate) — the Go ecosystem has no comparably mature Delta Lake library. This module replaced an earlier Go implementation entirely rather than running two worker binaries side by side; keep it that way — don't reintroduce a Go worker.
- Registers on boot, heartbeats, and reports fatal errors to `controlplane` over gRPC (`src/heartbeat.rs`, generated via `tonic-build` from the same `.proto` `worker`/`controlplane` already share).

### `broker` (`brokerd`)
- Serves queries against named `Projection`s.
- Asks `controlplane`'s `QueryPlanner.ResolvePlan` which physical store(s)/connection(s)/table(s) currently back a Projection.
- Executes the resolved query via a pluggable executor (`internal/executors`): `postgres` (`jackc/pgx`) and `lake` (DuckDB via `go-duckdb`, scanning Delta tables on MinIO — CGO, hence broker's non-default `CGO_ENABLED=1` build). A plain Projection resolves to one executor; a *tiered* Projection (`spec.tiering`, see below) uses both, with `broker` stitching pages that straddle the retention cutover so pagination looks seamless.

### `web`
- Next.js/TypeScript UI for authoring Connections/Syncs/Projections (REST → `controlplane`) and running queries (REST → `broker`).

### `core`
- Shared Go library only — **no service-specific logic** belongs here.
- `api/v1alpha1`: resource types + validation.
- `proto`: gRPC contracts (`.proto`) and generated stubs.
- `logging`, `redisutil`: shared infrastructure helpers.

### `tools/producer`
- Standalone synthetic Kafka event generator for local dev. Deliberately kept outside `go.work` — run it with `GOWORK=off go run ./cmd/producer` or `make run-producer`.

## CRD-Style Declarative Resources

`core/api/v1alpha1` defines `Connection`, `Sync`, and `Projection`, each carrying:

```go
TypeMeta{APIVersion: "dal.io/v1alpha1", Kind: "Sync"}
Spec <typed spec>
```

They're applied as YAML via `POST /apply` to `controlplane`, validated (`ValidateConnection`/`ValidateSync`/`ValidateProjection`), then persisted to Redis. This deliberately mirrors Kubernetes' own Resource/apply/validate model — new resource kinds should follow the same shape: `TypeMeta` + typed `Spec` + a `Validate*` function called before any write.

## Architecture Patterns

### Hexagonal (Ports & Adapters)
`broker/internal/executors` is a port with a `postgres` adapter implementation. Adding a new target store type for queries means adding a new adapter behind that port — never branching on store type inside broker's core logic. Apply the same discipline in `workerd`: each (source type, target type) pair is its own pipeline module (`kafkapostgres.rs`, `kafkalake.rs`, `lakepostgres.rs`), selected once in `main.rs`'s dispatch — new Sync pairs get a new module, not a branch buried inside an existing one.

### Data Abstraction Layer
The reason `Sync` + `Projection` + `broker` exist as separate concepts: callers query a named `Projection` and never know or care which physical store/connection/table currently answers it. `QueryPlanner.ResolvePlan` (`core/proto/controlplane/v1/query_plan.proto`) is the resolution seam; it returns a `primary` `StorePlan` always, and — for a tiered Projection — a `historical` `StorePlan` plus the retention window, letting `broker` route (and stitch) pages across two physical stores without the caller ever knowing.

**Time-range tiering**: a `Sync` can declare `spec.retention` (`days` + `timestampColumn`), enforced by `workerd` pruning the postgres target on an interval. A `Projection` can then declare `spec.tiering` (`recentSyncRef` + `historicalSyncRef`, both also listed in `spec.sources`) to combine a retention-limited postgres Sync with a full-history datalake Sync into one queryable view — the cutover point is read from the recent Sync's own `spec.retention`, never duplicated on the Projection. See `examples/tiered-events/` for a full worked example (`events-to-postgres` 7-day-retained, `events-to-lake` full history, `events-combined` tiered).

### Kubernetes-Native Per-Sync Orchestration
`controlplane/internal/workermgr` reconciles one `ReplicaSet` (labels `app=dal-worker,sync=<name>`) per `Sync` — created on apply, deleted on Sync delete, recreated (delete-then-recreate) when the Sync's spec changes so `workerd` picks up new values at startup. Restart-on-crash is delegated entirely to Kubernetes' built-in ReplicaSet controller — never reimplement backoff/restart logic in application code. `workermgr` itself has no awareness of Sync kind — every ReplicaSet runs the same `workerd` image; kind dispatch happens inside the Rust binary at startup.

**Important distinction**: a Sync's `mode: scheduled` does **not** create a Kubernetes `Job`/`CronJob`. The Pod is always a long-running `ReplicaSet` Pod; `streaming` vs `scheduled` is decided entirely inside `workerd`'s kafka-sourced pipelines, using an in-process cron scheduler for `scheduled` mode. Kubernetes is only asked to replace the ReplicaSet when the Sync spec changes — it has no awareness of the cron schedule itself.

## Frameworks & Libraries

| Library | Used for | Why |
|---|---|---|
| `k8s.io/client-go` | Cluster orchestration from `controlplane` | In-cluster config + typed clientset is the standard way to manage native objects (`ReplicaSet`) from inside a Pod |
| `google.golang.org/grpc`/`tonic` + `protoc`-generated stubs | Service-to-service calls (`workerd`→`controlplane`, `broker`→`controlplane`) | Typed contracts, low overhead, streaming-friendly; reserved for internal service-to-service traffic. `workerd` codegens its own Rust stubs via `tonic-build` from the same `.proto` files |
| REST (`net/http`) | `controlplane` apply API, `broker` query API | The only two human/browser-facing edges — kept as plain REST for `web`/CLI ergonomics; `workerd` also uses it (not gRPC) to fetch its own Sync/Connection specs, mirroring the old Go worker's `cpclient` |
| `rdkafka` | Kafka consumer in `workerd` | De facto standard Rust Kafka client (librdkafka bindings), consumer-group support |
| `deltalake` | Delta Lake read/write + Change Data Feed in `workerd`'s lake-touching pipelines | Only mature Delta Lake implementation in any language ecosystem accessible from Rust; this is *why* `workerd` is Rust rather than Go |
| `tokio-postgres` (`workerd`) / `jackc/pgx` (`broker`) | Postgres access | Native driver in each language |
| `go-duckdb` (DuckDB) | Delta Lake reads in `broker`'s `lake` executor | No mature pure-Go Delta reader exists; DuckDB's `delta`/`httpfs` extensions read Delta tables on S3-compatible storage directly, in-process — avoids standing up a second Rust service just for reads |
| Redis | `controlplane`'s resource store | Simple, fast KV store sufficient for resource specs + heartbeat TTLs |
| MinIO | S3-compatible object storage backing `datalake` Connections | Self-hostable S3 API for local dev/on-prem; `workerd`'s Delta tables live here |
| Next.js / TypeScript | `web` | UI framework for the authoring/query console |

## Good Practices / Conventions

- **Hostname-based service discovery only.** Every service address is an env var or Kubernetes Service DNS name (`kafka:9092`, `postgres:5432`, `controlplane:9090`, `minio:9000`, etc.) — never hardcode an address. This is what made the docker-compose→Kubernetes migration a zero-application-code-change operation; keep it that way.
- **Validate at the resource boundary, not downstream.** New `Connection`/`Sync`/`Projection` fields go through `core/api/v1alpha1` validation before being stored, plus `controlplane/internal/validate` for anything that needs cross-resource context (e.g. which source/target Connection type pairs are valid). Don't add ad hoc validation in `workerd`, `broker`, or `web` for data that should have been rejected at apply-time.
- **Orchestration is reconciliation, not imperative calls.** New behavior in `workermgr` should compare desired state (Syncs in the store) against actual state (ReplicaSets in the cluster) and converge — level-triggered, safe to re-run — not one-off imperative Kubernetes API calls scattered elsewhere.
- **Keep `core` a pure shared library.** No service-specific logic, no service-specific dependencies — only types, proto contracts, and generic infrastructure helpers (`logging`, `redisutil`). It stays Go-only; `workerd` consumes the `.proto` files directly rather than `core` itself, since Go and Rust can't share generated code.
- **gRPC internally, REST only at the two human-facing edges** (`controlplane` apply API, `broker` query API) — plus `workerd`'s REST calls to `controlplane` to fetch its own Sync/Connection specs, an established exception to the same rule the old Go worker also used. Don't add REST between two backend services beyond that, and don't add gRPC for `web`-facing endpoints.
- **New store/source adapters, not new branches.** Adding a new target store to `broker` means adding an adapter behind the existing port (`internal/executors`). Adding a new Sync pair to `workerd` means a new pipeline module dispatched from `main.rs`, not an `if`/`switch` on type deep inside an existing pipeline.

## Local Dev

Run `make up` to build images and deploy the full stack to the Rancher Desktop Kubernetes cluster, and `make run-producer` to generate synthetic Kafka events against it. See the `Makefile` for the full set of targets (`down`, `logs`, `test`, `tidy`, `proto`). `make up` also builds `workerd`'s image from its own `Dockerfile` (Rust toolchain, not Go) and runs a one-time MinIO bucket-provisioning Job (`infra/datalake/bucket-init-job.yaml`).
