# DAL — Architecture & Conventions

DAL (Data Abstraction Layer) is a set of Go microservices, orchestrated on Kubernetes, that move data from source systems into query-optimized stores and serve reads back through a stable, storage-agnostic query interface. This file documents the architecture and the conventions new work in this repo must follow.

## Overview

Users declare `Connection`, `Sync`, and `Projection` resources (YAML, CRD-style) against `controlplane`. `controlplane` validates and stores them, then reconciles a Kubernetes `ReplicaSet` per `Sync` so a dedicated `worker` Pod moves that Sync's data continuously or on a schedule. `broker` answers queries against named `Projection`s without callers ever knowing which physical store or table backs them. `web` is the UI over both.

## Components

### `controlplane` (`controlplaned`)
- Source of truth for `Connection`/`Sync`/`Projection` resources — stored in Redis (`internal/store`).
- Validates every resource on apply (`core/api/v1alpha1/validation.go`) before persisting.
- Exposes REST (`internal/httpapi`) for `web`/CLI apply and delete.
- Exposes gRPC `WorkerManager` and `QueryPlanner` servers (`internal/grpcserver`) for `worker` and `broker`.
- Owns Kubernetes orchestration of workers (`internal/workermgr`) — reconciles one `ReplicaSet` per `Sync`.

### `worker` (`workerd`)
- One process per `Sync`, run as a Kubernetes `ReplicaSet` Pod created by `controlplane/internal/workermgr`.
- Moves data per the Sync's spec (`internal/kafkapg`): Kafka source → Postgres target, either `streaming` (continuous consume) or `scheduled` (in-process `robfig/cron`-driven drain cycles).
- Registers on boot, heartbeats, and reports fatal errors to `controlplane` over gRPC (`internal/cpclient`).

### `broker` (`brokerd`)
- Serves queries against named `Projection`s.
- Asks `controlplane`'s `QueryPlanner.ResolvePlan` which physical store/connection/table currently backs a Projection.
- Executes the resolved query via a pluggable executor (`internal/executors`; currently a `postgres` adapter).

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
`broker/internal/executors` is a port with a `postgres` adapter implementation. Adding a new target store type for queries means adding a new adapter behind that port — never branching on store type inside broker's core logic. Apply the same discipline to `worker/internal/kafkapg` as new Sync sources/targets are added: new adapter, not a new special case.

### Data Abstraction Layer
The reason `Sync` + `Projection` + `broker` exist as separate concepts: callers query a named `Projection` and never know or care which physical store/connection/table currently answers it. `QueryPlanner.ResolvePlan` (`core/proto/controlplane/v1/query_plan.proto`) is the resolution seam, deliberately designed to leave room for future multi-store tiering (time-range/filter-aware routing) without breaking the contract.

### Kubernetes-Native Per-Sync Orchestration
`controlplane/internal/workermgr` reconciles one `ReplicaSet` (labels `app=dal-worker,sync=<name>`) per `Sync` — created on apply, deleted on Sync delete, recreated (delete-then-recreate) when the Sync's spec changes so `workerd` picks up new values at startup. Restart-on-crash is delegated entirely to Kubernetes' built-in ReplicaSet controller — never reimplement backoff/restart logic in application code.

**Important distinction**: a Sync's `mode: scheduled` does **not** create a Kubernetes `Job`/`CronJob`. The Pod is always a long-running `ReplicaSet` Pod; `streaming` vs `scheduled` is decided entirely inside `workerd` (`worker/internal/kafkapg.Run`), using an in-process `robfig/cron` scheduler for `scheduled` mode. Kubernetes is only asked to replace the ReplicaSet when the Sync spec changes — it has no awareness of the cron schedule itself.

## Frameworks & Libraries

| Library | Used for | Why |
|---|---|---|
| `k8s.io/client-go` | Cluster orchestration from `controlplane` | In-cluster config + typed clientset is the standard way to manage native objects (`ReplicaSet`) from inside a Pod |
| `google.golang.org/grpc` + `protoc`-generated stubs | Service-to-service calls (`worker`→`controlplane`, `broker`→`controlplane`) | Typed contracts, low overhead, streaming-friendly; reserved for internal service-to-service traffic |
| REST (`net/http`) | `controlplane` apply API, `broker` query API | The only two human/browser-facing edges — kept as plain REST for `web`/CLI ergonomics |
| `segmentio/kafka-go` | Kafka consumer in `worker` | Pure-Go, consumer-group support |
| `jackc/pgx` | Postgres access in `worker`/`broker` | Fast, well-maintained native Postgres driver |
| `robfig/cron/v3` | In-process scheduling for `scheduled`-mode Syncs | Standard 5-field cron parsing/scheduling without a Kubernetes CronJob per Sync |
| Redis | `controlplane`'s resource store | Simple, fast KV store sufficient for resource specs + heartbeat TTLs |
| Next.js / TypeScript | `web` | UI framework for the authoring/query console |

## Good Practices / Conventions

- **Hostname-based service discovery only.** Every service address is an env var or Kubernetes Service DNS name (`kafka:9092`, `postgres:5432`, `controlplane:9090`, etc.) — never hardcode an address. This is what made the docker-compose→Kubernetes migration a zero-application-code-change operation; keep it that way.
- **Validate at the resource boundary, not downstream.** New `Connection`/`Sync`/`Projection` fields go through `core/api/v1alpha1` validation before being stored. Don't add ad hoc validation in `worker`, `broker`, or `web` for data that should have been rejected at apply-time.
- **Orchestration is reconciliation, not imperative calls.** New behavior in `workermgr` should compare desired state (Syncs in the store) against actual state (ReplicaSets in the cluster) and converge — level-triggered, safe to re-run — not one-off imperative Kubernetes API calls scattered elsewhere.
- **Keep `core` a pure shared library.** No service-specific logic, no service-specific dependencies — only types, proto contracts, and generic infrastructure helpers (`logging`, `redisutil`).
- **gRPC internally, REST only at the two human-facing edges** (`controlplane` apply API, `broker` query API). Don't add REST between two backend services, and don't add gRPC for `web`-facing endpoints.
- **New store/source adapters, not new branches.** Adding a new target store to `broker` or a new source/target to `worker` means adding an adapter behind the existing port (`internal/executors`, `internal/kafkapg`), not an `if`/`switch` on type deep in shared logic.

## Local Dev

Run `make up` to build images and deploy the full stack to the Rancher Desktop Kubernetes cluster, and `make run-producer` to generate synthetic Kafka events against it. See the `Makefile` for the full set of targets (`down`, `logs`, `test`, `tidy`, `proto`).
