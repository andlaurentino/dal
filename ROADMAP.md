# Roadmap

Architecture cleanup roadmap coming out of the RustFS migration, which surfaced a real bug (duplicated, drifting S3 credential logic across `broker` and `workerd`) that turned out to be a symptom of a broader pattern: several places in DAL don't yet live up to the hexagonal/ports-and-adapters discipline `CLAUDE.md` prescribes, and the `Projection`/`Sync` resource model has design gaps blocking real flexibility (tiering hardcoded to exactly two named syncs, no per-source table/column control, worker health entirely unobservable).

Full audit and design rationale for each item lives in the architecture review conversation; this file tracks phase status only. Check items off as they land.

## Phase 1 — Foundation (zero schema/breaking changes)

- [x] Broker executor hexagonal port fix — replace the hardcoded `switch sp.GetStoreType()` in `broker/internal/httpapi/handler.go` with a real `Executor` interface implemented by `postgres.Executor` and `lake.Executor`
- [x] S3 config contract test — `broker/internal/executors/lake/s3_contract_test.go` (Go, `-tags=integration`) + `workerd/src/s3config.rs`'s `#[ignore]`d Rust test, both reading the same `DAL_TEST_S3_*`/`S3_ACCESS_KEY`/`S3_SECRET_KEY` env vars against a live store. Go half verified live against RustFS (passes with correct creds, fails with the exact 403 class from the RustFS-migration outage)
- [x] Validation hardening — reject duplicate `Projection.spec.sources[].syncRef`; reject a multi-source Projection with a source missing `routing` (verified live: both rejections confirmed against the deployed controlplane)

## Phase 2 — Projection schema redesign

- [x] Drop `spec.tiering` (`recentSyncRef`/`historicalSyncRef`); replace with per-source `routing` (discriminated union, `type: timeRange` implemented, `type: partition` reserved for later)
- [x] Per-source `view` (table + column mapping/rename), replacing the single global `Queryable.Table`
- [x] Planner: generalized `Resolve` from exactly-2 (`resolveTiered`) to N ordered `StorePlan`s, sorted youngest-to-oldest by routing window
- [x] Broker: collapsed the old fixed-pair stitch into one N-way loop (`handler.go`'s `handleQuery`) — a single-source Projection is now just the one-source case of the same loop, no separate code path
- [x] New cross-resource validation: non-overlapping/contiguous routing windows; consistent post-rename column shape across all sources of one Projection; routing/column refs checked against each Sync's actual mapping

Verified end-to-end against the live cluster: `events-recent` (postgres-only, 30 rows post-prune), `events-history` (lake-only, 230 rows full history), `events-combined` (2-source routed, 230 rows, correctly stitched, no double-counting). Examples and web UI (types, form, list/detail pages) updated to match.

## Phase 3 — Worker status

- [ ] Persist heartbeats: `grpcserver.Server`'s `Heartbeat`/`RegisterWorker`/`ReportError` write into `SyncObservedKey`/`SyncHeartbeatKey` (defined, currently unused) instead of only logging
- [ ] New read API: `GET /api/v1alpha1/syncs/{name}/status` (or `/workers`), combining observed state with live ReplicaSet/Pod status
- [ ] Web dashboard: `/workers` page (phase, last-heartbeat age, consumer lag, watermark/checkpoint, last error), client-side polling

Scope note: workers only — broker stays static infra, out of scope (controlplane has no awareness of it today).

## Phase 4 — Lineage visualization

- [ ] Client-side DAG render over existing Connection/Sync/Projection fetches (no new backend needed)
- [ ] Optional: overlay live health from Phase 3 once available

## Open questions

- `MaxStalenessSeconds`/`StalenessSeconds`: drop as dead schema, or wire up for real?
- Resource versioning/audit trail + dangling-reference protection on delete: worth its own initiative?
