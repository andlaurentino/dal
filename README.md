# DAL — Data Abstraction Layer

The **Data Abstraction Layer (DAL)** decouples data consumers from underlying physical storage, enabling organizations to seamlessly move, synchronize, and optimize data locations across different sources without breaking downstream applications.

By routing data from source systems (e.g., Kafka) into query-optimized stores (e.g., Postgres, data lakes), DAL serves reads back through a stable, unified query interface. This ensures that callers never need to know which physical store or technology is actually answering a given query.

**Key Benefits:**
- **Stable Interface:** Downstream applications are insulated from infrastructure migrations or underlying database changes.
- **Data Mobility:** Create cross-source projections and optimize data storage locations transparently.
- **Declarative Operations:** Resources (`Connection`, `Sync`, `Projection`) are defined as YAML and applied to a control plane, which orchestrates per-Sync workers on Kubernetes to manage data movement.

![DAL architecture](./docs/images/architecture.png)

## Components

| Component | Responsibility |
|---|---|
| `controlplane` | Source of truth for resources; validates and stores them; orchestrates per-Sync workers on Kubernetes |
| `worker` | One process per `Sync`; moves data from a source connection to a target connection (streaming or scheduled) |
| `broker` | Serves queries against named `Projection`s, resolving the current physical store/table via the control plane |
| `web` | UI for authoring resources and running queries |
| `core` | Shared Go library: resource types, validation, gRPC contracts |
| `tools/producer` | Standalone synthetic Kafka event generator for local dev |

## Run Local

Requires a Kubernetes cluster (this repo targets Rancher Desktop's built-in cluster, context `rancher-desktop`).

```
make up             # build images and deploy the full stack
make run-producer   # generate synthetic Kafka events
make logs           # tail logs from every DAL-managed pod
make down           # tear the stack down
```
