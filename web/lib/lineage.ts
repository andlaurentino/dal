// Builds a DAG purely from data already fetched elsewhere in this app
// (lib/controlplane.ts's list* calls) — no new backend endpoint. Kept
// separate from the rendering component so the graph-construction logic
// (in particular the layering) is easy to reason about on its own.
//
// Connections themselves aren't nodes — what actually matters for lineage
// is the physical table/topic/path each Sync reads from and writes to, so
// that's what's shown, labeled by name with its Connection as a subtitle.
// A Sync sits between its own source and target table/topic nodes.
// Projections attach to the table/topic node their view actually reads
// (view.table, under the referenced Sync's target Connection) rather than
// to the Sync itself — if a Projection's view.table doesn't match what its
// Sync nominally writes to, that shows up honestly as two separate nodes
// instead of silently glossing over the mismatch.
import type { ConnectionResource, ProjectionResource, SyncResource, WorkerStatus } from "./controlplane";

export type LineageNodeKind = "table" | "sync" | "projection";

export interface LineageNode {
  id: string;
  kind: LineageNodeKind;
  name: string;
  // Only set for kind "table" — the Connection this table/topic/path
  // belongs to, shown as a subtitle.
  connectionName?: string;
  layer: number;
  // Only ever set for kind "sync", when a matching entry exists in the
  // workers list passed to buildLineageGraph — undefined means "no
  // worker-status data available", not "worker is down".
  workerAlive?: boolean;
  workerPhase?: string;
}

export interface LineageEdge {
  from: string;
  to: string;
}

export interface LineageGraph {
  nodes: LineageNode[];
  edges: LineageEdge[];
  layerCount: number;
}

const tableId = (connectionRef: string, name: string) => `table:${connectionRef}:${name}`;

// Exported so callers (e.g. a Sync/Projection detail page) can compute the
// right focus id for subgraphAround without re-deriving this convention.
export const syncId = (name: string) => `sync:${name}`;
export const projectionId = (name: string) => `projection:${name}`;

export function buildLineageGraph(
  connections: ConnectionResource[],
  syncs: SyncResource[],
  projections: ProjectionResource[],
  workers: WorkerStatus[] = [],
): LineageGraph {
  const nodesById = new Map<string, LineageNode>();
  const workerByName = new Map(workers.map((w) => [w.syncName, w]));
  const connectionNames = new Map(connections.map((c) => [c.name, c.name]));

  function ensureTable(connectionRef: string, name: string): string | null {
    if (!name || !connectionNames.has(connectionRef)) return null;
    const id = tableId(connectionRef, name);
    if (!nodesById.has(id)) {
      nodesById.set(id, {
        id,
        kind: "table",
        name,
        connectionName: connectionRef,
        layer: 0,
      });
    }
    return id;
  }

  for (const s of syncs) {
    const w = workerByName.get(s.name);
    nodesById.set(syncId(s.name), {
      id: syncId(s.name),
      kind: "sync",
      name: s.name,
      layer: 0,
      workerAlive: w?.alive,
      workerPhase: w?.observedPhase,
    });
  }
  for (const p of projections) {
    nodesById.set(projectionId(p.name), {
      id: projectionId(p.name),
      kind: "projection",
      name: p.name,
      layer: 0,
    });
  }

  const edges: LineageEdge[] = [];
  const syncTargetConnection = new Map(syncs.map((s) => [s.name, s.spec.target.connectionRef]));

  for (const s of syncs) {
    const sourceName = s.spec.source.topic || s.spec.source.path || "";
    const targetName = s.spec.target.table || s.spec.target.path || "";

    const sourceTable = ensureTable(s.spec.source.connectionRef, sourceName);
    if (sourceTable) edges.push({ from: sourceTable, to: syncId(s.name) });

    const targetTable = ensureTable(s.spec.target.connectionRef, targetName);
    if (targetTable) edges.push({ from: syncId(s.name), to: targetTable });
  }

  for (const p of projections) {
    for (const src of p.spec.sources) {
      const connectionRef = syncTargetConnection.get(src.syncRef);
      if (!connectionRef) continue;
      const table = ensureTable(connectionRef, src.view.table);
      if (table) edges.push({ from: table, to: projectionId(p.name) });
    }
  }

  // Longest-path layering: a node's layer is one past the deepest of its
  // predecessors' layers (0 if it has none). Nothing here cycles back on
  // itself, so relaxing every node at most |nodes| times reaches a fixed
  // point.
  const incoming = new Map<string, string[]>();
  for (const e of edges) {
    incoming.set(e.to, [...(incoming.get(e.to) ?? []), e.from]);
  }
  let changed = true;
  for (let guard = 0; changed && guard <= nodesById.size; guard++) {
    changed = false;
    for (const node of nodesById.values()) {
      const preds = incoming.get(node.id) ?? [];
      const wantLayer =
        preds.length === 0 ? 0 : Math.max(...preds.map((id) => (nodesById.get(id)?.layer ?? 0) + 1));
      if (wantLayer !== node.layer) {
        node.layer = wantLayer;
        changed = true;
      }
    }
  }

  const nodes = [...nodesById.values()].sort(
    (a, b) => a.layer - b.layer || a.kind.localeCompare(b.kind) || a.name.localeCompare(b.name),
  );
  const layerCount = nodes.length ? Math.max(...nodes.map((n) => n.layer)) + 1 : 0;

  return { nodes, edges, layerCount };
}

// subgraphAround extracts the slice of graph actually connected to
// focusId — its full ancestor closure (everything upstream, transitively)
// and full descendant closure (everything downstream, transitively) — for
// embedding a focused lineage view on that one resource's own detail page,
// where showing the whole cluster's graph would be noise. Layers are
// renumbered relative to the subset's own minimum so the focused view
// always starts at column 0, regardless of where focusId sat in the full
// graph.
export function subgraphAround(graph: LineageGraph, focusId: string): LineageGraph {
  if (!graph.nodes.some((n) => n.id === focusId)) {
    return { nodes: [], edges: [], layerCount: 0 };
  }

  const outgoing = new Map<string, string[]>();
  const incoming = new Map<string, string[]>();
  for (const e of graph.edges) {
    outgoing.set(e.from, [...(outgoing.get(e.from) ?? []), e.to]);
    incoming.set(e.to, [...(incoming.get(e.to) ?? []), e.from]);
  }

  const included = new Set<string>([focusId]);
  function walk(start: string, adjacency: Map<string, string[]>) {
    const queue = [start];
    while (queue.length > 0) {
      const id = queue.shift()!;
      for (const next of adjacency.get(id) ?? []) {
        if (!included.has(next)) {
          included.add(next);
          queue.push(next);
        }
      }
    }
  }
  walk(focusId, incoming);
  walk(focusId, outgoing);

  const nodes = graph.nodes.filter((n) => included.has(n.id));
  const edges = graph.edges.filter((e) => included.has(e.from) && included.has(e.to));

  const minLayer = Math.min(...nodes.map((n) => n.layer));
  const renumbered = nodes.map((n) => ({ ...n, layer: n.layer - minLayer }));
  const layerCount = renumbered.length ? Math.max(...renumbered.map((n) => n.layer)) + 1 : 0;

  return { nodes: renumbered, edges, layerCount };
}
