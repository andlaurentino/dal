import type { LineageGraph, LineageNode } from "@/lib/lineage";

const COLUMN_WIDTH = 240;
const NODE_WIDTH = 188;
const NODE_HEIGHT = 48;
const ROW_GAP = 20;
const PADDING = 24;

const kindHref = (n: LineageNode) =>
  n.kind === "table"
    ? `/connections/${encodeURIComponent(n.connectionName ?? "")}`
    : n.kind === "sync"
      ? `/syncs/${encodeURIComponent(n.name)}`
      : `/projections/${encodeURIComponent(n.name)}`;

// Distinct border color per kind so the columns of a typical graph (source
// table/topic, Sync, target table/topic, Projection) stay visually
// separable even before reading labels — same idea as the colored nav
// icons, just reused here since there's no icon-in-SVG story without
// foreignObject overhead for a handful of glyphs.
const kindBorderClass: Record<LineageNode["kind"], string> = {
  table: "border-l-4 border-l-blue-500",
  sync: "border-l-4 border-l-amber-500",
  projection: "border-l-4 border-l-emerald-500",
};

export default function LineageGraphView({ graph }: { graph: LineageGraph }) {
  const layers: LineageNode[][] = Array.from({ length: graph.layerCount }, () => []);
  for (const n of graph.nodes) layers[n.layer].push(n);

  const positions = new Map<string, { x: number; y: number }>();
  for (const layer of layers) {
    layer.forEach((n, i) => {
      positions.set(n.id, {
        x: PADDING + n.layer * COLUMN_WIDTH,
        y: PADDING + i * (NODE_HEIGHT + ROW_GAP),
      });
    });
  }

  const maxRows = Math.max(1, ...layers.map((l) => l.length));
  const width = PADDING * 2 + Math.max(1, graph.layerCount - 1) * COLUMN_WIDTH + NODE_WIDTH;
  const height = PADDING * 2 + maxRows * NODE_HEIGHT + Math.max(0, maxRows - 1) * ROW_GAP;

  if (graph.nodes.length === 0) {
    return (
      <div className="flex h-40 items-center justify-center rounded-xl border border-border bg-card text-sm text-muted-foreground">
        No resources yet — apply a Connection, Sync, and Projection to see their lineage.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-xl border border-border bg-card p-4">
      <svg width={width} height={height} className="min-w-full">
        <defs>
          <marker
            id="lineage-arrow"
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse"
          >
            <path d="M0,0 L10,5 L0,10 z" className="fill-muted-foreground" />
          </marker>
        </defs>

        {graph.edges.map((e, i) => {
          const from = positions.get(e.from);
          const to = positions.get(e.to);
          if (!from || !to) return null;
          const x1 = from.x + NODE_WIDTH;
          const y1 = from.y + NODE_HEIGHT / 2;
          const x2 = to.x;
          const y2 = to.y + NODE_HEIGHT / 2;
          const midX = (x1 + x2) / 2;
          return (
            <path
              key={i}
              d={`M${x1},${y1} C${midX},${y1} ${midX},${y2} ${x2 - 6},${y2}`}
              className="fill-none stroke-border"
              strokeWidth={1.5}
              markerEnd="url(#lineage-arrow)"
            />
          );
        })}

        {graph.nodes.map((n) => {
          const pos = positions.get(n.id)!;
          return (
            <foreignObject key={n.id} x={pos.x} y={pos.y} width={NODE_WIDTH} height={NODE_HEIGHT}>
              <a
                href={kindHref(n)}
                className={`flex h-full flex-col justify-center gap-0.5 rounded-md border border-border bg-background px-3 py-1 shadow-sm transition-colors hover:bg-muted ${kindBorderClass[n.kind]}`}
              >
                <span className="flex items-center gap-1.5 truncate text-sm font-medium text-foreground">
                  {n.name}
                  {n.kind === "sync" && n.workerAlive !== undefined && (
                    <span
                      title={n.workerAlive ? "worker alive" : "worker not heartbeating"}
                      className={`inline-block size-1.5 shrink-0 rounded-full ${n.workerAlive ? "bg-emerald-500" : "bg-destructive"}`}
                    />
                  )}
                </span>
                <span className="truncate text-xs text-muted-foreground">
                  {n.kind === "table" && (n.connectionName ?? "")}
                  {n.kind === "sync" && `Sync${n.workerPhase ? ` · ${n.workerPhase}` : ""}`}
                  {n.kind === "projection" && "Projection"}
                </span>
              </a>
            </foreignObject>
          );
        })}
      </svg>
    </div>
  );
}
