import { listConnections, listProjections, listSyncs, listWorkers } from "@/lib/controlplane";
import { buildLineageGraph } from "@/lib/lineage";
import LineageGraphView from "@/components/LineageGraph";

export default async function LineagePage() {
  const [connections, syncs, projections, workers] = await Promise.all([
    listConnections(),
    listSyncs(),
    listProjections(),
    listWorkers(),
  ]);

  const graph = buildLineageGraph(connections, syncs, projections, workers);

  return (
    <div className="grid gap-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Lineage</h1>
        <p className="text-sm text-muted-foreground">
          Every table/topic → Sync → table/topic → Projection edge across the whole cluster,
          derived from the resources themselves — each table/topic node is labeled by name with
          its Connection as a subtitle; click a node to jump to it. The dot on a Sync reflects
          its worker&apos;s current heartbeat.
        </p>
      </div>

      <LineageGraphView graph={graph} />
    </div>
  );
}
