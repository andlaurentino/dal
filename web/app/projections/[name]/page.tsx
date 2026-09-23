import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowRight, Pencil } from "lucide-react";
import {
  getProjection,
  listConnections,
  listProjections,
  listSyncs,
  listWorkers,
} from "@/lib/controlplane";
import { queryProjection } from "@/lib/broker";
import { buildLineageGraph, projectionId, subgraphAround } from "@/lib/lineage";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import DeleteButton from "@/components/DeleteButton";
import LineageGraphView from "@/components/LineageGraph";
import RawSourceViewer from "@/components/RawSourceViewer";

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  return String(value);
}

export default async function ProjectionDetailPage({
  params,
}: {
  params: Promise<{ name: string }>;
}) {
  const { name } = await params;
  const projection = await getProjection(name);
  if (!projection) notFound();

  const { spec } = projection;
  const sample = await queryProjection(name, { limit: 10, offset: 0 });

  const [connections, syncs, projections, workers] = await Promise.all([
    listConnections(),
    listSyncs(),
    listProjections(),
    listWorkers(),
  ]);
  const lineage = subgraphAround(
    buildLineageGraph(connections, syncs, projections, workers),
    projectionId(name),
  );

  return (
    <div className="grid gap-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">{projection.name}</h1>
        <div className="flex gap-2">
          <Button
            variant="outline"
            render={
              <Link href={`/projections/${projection.name}/edit`}>
                <Pencil />
                Edit
              </Link>
            }
          />
          <DeleteButton kind="Projection" name={projection.name} />
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Sources</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-hidden rounded-lg border border-border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Connection</TableHead>
                  <TableHead>Table / path</TableHead>
                  <TableHead>Columns</TableHead>
                  <TableHead>Routing</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {spec.sources.map((s) => (
                  <TableRow key={s.connectionRef}>
                    <TableCell>
                      <Link href={`/connections/${s.connectionRef}`} className="hover:underline">
                        {s.connectionRef}
                      </Link>
                    </TableCell>
                    <TableCell>{s.view.table}</TableCell>
                    <TableCell>
                      {s.view.columns.map((c) => (c.as ? `${c.source} as ${c.as}` : c.source)).join(", ")}
                    </TableCell>
                    <TableCell>
                      {s.routing
                        ? `${s.routing.timestampColumn}: ${s.routing.minAge ?? "0"} – ${s.routing.maxAge ?? "∞"}`
                        : "—"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Lineage</CardTitle>
          <Button variant="ghost" size="sm" render={<Link href="/lineage" />}>
            Full graph
            <ArrowRight />
          </Button>
        </CardHeader>
        <CardContent>
          <LineageGraphView graph={lineage} />
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Sample data</CardTitle>
          <Button
            variant="ghost"
            size="sm"
            render={<Link href={`/query/${encodeURIComponent(projection.name)}`} />}
          >
            View all
            <ArrowRight />
          </Button>
        </CardHeader>
        <CardContent>
          {!sample.ok ? (
            <p role="alert" className="text-sm text-destructive">
              {sample.error}
            </p>
          ) : (
            <div className="overflow-hidden rounded-lg border border-border">
              <Table>
                <TableHeader>
                  <TableRow>
                    {sample.data.columns.map((c) => (
                      <TableHead key={c.name}>{c.name}</TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sample.data.rows.length === 0 && (
                    <TableRow>
                      <TableCell
                        colSpan={sample.data.columns.length || 1}
                        className="text-center text-muted-foreground"
                      >
                        No rows.
                      </TableCell>
                    </TableRow>
                  )}
                  {sample.data.rows.map((row, i) => (
                    <TableRow key={i}>
                      {row.map((value, j) => (
                        <TableCell key={j}>{formatCell(value)}</TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      <RawSourceViewer raw={projection.raw} />
    </div>
  );
}
