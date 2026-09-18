import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowRight, Pencil } from "lucide-react";
import { getSync } from "@/lib/controlplane";
import { Badge } from "@/components/ui/badge";
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
import RawSourceViewer from "@/components/RawSourceViewer";

export default async function SyncDetailPage({
  params,
}: {
  params: Promise<{ name: string }>;
}) {
  const { name } = await params;
  const sync = await getSync(name);
  if (!sync) notFound();

  const { spec } = sync;

  return (
    <div className="grid gap-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold tracking-tight">{sync.name}</h1>
          <Badge variant="secondary">{spec.mode}</Badge>
          <Badge variant="outline">{spec.replication}</Badge>
        </div>
        <div className="flex gap-2">
          <Button
            variant="outline"
            render={
              <Link href={`/syncs/${sync.name}/edit`}>
                <Pencil />
                Edit
              </Link>
            }
          />
          <DeleteButton kind="Sync" name={sync.name} />
        </div>
      </div>

      <div className="grid gap-4 sm:grid-cols-[1fr_auto_1fr] sm:items-center">
        <Card>
          <CardHeader>
            <CardTitle>Source</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-1 text-sm">
            <span className="text-muted-foreground">Connection</span>
            <Link href={`/connections/${spec.source.connectionRef}`} className="hover:underline">
              {spec.source.connectionRef}
            </Link>
            <span className="mt-2 text-muted-foreground">Topic</span>
            <span>{spec.source.topic}</span>
          </CardContent>
        </Card>
        <ArrowRight className="mx-auto hidden size-6 text-muted-foreground sm:block" />
        <Card>
          <CardHeader>
            <CardTitle>Target</CardTitle>
          </CardHeader>
          <CardContent className="grid gap-1 text-sm">
            <span className="text-muted-foreground">Connection</span>
            <Link href={`/connections/${spec.target.connectionRef}`} className="hover:underline">
              {spec.target.connectionRef}
            </Link>
            <span className="mt-2 text-muted-foreground">Table</span>
            <span>{spec.target.table}</span>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Field mapping</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="mb-3 text-sm text-muted-foreground">
            Key field: <span className="font-medium text-foreground">{spec.mapping.keyField}</span>
          </p>
          <div className="overflow-hidden rounded-lg border border-border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>JSON path</TableHead>
                  <TableHead>Column</TableHead>
                  <TableHead>Type</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {spec.mapping.schema.map((m, i) => (
                  <TableRow key={i}>
                    <TableCell className="font-mono text-xs">{m.jsonPath}</TableCell>
                    <TableCell>{m.column}</TableCell>
                    <TableCell>{m.type}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <RawSourceViewer raw={sync.raw} />
    </div>
  );
}
