import Link from "next/link";
import { ArrowRight, Plus } from "lucide-react";
import { listSyncs } from "@/lib/controlplane";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

export default async function SyncsPage() {
  const syncs = await listSyncs();

  return (
    <div className="grid gap-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Syncs</h1>
          <p className="text-sm text-muted-foreground">
            Replication jobs moving data from a source connection to a target connection.
          </p>
        </div>
        <Button
          render={
            <Link href="/syncs/new">
              <Plus />
              New sync
            </Link>
          }
        />
      </div>

      <div className="overflow-hidden rounded-xl border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Source → Target</TableHead>
              <TableHead>Mode</TableHead>
              <TableHead className="w-10" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {syncs.map((s) => (
              <TableRow key={s.name}>
                <TableCell className="font-medium">
                  <Link href={`/syncs/${s.name}`} className="hover:underline">
                    {s.name}
                  </Link>
                </TableCell>
                <TableCell>
                  <span className="inline-flex items-center gap-1.5 text-sm">
                    {s.spec.source.connectionRef}
                    <ArrowRight className="size-3.5 text-muted-foreground" />
                    {s.spec.target.connectionRef}
                  </span>
                </TableCell>
                <TableCell>
                  <Badge variant="secondary">{s.spec.mode}</Badge>
                </TableCell>
                <TableCell>
                  <Link href={`/syncs/${s.name}`}>
                    <ArrowRight className="size-4 text-muted-foreground" />
                  </Link>
                </TableCell>
              </TableRow>
            ))}
            {syncs.length === 0 && (
              <TableRow>
                <TableCell colSpan={4} className="py-8 text-center text-muted-foreground">
                  No syncs yet.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
