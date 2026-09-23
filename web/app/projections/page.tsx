import Link from "next/link";
import { ArrowRight, Plus } from "lucide-react";
import { listProjections } from "@/lib/controlplane";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

export default async function ProjectionsPage() {
  const projections = await listProjections();

  return (
    <div className="grid gap-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Projections</h1>
          <p className="text-sm text-muted-foreground">
            Queryable views composed from one or more Syncs.
          </p>
        </div>
        <Button
          render={
            <Link href="/projections/new">
              <Plus />
              New projection
            </Link>
          }
        />
      </div>

      <div className="overflow-hidden rounded-xl border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Tables</TableHead>
              <TableHead>Sources</TableHead>
              <TableHead className="w-10" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {projections.map((p) => (
              <TableRow key={p.name}>
                <TableCell className="font-medium">
                  <Link href={`/projections/${p.name}`} className="hover:underline">
                    {p.name}
                  </Link>
                </TableCell>
                <TableCell>{p.spec.sources.map((s) => s.view.table).join(", ")}</TableCell>
                <TableCell>{p.spec.sources.length}</TableCell>
                <TableCell>
                  <Link href={`/projections/${p.name}`}>
                    <ArrowRight className="size-4 text-muted-foreground" />
                  </Link>
                </TableCell>
              </TableRow>
            ))}
            {projections.length === 0 && (
              <TableRow>
                <TableCell colSpan={4} className="py-8 text-center text-muted-foreground">
                  No projections yet.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
