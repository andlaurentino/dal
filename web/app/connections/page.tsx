import Link from "next/link";
import { ArrowRight, Plus } from "lucide-react";
import { listConnections } from "@/lib/controlplane";
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

export default async function ConnectionsPage() {
  const connections = await listConnections();

  return (
    <div className="grid gap-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Connections</h1>
          <p className="text-sm text-muted-foreground">
            Endpoints for the storage engines Syncs read from and write to.
          </p>
        </div>
        <Button
          render={
            <Link href="/connections/new">
              <Plus />
              New connection
            </Link>
          }
        />
      </div>

      <div className="overflow-hidden rounded-xl border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Type</TableHead>
              <TableHead className="w-10" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {connections.map((c) => (
              <TableRow key={c.name}>
                <TableCell className="font-medium">
                  <Link href={`/connections/${c.name}`} className="hover:underline">
                    {c.name}
                  </Link>
                </TableCell>
                <TableCell>
                  <Badge variant="secondary">{c.spec.type}</Badge>
                </TableCell>
                <TableCell>
                  <Link href={`/connections/${c.name}`}>
                    <ArrowRight className="size-4 text-muted-foreground" />
                  </Link>
                </TableCell>
              </TableRow>
            ))}
            {connections.length === 0 && (
              <TableRow>
                <TableCell colSpan={3} className="py-8 text-center text-muted-foreground">
                  No connections yet.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
