import Link from "next/link";
import { notFound } from "next/navigation";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { getProjection } from "@/lib/controlplane";
import { queryProjection } from "@/lib/broker";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  return String(value);
}

export default async function QueryProjectionPage({
  params,
  searchParams,
}: {
  params: Promise<{ name: string }>;
  searchParams: Promise<{ page?: string }>;
}) {
  const { name } = await params;
  const { page: pageParam } = await searchParams;
  const projection = await getProjection(name);
  if (!projection) notFound();

  const limit = projection.spec.queryable.defaultLimit || 20;
  const page = Math.max(1, Number(pageParam) || 1);
  const offset = (page - 1) * limit;

  const result = await queryProjection(name, { limit, offset });

  return (
    <div className="grid gap-4">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">{name}</h1>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          {result.ok && result.data.tiered && (
            <span className="rounded-full border border-border px-2 py-0.5 text-xs">
              {projection.spec.sources.length} sources
            </span>
          )}
          <span>{projection.spec.sources.map((s) => s.view.table).join(", ")}</span>
        </div>
      </div>

      {!result.ok ? (
        <p role="alert" className="rounded-lg bg-destructive/10 p-3 text-sm text-destructive">
          {result.error}
        </p>
      ) : (
        <>
          <div className="overflow-hidden rounded-lg border border-border">
            <Table>
              <TableHeader>
                <TableRow>
                  {result.data.columns.map((c) => (
                    <TableHead key={c.name}>{c.name}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {result.data.rows.length === 0 && (
                  <TableRow>
                    <TableCell
                      colSpan={result.data.columns.length || 1}
                      className="text-center text-muted-foreground"
                    >
                      No rows.
                    </TableCell>
                  </TableRow>
                )}
                {result.data.rows.map((row, i) => (
                  <TableRow key={i}>
                    {row.map((value, j) => (
                      <TableCell key={j}>{formatCell(value)}</TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          <div className="flex items-center justify-between">
            <p className="text-sm text-muted-foreground">
              {result.data.total === 0
                ? "0 rows"
                : `${offset + 1}–${Math.min(offset + limit, result.data.total)} of ${result.data.total}`}
            </p>
            <div className="flex gap-2">
              {page <= 1 ? (
                <Button variant="outline" size="sm" disabled>
                  <ChevronLeft />
                  Previous
                </Button>
              ) : (
                <Button
                  variant="outline"
                  size="sm"
                  render={<Link href={`/query/${encodeURIComponent(name)}?page=${page - 1}`} />}
                >
                  <ChevronLeft />
                  Previous
                </Button>
              )}
              {offset + limit >= result.data.total ? (
                <Button variant="outline" size="sm" disabled>
                  Next
                  <ChevronRight />
                </Button>
              ) : (
                <Button
                  variant="outline"
                  size="sm"
                  render={<Link href={`/query/${encodeURIComponent(name)}?page=${page + 1}`} />}
                >
                  Next
                  <ChevronRight />
                </Button>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
