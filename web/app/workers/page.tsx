import Link from "next/link";
import { listWorkers } from "@/lib/controlplane";
import AutoRefresh from "@/components/AutoRefresh";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

function timeAgo(iso?: string): string {
  if (!iso) return "—";
  const seconds = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ago`;
}

function AliveBadge({ alive }: { alive: boolean }) {
  return (
    <Badge variant={alive ? "default" : "destructive"}>{alive ? "alive" : "not heartbeating"}</Badge>
  );
}

function PodBadge({ phase, ready }: { phase: string; ready: boolean }) {
  const variant = phase === "Running" && ready ? "default" : phase === "Missing" ? "outline" : "destructive";
  return <Badge variant={variant}>{phase}</Badge>;
}

export default async function WorkersPage() {
  const workers = await listWorkers();

  return (
    <div className="grid gap-6">
      <AutoRefresh intervalMs={5000} />
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Workers</h1>
        <p className="text-sm text-muted-foreground">
          Live status of every per-Sync worker Pod, reconciled by controlplane — Pod state from
          Kubernetes, everything else from workerd&apos;s own heartbeat. Refreshes every 5s.
        </p>
      </div>

      <div className="overflow-hidden rounded-xl border border-border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Sync</TableHead>
              <TableHead>Pod</TableHead>
              <TableHead>Heartbeat</TableHead>
              <TableHead>Phase</TableHead>
              <TableHead>Last heartbeat</TableHead>
              <TableHead>Consumer lag</TableHead>
              <TableHead>Watermark</TableHead>
              <TableHead>Last error</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {workers.map((w) => (
              <TableRow key={w.syncName}>
                <TableCell className="font-medium">
                  <Link href={`/syncs/${w.syncName}`} className="hover:underline">
                    {w.syncName}
                  </Link>
                </TableCell>
                <TableCell>
                  <PodBadge phase={w.podPhase} ready={w.ready} />
                  {w.restarts > 0 && (
                    <span className="ml-2 text-xs text-muted-foreground">
                      {w.restarts} restart{w.restarts === 1 ? "" : "s"}
                    </span>
                  )}
                </TableCell>
                <TableCell>
                  <AliveBadge alive={w.alive} />
                </TableCell>
                <TableCell>{w.observedPhase || "—"}</TableCell>
                <TableCell>{timeAgo(w.lastHeartbeatAt)}</TableCell>
                <TableCell>{w.consumerLag ?? "—"}</TableCell>
                <TableCell className="max-w-40 truncate">{w.watermark || "—"}</TableCell>
                <TableCell className="max-w-60 truncate text-destructive">
                  {w.lastError || "—"}
                </TableCell>
              </TableRow>
            ))}
            {workers.length === 0 && (
              <TableRow>
                <TableCell colSpan={8} className="py-8 text-center text-muted-foreground">
                  No workers — apply a Sync to spawn one.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
