// Typed client for the broker's query API (broker/internal/httpapi). Runs
// server-side only (Server Components) — BROKER_URL is not prefixed with
// NEXT_PUBLIC_, so it never reaches the browser.

const BASE = process.env.BROKER_URL ?? "http://localhost:8081";

export interface QueryColumn {
  name: string;
  type: string;
}

export interface QueryResult {
  columns: QueryColumn[];
  rows: unknown[][];
  total: number;
  limit: number;
  offset: number;
}

export type QueryResponse = { ok: true; data: QueryResult } | { ok: false; error: string };

export async function queryProjection(
  name: string,
  { limit, offset }: { limit: number; offset: number },
): Promise<QueryResponse> {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  const res = await fetch(`${BASE}/query/${encodeURIComponent(name)}?${params}`, {
    cache: "no-store",
  });
  if (!res.ok) {
    const body = (await res.json().catch(() => ({ error: res.statusText }))) as { error: string };
    return { ok: false, error: body.error };
  }
  return { ok: true, data: (await res.json()) as QueryResult };
}
