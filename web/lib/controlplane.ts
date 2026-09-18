// Typed client for the Control Plane's REST API (controlplane/internal/httpapi).
// Runs server-side only (Server Components + Server Actions) — CONTROLPLANE_URL
// is not prefixed with NEXT_PUBLIC_, so it never reaches the browser.

const BASE = process.env.CONTROLPLANE_URL ?? "http://localhost:8080";

export type Kind = "Connection" | "Sync" | "Projection";

export interface KafkaConnectionSpec {
  brokers: string[];
  saslMechanism?: string;
  credentialsRef?: string;
}

export interface PostgresConnectionSpec {
  dsn: string;
  credentialsRef?: string;
}

export interface DatalakeConnectionSpec {
  endpoint: string;
  bucket: string;
  credentialsRef?: string;
}

export interface ConnectionSpec {
  type: "kafka" | "postgres" | "datalake";
  kafka?: KafkaConnectionSpec;
  postgres?: PostgresConnectionSpec;
  datalake?: DatalakeConnectionSpec;
}

export interface SyncFieldMapping {
  jsonPath: string;
  column: string;
  type: string;
}

export interface SyncSpec {
  source: { connectionRef: string; topic: string };
  target: { connectionRef: string; table: string };
  mode: "streaming" | "scheduled";
  replication: "delta" | "snapshot";
  schedule?: string;
  mapping: { keyField: string; schema: SyncFieldMapping[] };
  consumerGroup?: string;
}

export interface ProjectionSpec {
  sources: { syncRef: string; maxStalenessSeconds?: number }[];
  queryable: { table: string; defaultLimit?: number };
}

// Resource<T> mirrors httpapi.okResponse: the decoded spec plus the raw YAML
// text as stored, so the UI can round-trip the exact text a user submitted.
export interface Resource<TSpec> {
  kind: Kind;
  name: string;
  spec: TSpec;
  raw: string;
}

export type ConnectionResource = Resource<ConnectionSpec>;
export type SyncResource = Resource<SyncSpec>;
export type ProjectionResource = Resource<ProjectionSpec>;

export type ApplyResult = { ok: true } | { ok: false; error: string };

interface ErrorBody {
  error: string;
  code: string;
}

async function get<T>(path: string): Promise<T | null> {
  const res = await fetch(`${BASE}${path}`, { cache: "no-store" });
  if (res.status === 404) return null;
  if (!res.ok) {
    const body = (await res.json()) as ErrorBody;
    throw new Error(body.error);
  }
  return (await res.json()) as T;
}

export const listConnections = () =>
  get<{ items: ConnectionResource[] }>("/api/v1alpha1/connections").then((r) => r?.items ?? []);
export const getConnection = (name: string) =>
  get<ConnectionResource>(`/api/v1alpha1/connections/${encodeURIComponent(name)}`);

export const listSyncs = () =>
  get<{ items: SyncResource[] }>("/api/v1alpha1/syncs").then((r) => r?.items ?? []);
export const getSync = (name: string) =>
  get<SyncResource>(`/api/v1alpha1/syncs/${encodeURIComponent(name)}`);

export const listProjections = () =>
  get<{ items: ProjectionResource[] }>("/api/v1alpha1/projections").then((r) => r?.items ?? []);
export const getProjection = (name: string) =>
  get<ProjectionResource>(`/api/v1alpha1/projections/${encodeURIComponent(name)}`);

export async function applyResource(yamlText: string): Promise<ApplyResult> {
  const res = await fetch(`${BASE}/api/v1alpha1/apply`, {
    method: "POST",
    body: yamlText,
    cache: "no-store",
  });
  if (!res.ok) {
    const body = (await res.json()) as ErrorBody;
    return { ok: false, error: body.error };
  }
  return { ok: true };
}

export async function deleteResource(kind: Kind, name: string): Promise<void> {
  const path = kindPath(kind);
  const res = await fetch(`${BASE}/api/v1alpha1/${path}/${encodeURIComponent(name)}`, {
    method: "DELETE",
    cache: "no-store",
  });
  if (!res.ok && res.status !== 404) {
    const body = (await res.json()) as ErrorBody;
    throw new Error(body.error);
  }
}

export function kindPath(kind: Kind): string {
  switch (kind) {
    case "Connection":
      return "connections";
    case "Sync":
      return "syncs";
    case "Projection":
      return "projections";
  }
}
