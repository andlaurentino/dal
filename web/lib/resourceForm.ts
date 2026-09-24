// Pure, client-safe helpers shared between app/actions.ts (server actions,
// "use server" — every export there becomes an RPC, so builder logic can't
// live in that file) and the client-side Editor components that convert
// between form state and YAML text.

import { parse } from "yaml";
import type { Kind } from "@/lib/controlplane";

export function buildConnectionSpec(formData: FormData): Record<string, unknown> {
  const type = String(formData.get("type") ?? "");
  if (type === "kafka") {
    return {
      type,
      kafka: {
        brokers: String(formData.get("brokers") ?? "")
          .split(",")
          .map((b) => b.trim())
          .filter(Boolean),
        saslMechanism: String(formData.get("saslMechanism") ?? "") || undefined,
      },
    };
  }
  if (type === "postgres") {
    return { type, postgres: { dsn: String(formData.get("dsn") ?? "") } };
  }
  return {
    type,
    datalake: {
      endpoint: String(formData.get("endpoint") ?? ""),
      bucket: String(formData.get("bucket") ?? ""),
    },
  };
}

export function buildSyncSpec(formData: FormData): Record<string, unknown> {
  let mappingSchema: unknown[] = [];
  try {
    mappingSchema = JSON.parse(String(formData.get("mappingSchema") ?? "[]"));
  } catch {
    mappingSchema = [];
  }

  return {
    source: {
      connectionRef: String(formData.get("sourceConnectionRef") ?? ""),
      topic: String(formData.get("sourceTopic") ?? "") || undefined,
      path: String(formData.get("sourcePath") ?? "") || undefined,
      checkpointPath: String(formData.get("sourceCheckpointPath") ?? "") || undefined,
    },
    target: {
      connectionRef: String(formData.get("targetConnectionRef") ?? ""),
      table: String(formData.get("targetTable") ?? "") || undefined,
      path: String(formData.get("targetPath") ?? "") || undefined,
    },
    mode: String(formData.get("mode") ?? ""),
    replication: String(formData.get("replication") ?? ""),
    schedule: String(formData.get("schedule") ?? "") || undefined,
    consumerGroup: String(formData.get("consumerGroup") ?? "") || undefined,
    mapping: {
      keyField: String(formData.get("keyField") ?? ""),
      schema: mappingSchema,
    },
  };
}

export function buildProjectionSpec(formData: FormData): Record<string, unknown> {
  let rows: {
    connectionRef: string;
    table: string;
    columns: { source: string; as?: string }[];
    timestampColumn?: string;
    minAge?: string;
    maxAge?: string;
  }[] = [];
  try {
    rows = JSON.parse(String(formData.get("sources") ?? "[]"));
  } catch {
    rows = [];
  }

  const sources = rows.map((r) => ({
    connectionRef: r.connectionRef,
    view: { table: r.table, columns: r.columns },
    ...(rows.length > 1
      ? {
          routing: {
            type: "timeRange" as const,
            timestampColumn: r.timestampColumn ?? "",
            minAge: r.minAge || undefined,
            maxAge: r.maxAge || undefined,
          },
        }
      : {}),
  }));

  return { sources };
}

export function zodFieldErrors(issues: { path: PropertyKey[]; message: string }[]): Record<string, string> {
  const errors: Record<string, string> = {};
  for (const issue of issues) {
    const path = issue.path.join(".");
    if (!(path in errors)) errors[path] = issue.message;
  }
  return errors;
}

export type ParsedYamlDocument =
  | { ok: true; kind: Kind; name: string; spec: unknown }
  | { ok: false; error: string };

// Light structural read of the {apiVersion, kind, metadata: {name}, spec}
// envelope — not full validation. Full validation stays with Zod (Form
// submit path) and controlplane (YAML submit path); don't duplicate it here.
export function parseYamlDocument(yamlText: string): ParsedYamlDocument {
  let doc: unknown;
  try {
    doc = parse(yamlText);
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "could not parse YAML" };
  }

  if (typeof doc !== "object" || doc === null) {
    return { ok: false, error: "document must be a YAML mapping" };
  }
  const { kind, metadata, spec } = doc as Record<string, unknown>;
  if (typeof kind !== "string" || !["Connection", "Sync", "Projection"].includes(kind)) {
    return { ok: false, error: 'kind must be one of "Connection", "Sync", "Projection"' };
  }
  const name =
    typeof metadata === "object" && metadata !== null
      ? (metadata as Record<string, unknown>).name
      : undefined;
  if (typeof name !== "string" || !name) {
    return { ok: false, error: "metadata.name is required" };
  }
  if (typeof spec !== "object" || spec === null) {
    return { ok: false, error: "spec is required" };
  }

  return { ok: true, kind: kind as Kind, name, spec };
}
