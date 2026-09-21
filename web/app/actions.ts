"use server";

import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { applyResource, deleteResource, kindPath, type Kind } from "@/lib/controlplane";
import { connectionSchema, nameSchema, projectionSchema, syncSchema } from "@/lib/schemas";
import type { FormActionState } from "@/lib/form-state";

function zodFieldErrors(issues: { path: PropertyKey[]; message: string }[]): Record<string, string> {
  const errors: Record<string, string> = {};
  for (const issue of issues) {
    const path = issue.path.join(".");
    if (!(path in errors)) errors[path] = issue.message;
  }
  return errors;
}

async function submitApply(
  kind: Kind,
  name: string,
  spec: unknown,
): Promise<FormActionState> {
  const body = JSON.stringify({
    apiVersion: "dal.io/v1alpha1",
    kind,
    metadata: { name },
    spec,
  });

  const result = await applyResource(body);
  if (!result.ok) {
    return { ok: false, formError: result.error };
  }

  const path = kindPath(kind);
  revalidatePath(`/${path}`);
  revalidatePath(`/${path}/${name}`);
  redirect(`/${path}/${name}`);
}

export async function applyConnectionAction(
  _prevState: FormActionState,
  formData: FormData,
): Promise<FormActionState> {
  const name = String(formData.get("name") ?? "");
  const nameCheck = nameSchema.safeParse(name);
  if (!nameCheck.success) {
    return { ok: false, fieldErrors: { name: nameCheck.error.issues[0].message } };
  }

  const type = String(formData.get("type") ?? "");
  let spec: Record<string, unknown>;
  if (type === "kafka") {
    spec = {
      type,
      kafka: {
        brokers: String(formData.get("brokers") ?? "")
          .split(",")
          .map((b) => b.trim())
          .filter(Boolean),
        saslMechanism: String(formData.get("saslMechanism") ?? "") || undefined,
      },
    };
  } else if (type === "postgres") {
    spec = { type, postgres: { dsn: String(formData.get("dsn") ?? "") } };
  } else {
    spec = {
      type,
      datalake: {
        endpoint: String(formData.get("endpoint") ?? ""),
        bucket: String(formData.get("bucket") ?? ""),
      },
    };
  }

  const parsed = connectionSchema.safeParse(spec);
  if (!parsed.success) {
    return { ok: false, fieldErrors: zodFieldErrors(parsed.error.issues) };
  }

  return submitApply("Connection", name, parsed.data);
}

export async function applySyncAction(
  _prevState: FormActionState,
  formData: FormData,
): Promise<FormActionState> {
  const name = String(formData.get("name") ?? "");
  const nameCheck = nameSchema.safeParse(name);
  if (!nameCheck.success) {
    return { ok: false, fieldErrors: { name: nameCheck.error.issues[0].message } };
  }

  let mappingSchema: unknown[] = [];
  try {
    mappingSchema = JSON.parse(String(formData.get("mappingSchema") ?? "[]"));
  } catch {
    mappingSchema = [];
  }

  const spec = {
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

  const parsed = syncSchema.safeParse(spec);
  if (!parsed.success) {
    return { ok: false, fieldErrors: zodFieldErrors(parsed.error.issues) };
  }

  return submitApply("Sync", name, parsed.data);
}

export async function applyProjectionAction(
  _prevState: FormActionState,
  formData: FormData,
): Promise<FormActionState> {
  const name = String(formData.get("name") ?? "");
  const nameCheck = nameSchema.safeParse(name);
  if (!nameCheck.success) {
    return { ok: false, fieldErrors: { name: nameCheck.error.issues[0].message } };
  }

  let sources: unknown[] = [];
  try {
    sources = JSON.parse(String(formData.get("sources") ?? "[]"));
  } catch {
    sources = [];
  }

  const defaultLimitRaw = String(formData.get("defaultLimit") ?? "");
  const spec = {
    sources,
    queryable: {
      table: String(formData.get("table") ?? ""),
      defaultLimit: defaultLimitRaw || undefined,
    },
  };

  const parsed = projectionSchema.safeParse(spec);
  if (!parsed.success) {
    return { ok: false, fieldErrors: zodFieldErrors(parsed.error.issues) };
  }

  return submitApply("Projection", name, parsed.data);
}

export async function deleteResourceAction(kind: Kind, name: string): Promise<void> {
  await deleteResource(kind, name);
  revalidatePath(`/${kindPath(kind)}`);
  redirect(`/${kindPath(kind)}`);
}
