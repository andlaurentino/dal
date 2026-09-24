"use server";

import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { applyResource, deleteResource, kindPath, type Kind } from "@/lib/controlplane";
import { connectionSchema, nameSchema, projectionSchema, syncSchema } from "@/lib/schemas";
import {
  buildConnectionSpec,
  buildProjectionSpec,
  buildSyncSpec,
  parseYamlDocument,
  zodFieldErrors,
} from "@/lib/resourceForm";
import type { FormActionState } from "@/lib/form-state";

function applyDone(kind: Kind, name: string): never {
  const path = kindPath(kind);
  revalidatePath(`/${path}`);
  revalidatePath(`/${path}/${name}`);
  redirect(`/${path}/${name}`);
}

async function submitApply(kind: Kind, name: string, spec: unknown): Promise<FormActionState> {
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

  applyDone(kind, name);
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

  const spec = buildConnectionSpec(formData);

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

  const spec = buildSyncSpec(formData);

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

  const spec = buildProjectionSpec(formData);

  const parsed = projectionSchema.safeParse(spec);
  if (!parsed.success) {
    return { ok: false, fieldErrors: zodFieldErrors(parsed.error.issues) };
  }

  return submitApply("Projection", name, parsed.data);
}

export async function applyRawResourceAction(
  kind: Kind,
  expectedName: string | undefined,
  _prevState: FormActionState,
  formData: FormData,
): Promise<FormActionState> {
  const yamlText = String(formData.get("yaml") ?? "");
  const doc = parseYamlDocument(yamlText);
  if (!doc.ok) {
    return { ok: false, formError: `Invalid YAML: ${doc.error}` };
  }
  if (doc.kind !== kind) {
    return { ok: false, formError: `kind must be "${kind}"` };
  }
  if (expectedName && doc.name !== expectedName) {
    return {
      ok: false,
      formError: `Renaming via YAML isn't supported — keep metadata.name as "${expectedName}"`,
    };
  }

  const result = await applyResource(yamlText);
  if (!result.ok) {
    return { ok: false, formError: result.error };
  }

  applyDone(kind, doc.name);
}

export async function deleteResourceAction(kind: Kind, name: string): Promise<void> {
  await deleteResource(kind, name);
  revalidatePath(`/${kindPath(kind)}`);
  redirect(`/${kindPath(kind)}`);
}
