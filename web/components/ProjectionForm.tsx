"use client";

import { useActionState, useState } from "react";
import { Loader2, Plus, Trash2 } from "lucide-react";
import { applyProjectionAction } from "@/app/actions";
import { initialFormActionState } from "@/lib/form-state";
import type { ProjectionResource } from "@/lib/controlplane";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

function Field({
  label,
  htmlFor,
  error,
  children,
}: {
  label: string;
  htmlFor: string;
  error?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="grid gap-1.5">
      <Label htmlFor={htmlFor}>{label}</Label>
      {children}
      {error && <p className="text-xs text-destructive">{error}</p>}
    </div>
  );
}

interface SourceRow {
  syncRef: string;
  maxStalenessSeconds?: number;
}

export default function ProjectionForm({
  mode,
  initial,
  syncNames,
}: {
  mode: "create" | "edit";
  initial?: ProjectionResource;
  syncNames: string[];
}) {
  const [state, formAction, pending] = useActionState(
    applyProjectionAction,
    initialFormActionState,
  );
  const [rows, setRows] = useState<SourceRow[]>(
    initial?.spec.sources.length ? initial.spec.sources : [{ syncRef: "" }],
  );
  const errors = state.fieldErrors ?? {};

  function updateRow(index: number, patch: Partial<SourceRow>) {
    setRows((prev) => prev.map((row, i) => (i === index ? { ...row, ...patch } : row)));
  }

  return (
    <form action={formAction} className="grid gap-6">
      {state.formError && (
        <p role="alert" className="rounded-lg bg-destructive/10 p-3 text-sm text-destructive">
          {state.formError}
        </p>
      )}

      <input type="hidden" name="sources" value={JSON.stringify(rows)} />

      <Field label="Name" htmlFor="name" error={errors.name}>
        {mode === "edit" ? (
          <>
            <Input id="name" value={initial?.name} disabled />
            <input type="hidden" name="name" value={initial?.name} />
          </>
        ) : (
          <Input id="name" name="name" placeholder="my-projection" required />
        )}
      </Field>

      <Separator />

      <div className="grid gap-3">
        <div className="flex items-center justify-between">
          <Label>Sources</Label>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => setRows((prev) => [...prev, { syncRef: "" }])}
          >
            <Plus />
            Add source
          </Button>
        </div>
        {errors.sources && <p className="text-xs text-destructive">{errors.sources}</p>}

        <div className="grid gap-2">
          {rows.map((row, i) => (
            <div key={i} className="grid grid-cols-[1fr_180px_auto] items-center gap-2">
              <Select
                value={row.syncRef || undefined}
                onValueChange={(v) => updateRow(i, { syncRef: v ?? "" })}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select a sync" />
                </SelectTrigger>
                <SelectContent>
                  {syncNames.map((n) => (
                    <SelectItem key={n} value={n}>
                      {n}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Input
                type="number"
                min={0}
                placeholder="Max staleness (s)"
                value={row.maxStalenessSeconds ?? ""}
                onChange={(e) =>
                  updateRow(i, {
                    maxStalenessSeconds: e.target.value ? Number(e.target.value) : undefined,
                  })
                }
              />
              <Button
                type="button"
                variant="ghost"
                size="icon"
                disabled={rows.length === 1}
                onClick={() => setRows((prev) => prev.filter((_, j) => j !== i))}
              >
                <Trash2 />
              </Button>
            </div>
          ))}
        </div>
      </div>

      <Separator />

      <div className="grid gap-4 sm:grid-cols-2">
        <Field label="Queryable table" htmlFor="table" error={errors["queryable.table"]}>
          <Input
            id="table"
            name="table"
            defaultValue={initial?.spec.queryable.table}
            placeholder="user_events"
          />
        </Field>
        <Field label="Default limit (optional)" htmlFor="defaultLimit">
          <Input
            id="defaultLimit"
            name="defaultLimit"
            type="number"
            min={1}
            defaultValue={initial?.spec.queryable.defaultLimit}
          />
        </Field>
      </div>

      <div>
        <Button type="submit" disabled={pending}>
          {pending && <Loader2 className="animate-spin" />}
          {mode === "create" ? "Create projection" : "Save changes"}
        </Button>
      </div>
    </form>
  );
}
