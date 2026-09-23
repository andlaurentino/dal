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
  table: string;
  timestampColumn?: string;
  minAge?: string;
  maxAge?: string;
}

function rowsFromInitial(initial?: ProjectionResource): SourceRow[] {
  if (!initial?.spec.sources.length) {
    return [{ syncRef: "", table: "" }];
  }
  return initial.spec.sources.map((s) => ({
    syncRef: s.syncRef,
    table: s.view.table,
    timestampColumn: s.routing?.timestampColumn,
    minAge: s.routing?.minAge,
    maxAge: s.routing?.maxAge,
  }));
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
  const [rows, setRows] = useState<SourceRow[]>(rowsFromInitial(initial));
  const errors = state.fieldErrors ?? {};
  const needsRouting = rows.length > 1;

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
            onClick={() => setRows((prev) => [...prev, { syncRef: "", table: "" }])}
          >
            <Plus />
            Add source
          </Button>
        </div>
        {errors.sources && <p className="text-xs text-destructive">{errors.sources}</p>}
        {needsRouting && (
          <p className="text-xs text-muted-foreground">
            With more than one source, each needs its own routing: a timestamp column and an
            age window (minAge/maxAge, e.g. &quot;168h&quot;) that, together across all sources,
            partition time with no gaps or overlaps — youngest source&apos;s minAge and oldest
            source&apos;s maxAge stay blank.
          </p>
        )}

        <div className="grid gap-3">
          {rows.map((row, i) => (
            <div key={i} className="grid gap-2 rounded-lg border border-border p-3">
              <div className="grid grid-cols-[1fr_1fr_auto] items-end gap-2">
                <Field label="Sync" htmlFor={`sync-${i}`}>
                  <Select
                    value={row.syncRef || undefined}
                    onValueChange={(v) => updateRow(i, { syncRef: v ?? "" })}
                  >
                    <SelectTrigger id={`sync-${i}`} className="w-full">
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
                </Field>
                <Field label="Table / path" htmlFor={`table-${i}`}>
                  <Input
                    id={`table-${i}`}
                    placeholder="user_events"
                    value={row.table}
                    onChange={(e) => updateRow(i, { table: e.target.value })}
                  />
                </Field>
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

              {needsRouting && (
                <div className="grid grid-cols-3 gap-2">
                  <Field label="Timestamp column" htmlFor={`ts-${i}`}>
                    <Input
                      id={`ts-${i}`}
                      placeholder="occurred_at"
                      value={row.timestampColumn ?? ""}
                      onChange={(e) => updateRow(i, { timestampColumn: e.target.value })}
                    />
                  </Field>
                  <Field label="Min age (blank = youngest)" htmlFor={`minage-${i}`}>
                    <Input
                      id={`minage-${i}`}
                      placeholder="168h"
                      value={row.minAge ?? ""}
                      onChange={(e) => updateRow(i, { minAge: e.target.value })}
                    />
                  </Field>
                  <Field label="Max age (blank = oldest)" htmlFor={`maxage-${i}`}>
                    <Input
                      id={`maxage-${i}`}
                      placeholder="168h"
                      value={row.maxAge ?? ""}
                      onChange={(e) => updateRow(i, { maxAge: e.target.value })}
                    />
                  </Field>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      <Separator />

      <Field label="Default limit (optional)" htmlFor="defaultLimit">
        <Input
          id="defaultLimit"
          name="defaultLimit"
          type="number"
          min={1}
          defaultValue={initial?.spec.queryable.defaultLimit}
        />
      </Field>

      <div>
        <Button type="submit" disabled={pending}>
          {pending && <Loader2 className="animate-spin" />}
          {mode === "create" ? "Create projection" : "Save changes"}
        </Button>
      </div>
    </form>
  );
}
