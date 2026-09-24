"use client";

import { forwardRef, useActionState, useState } from "react";
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

interface ColumnRow {
  source: string;
  as?: string;
}

interface SourceRow {
  connectionRef: string;
  table: string;
  columns: ColumnRow[];
  timestampColumn?: string;
  minAge?: string;
  maxAge?: string;
}

function emptySourceRow(): SourceRow {
  return { connectionRef: "", table: "", columns: [{ source: "" }] };
}

function rowsFromInitial(initial?: ProjectionResource): SourceRow[] {
  if (!initial?.spec.sources.length) {
    return [emptySourceRow()];
  }
  return initial.spec.sources.map((s) => ({
    connectionRef: s.connectionRef,
    table: s.view.table,
    columns: s.view.columns.length ? s.view.columns : [{ source: "" }],
    timestampColumn: s.routing?.timestampColumn,
    minAge: s.routing?.minAge,
    maxAge: s.routing?.maxAge,
  }));
}

const ProjectionForm = forwardRef<
  HTMLFormElement,
  { mode: "create" | "edit"; initial?: ProjectionResource; connectionNames: string[] }
>(function ProjectionForm({ mode, initial, connectionNames }, ref) {
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

  function updateColumn(rowIndex: number, colIndex: number, patch: Partial<ColumnRow>) {
    setRows((prev) =>
      prev.map((row, i) =>
        i === rowIndex
          ? {
              ...row,
              columns: row.columns.map((c, j) => (j === colIndex ? { ...c, ...patch } : c)),
            }
          : row,
      ),
    );
  }

  function addColumn(rowIndex: number) {
    setRows((prev) =>
      prev.map((row, i) =>
        i === rowIndex ? { ...row, columns: [...row.columns, { source: "" }] } : row,
      ),
    );
  }

  function removeColumn(rowIndex: number, colIndex: number) {
    setRows((prev) =>
      prev.map((row, i) =>
        i === rowIndex ? { ...row, columns: row.columns.filter((_, j) => j !== colIndex) } : row,
      ),
    );
  }

  return (
    <form ref={ref} action={formAction} className="grid gap-6">
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
          <Input
            id="name"
            name="name"
            placeholder="my-projection"
            defaultValue={initial?.name}
            required
          />
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
            onClick={() => setRows((prev) => [...prev, emptySourceRow()])}
          >
            <Plus />
            Add source
          </Button>
        </div>
        {errors.sources && <p className="text-xs text-destructive">{errors.sources}</p>}
        {needsRouting && (
          <p className="text-xs text-muted-foreground">
            With more than one source, each needs its own routing: a timestamp column and an
            age window (minAge/maxAge, e.g. &quot;7d&quot; or &quot;168h&quot;) that, together across all sources,
            partition time with no gaps or overlaps — youngest source&apos;s minAge and oldest
            source&apos;s maxAge stay blank.
          </p>
        )}

        <div className="grid gap-3">
          {rows.map((row, i) => (
            <div key={i} className="grid gap-2 rounded-lg border border-border p-3">
              <div className="grid grid-cols-[1fr_1fr_auto] items-end gap-2">
                <Field label="Connection" htmlFor={`connection-${i}`}>
                  <Select
                    value={row.connectionRef || undefined}
                    onValueChange={(v) => updateRow(i, { connectionRef: v ?? "" })}
                  >
                    <SelectTrigger id={`connection-${i}`} className="w-full">
                      <SelectValue placeholder="Select a connection" />
                    </SelectTrigger>
                    <SelectContent>
                      {connectionNames.map((n) => (
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

              <div className="grid gap-2">
                <div className="flex items-center justify-between">
                  <Label>Columns</Label>
                  <Button type="button" variant="outline" size="sm" onClick={() => addColumn(i)}>
                    <Plus />
                    Add column
                  </Button>
                </div>
                <div className="grid gap-2">
                  {row.columns.map((col, j) => (
                    <div key={j} className="grid grid-cols-[1fr_1fr_auto] items-end gap-2">
                      <Field label="Column" htmlFor={`col-source-${i}-${j}`}>
                        <Input
                          id={`col-source-${i}-${j}`}
                          placeholder="occurred_at"
                          value={col.source}
                          onChange={(e) => updateColumn(i, j, { source: e.target.value })}
                        />
                      </Field>
                      <Field label="Rename to (optional)" htmlFor={`col-as-${i}-${j}`}>
                        <Input
                          id={`col-as-${i}-${j}`}
                          placeholder={col.source || "same name"}
                          value={col.as ?? ""}
                          onChange={(e) => updateColumn(i, j, { as: e.target.value })}
                        />
                      </Field>
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        disabled={row.columns.length === 1}
                        onClick={() => removeColumn(i, j)}
                      >
                        <Trash2 />
                      </Button>
                    </div>
                  ))}
                </div>
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
                      placeholder="7d"
                      value={row.minAge ?? ""}
                      onChange={(e) => updateRow(i, { minAge: e.target.value })}
                    />
                  </Field>
                  <Field label="Max age (blank = oldest)" htmlFor={`maxage-${i}`}>
                    <Input
                      id={`maxage-${i}`}
                      placeholder="7d"
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

      <div>
        <Button type="submit" disabled={pending}>
          {pending && <Loader2 className="animate-spin" />}
          {mode === "create" ? "Create projection" : "Save changes"}
        </Button>
      </div>
    </form>
  );
});

export default ProjectionForm;
