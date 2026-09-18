"use client";

import { useActionState, useState } from "react";
import { Loader2, Plus, Trash2 } from "lucide-react";
import { applySyncAction } from "@/app/actions";
import { initialFormActionState } from "@/lib/form-state";
import type { SyncResource } from "@/lib/controlplane";
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

interface MappingRow {
  jsonPath: string;
  column: string;
  type: string;
}

export default function SyncForm({
  mode,
  initial,
  connectionNames,
}: {
  mode: "create" | "edit";
  initial?: SyncResource;
  connectionNames: string[];
}) {
  const [state, formAction, pending] = useActionState(applySyncAction, initialFormActionState);
  const [syncMode, setSyncMode] = useState(initial?.spec.mode ?? "streaming");
  const [rows, setRows] = useState<MappingRow[]>(
    initial?.spec.mapping.schema.length
      ? initial.spec.mapping.schema
      : [{ jsonPath: "", column: "", type: "" }],
  );
  const errors = state.fieldErrors ?? {};

  function updateRow(index: number, field: keyof MappingRow, value: string) {
    setRows((prev) => prev.map((row, i) => (i === index ? { ...row, [field]: value } : row)));
  }

  return (
    <form action={formAction} className="grid gap-6">
      {state.formError && (
        <p role="alert" className="rounded-lg bg-destructive/10 p-3 text-sm text-destructive">
          {state.formError}
        </p>
      )}

      <input type="hidden" name="mappingSchema" value={JSON.stringify(rows)} />

      <Field label="Name" htmlFor="name" error={errors.name}>
        {mode === "edit" ? (
          <>
            <Input id="name" value={initial?.name} disabled />
            <input type="hidden" name="name" value={initial?.name} />
          </>
        ) : (
          <Input id="name" name="name" placeholder="my-sync" required />
        )}
      </Field>

      <div className="grid gap-4 sm:grid-cols-2">
        <Field label="Source connection" htmlFor="sourceConnectionRef" error={errors["source.connectionRef"]}>
          <Select name="sourceConnectionRef" defaultValue={initial?.spec.source.connectionRef}>
            <SelectTrigger id="sourceConnectionRef" className="w-full">
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
        <Field label="Source topic" htmlFor="sourceTopic" error={errors["source.topic"]}>
          <Input
            id="sourceTopic"
            name="sourceTopic"
            defaultValue={initial?.spec.source.topic}
            placeholder="user-events"
          />
        </Field>
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        <Field label="Target connection" htmlFor="targetConnectionRef" error={errors["target.connectionRef"]}>
          <Select name="targetConnectionRef" defaultValue={initial?.spec.target.connectionRef}>
            <SelectTrigger id="targetConnectionRef" className="w-full">
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
        <Field label="Target table" htmlFor="targetTable" error={errors["target.table"]}>
          <Input
            id="targetTable"
            name="targetTable"
            defaultValue={initial?.spec.target.table}
            placeholder="user_events"
          />
        </Field>
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        <Field label="Mode" htmlFor="mode">
          <Select name="mode" value={syncMode} onValueChange={(v) => setSyncMode(v as typeof syncMode)}>
            <SelectTrigger id="mode" className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="streaming">Streaming</SelectItem>
              <SelectItem value="scheduled">Scheduled</SelectItem>
            </SelectContent>
          </Select>
        </Field>
        <Field label="Replication" htmlFor="replication">
          <Select name="replication" defaultValue={initial?.spec.replication ?? "delta"}>
            <SelectTrigger id="replication" className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="delta">Delta</SelectItem>
              <SelectItem value="snapshot">Snapshot</SelectItem>
            </SelectContent>
          </Select>
        </Field>
      </div>

      {syncMode === "scheduled" && (
        <Field label="Schedule (cron expression)" htmlFor="schedule" error={errors.schedule}>
          <Input
            id="schedule"
            name="schedule"
            placeholder="*/15 * * * *"
            defaultValue={initial?.spec.schedule}
          />
        </Field>
      )}

      <Field label="Consumer group (optional)" htmlFor="consumerGroup">
        <Input
          id="consumerGroup"
          name="consumerGroup"
          defaultValue={initial?.spec.consumerGroup}
        />
      </Field>

      <Separator />

      <div className="grid gap-3">
        <Field label="Key field" htmlFor="keyField" error={errors["mapping.keyField"]}>
          <Input
            id="keyField"
            name="keyField"
            defaultValue={initial?.spec.mapping.keyField}
            placeholder="id"
          />
        </Field>

        <div className="flex items-center justify-between">
          <Label>Field mappings</Label>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => setRows((prev) => [...prev, { jsonPath: "", column: "", type: "" }])}
          >
            <Plus />
            Add mapping
          </Button>
        </div>
        {errors["mapping.schema"] && (
          <p className="text-xs text-destructive">{errors["mapping.schema"]}</p>
        )}

        <div className="grid gap-2">
          {rows.map((row, i) => (
            <div key={i} className="grid grid-cols-[1fr_1fr_1fr_auto] items-center gap-2">
              <Input
                placeholder="JSON path"
                value={row.jsonPath}
                onChange={(e) => updateRow(i, "jsonPath", e.target.value)}
              />
              <Input
                placeholder="Column"
                value={row.column}
                onChange={(e) => updateRow(i, "column", e.target.value)}
              />
              <Input
                placeholder="Type"
                value={row.type}
                onChange={(e) => updateRow(i, "type", e.target.value)}
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

      <div>
        <Button type="submit" disabled={pending}>
          {pending && <Loader2 className="animate-spin" />}
          {mode === "create" ? "Create sync" : "Save changes"}
        </Button>
      </div>
    </form>
  );
}
