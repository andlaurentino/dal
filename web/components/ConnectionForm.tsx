"use client";

import { useActionState, useState } from "react";
import { applyConnectionAction } from "@/app/actions";
import { initialFormActionState } from "@/lib/form-state";
import type { ConnectionResource } from "@/lib/controlplane";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Loader2 } from "lucide-react";

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

export default function ConnectionForm({
  mode,
  initial,
}: {
  mode: "create" | "edit";
  initial?: ConnectionResource;
}) {
  const [state, formAction, pending] = useActionState(
    applyConnectionAction,
    initialFormActionState,
  );
  const [type, setType] = useState(initial?.spec.type ?? "kafka");
  const errors = state.fieldErrors ?? {};

  return (
    <form action={formAction} className="grid gap-6">
      {state.formError && (
        <p role="alert" className="rounded-lg bg-destructive/10 p-3 text-sm text-destructive">
          {state.formError}
        </p>
      )}

      <Field label="Name" htmlFor="name" error={errors.name}>
        {mode === "edit" ? (
          <>
            <Input id="name" value={initial?.name} disabled />
            <input type="hidden" name="name" value={initial?.name} />
          </>
        ) : (
          <Input id="name" name="name" placeholder="my-connection" required />
        )}
      </Field>

      <Field label="Type" htmlFor="type">
        <Select name="type" value={type} onValueChange={(v) => setType(v as typeof type)}>
          <SelectTrigger id="type" className="w-full">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="kafka">Kafka</SelectItem>
            <SelectItem value="postgres">Postgres</SelectItem>
            <SelectItem value="datalake">Datalake</SelectItem>
          </SelectContent>
        </Select>
      </Field>

      {type === "kafka" && (
        <>
          <Field label="Brokers (comma-separated)" htmlFor="brokers" error={errors["kafka.brokers"]}>
            <Input
              id="brokers"
              name="brokers"
              placeholder="kafka:9092"
              defaultValue={initial?.spec.kafka?.brokers.join(", ")}
            />
          </Field>
          <Field label="SASL mechanism (optional)" htmlFor="saslMechanism">
            <Input
              id="saslMechanism"
              name="saslMechanism"
              defaultValue={initial?.spec.kafka?.saslMechanism}
            />
          </Field>
        </>
      )}

      {type === "postgres" && (
        <Field label="DSN" htmlFor="dsn" error={errors["postgres.dsn"]}>
          <Input
            id="dsn"
            name="dsn"
            placeholder="postgres://user:pass@host:5432/db"
            defaultValue={initial?.spec.postgres?.dsn}
          />
        </Field>
      )}

      {type === "datalake" && (
        <>
          <Field label="Endpoint" htmlFor="endpoint" error={errors["datalake.endpoint"]}>
            <Input
              id="endpoint"
              name="endpoint"
              placeholder="https://minio:9000"
              defaultValue={initial?.spec.datalake?.endpoint}
            />
          </Field>
          <Field label="Bucket" htmlFor="bucket" error={errors["datalake.bucket"]}>
            <Input id="bucket" name="bucket" defaultValue={initial?.spec.datalake?.bucket} />
          </Field>
        </>
      )}

      <div>
        <Button type="submit" disabled={pending}>
          {pending && <Loader2 className="animate-spin" />}
          {mode === "create" ? "Create connection" : "Save changes"}
        </Button>
      </div>
    </form>
  );
}
