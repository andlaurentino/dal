import { z } from "zod";

export const nameSchema = z
  .string()
  .min(1, "Name is required")
  .regex(/^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/, "Use lowercase letters, numbers, and hyphens only");

const kafkaConnectionSchema = z.object({
  brokers: z.array(z.string().min(1)).min(1, "At least one broker is required"),
  saslMechanism: z.string().optional(),
});

const postgresConnectionSchema = z.object({
  dsn: z.string().min(1, "DSN is required"),
});

const datalakeConnectionSchema = z.object({
  endpoint: z.string().min(1, "Endpoint is required"),
  bucket: z.string().min(1, "Bucket is required"),
});

export const connectionSchema = z.discriminatedUnion("type", [
  z.object({ type: z.literal("kafka"), kafka: kafkaConnectionSchema }),
  z.object({ type: z.literal("postgres"), postgres: postgresConnectionSchema }),
  z.object({ type: z.literal("datalake"), datalake: datalakeConnectionSchema }),
]);

export type ConnectionFormValues = z.infer<typeof connectionSchema>;

const syncFieldMappingSchema = z.object({
  jsonPath: z.string().min(1, "JSON path is required"),
  column: z.string().min(1, "Column is required"),
  type: z.string().min(1, "Type is required"),
});

// Source/target shape depends on the referenced Connection's type: a kafka
// source sets topic (path empty); a datalake source sets path+checkpointPath
// (topic empty) — see core/api/v1alpha1/sync_types.go. This form doesn't
// know the selected Connection's type ahead of submission, so it collects
// both and lets the exactly-one-of checks below (mirroring ValidateSync)
// catch the mismatch.
export const syncSchema = z
  .object({
    source: z.object({
      connectionRef: z.string().min(1, "Source connection is required"),
      topic: z.string().optional(),
      path: z.string().optional(),
      checkpointPath: z.string().optional(),
    }),
    target: z.object({
      connectionRef: z.string().min(1, "Target connection is required"),
      table: z.string().optional(),
      path: z.string().optional(),
    }),
    mode: z.enum(["streaming", "scheduled"]),
    replication: z.enum(["delta", "snapshot"]),
    schedule: z.string().optional(),
    consumerGroup: z.string().optional(),
    mapping: z.object({
      keyField: z.string().min(1, "Key field is required"),
      schema: z.array(syncFieldMappingSchema).min(1, "At least one field mapping is required"),
    }),
  })
  .refine((v) => v.mode !== "scheduled" || !!v.schedule, {
    message: "Schedule is required when mode is scheduled",
    path: ["schedule"],
  })
  .refine((v) => !!v.source.topic !== !!v.source.path, {
    message: "Exactly one of topic (kafka source) or path (datalake source) is required",
    path: ["source", "topic"],
  })
  .refine((v) => !v.source.path || !!v.source.checkpointPath, {
    message: "Checkpoint path is required when source path is set",
    path: ["source", "checkpointPath"],
  })
  .refine((v) => !!v.target.table !== !!v.target.path, {
    message: "Exactly one of table (postgres target) or path (datalake target) is required",
    path: ["target", "table"],
  });

export type SyncFormValues = z.infer<typeof syncSchema>;

// Matches core/api/v1alpha1.ParseAge: a Go duration (168h, 30m, ...) or a
// plain day count suffixed with "d" (7d, 1.5d) — Go duration strings have
// no "d" unit of their own.
const durationSchema = z
  .string()
  .regex(/^\d+(\.\d+)?(ns|us|µs|ms|s|m|h|d)$/, 'Use a duration like "7d", "168h", or "30m"')
  .optional();

const sourceColumnSchema = z.object({
  source: z.string().min(1, "Source column is required"),
  as: z.string().optional(),
});

const sourceRoutingSchema = z.object({
  type: z.literal("timeRange"),
  timestampColumn: z.string().min(1, "Timestamp column is required"),
  minAge: durationSchema,
  maxAge: durationSchema,
});

const sourceViewSchema = z.object({
  table: z.string().min(1, "Table is required"),
  columns: z.array(sourceColumnSchema).min(1, "At least one column is required"),
});

const projectionSourceSchema = z.object({
  connectionRef: z.string().min(1, "Connection is required"),
  routing: sourceRoutingSchema.optional(),
  view: sourceViewSchema,
});

export const projectionSchema = z
  .object({
    sources: z.array(projectionSourceSchema).min(1, "At least one source is required"),
  })
  .refine((v) => v.sources.length === 1 || v.sources.every((s) => !!s.routing), {
    message: "Every source needs routing when there's more than one source",
    path: ["sources"],
  })
  .refine((v) => v.sources.length !== 1 || !v.sources[0].routing, {
    message: "Routing must be unset when there's only one source",
    path: ["sources"],
  });

export type ProjectionFormValues = z.infer<typeof projectionSchema>;
