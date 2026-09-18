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

export const syncSchema = z
  .object({
    source: z.object({
      connectionRef: z.string().min(1, "Source connection is required"),
      topic: z.string().min(1, "Topic is required"),
    }),
    target: z.object({
      connectionRef: z.string().min(1, "Target connection is required"),
      table: z.string().min(1, "Table is required"),
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
  });

export type SyncFormValues = z.infer<typeof syncSchema>;

const projectionSourceSchema = z.object({
  syncRef: z.string().min(1, "Sync is required"),
  maxStalenessSeconds: z.coerce.number().int().nonnegative().optional(),
});

export const projectionSchema = z.object({
  sources: z.array(projectionSourceSchema).min(1, "At least one source is required"),
  queryable: z.object({
    table: z.string().min(1, "Table is required"),
    defaultLimit: z.coerce.number().int().positive().optional(),
  }),
});

export type ProjectionFormValues = z.infer<typeof projectionSchema>;
