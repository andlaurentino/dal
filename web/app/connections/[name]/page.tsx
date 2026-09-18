import Link from "next/link";
import { notFound } from "next/navigation";
import { Pencil } from "lucide-react";
import { getConnection } from "@/lib/controlplane";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import DeleteButton from "@/components/DeleteButton";
import RawSourceViewer from "@/components/RawSourceViewer";

function DefinitionRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="grid grid-cols-3 gap-4 py-2 text-sm">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="col-span-2 break-all">{value}</dd>
    </div>
  );
}

export default async function ConnectionDetailPage({
  params,
}: {
  params: Promise<{ name: string }>;
}) {
  const { name } = await params;
  const connection = await getConnection(name);
  if (!connection) notFound();

  const { spec } = connection;

  return (
    <div className="grid gap-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold tracking-tight">{connection.name}</h1>
          <Badge variant="secondary">{spec.type}</Badge>
        </div>
        <div className="flex gap-2">
          <Button
            variant="outline"
            render={
              <Link href={`/connections/${connection.name}/edit`}>
                <Pencil />
                Edit
              </Link>
            }
          />
          <DeleteButton kind="Connection" name={connection.name} />
        </div>
      </div>

      <Card>
        <CardContent>
          <dl className="divide-y divide-border">
            {spec.type === "kafka" && spec.kafka && (
              <>
                <DefinitionRow label="Brokers" value={spec.kafka.brokers.join(", ")} />
                {spec.kafka.saslMechanism && (
                  <DefinitionRow label="SASL mechanism" value={spec.kafka.saslMechanism} />
                )}
              </>
            )}
            {spec.type === "postgres" && spec.postgres && (
              <DefinitionRow label="DSN" value={spec.postgres.dsn} />
            )}
            {spec.type === "datalake" && spec.datalake && (
              <>
                <DefinitionRow label="Endpoint" value={spec.datalake.endpoint} />
                <DefinitionRow label="Bucket" value={spec.datalake.bucket} />
              </>
            )}
          </dl>
        </CardContent>
      </Card>

      <RawSourceViewer raw={connection.raw} />
    </div>
  );
}
