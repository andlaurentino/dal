import { notFound } from "next/navigation";
import { getConnection } from "@/lib/controlplane";
import ConnectionEditor from "@/components/ConnectionEditor";

export default async function EditConnectionPage({
  params,
}: {
  params: Promise<{ name: string }>;
}) {
  const { name } = await params;
  const connection = await getConnection(name);
  if (!connection) notFound();

  return (
    <div className="grid gap-6">
      <h1 className="text-2xl font-semibold tracking-tight">Edit {connection.name}</h1>
      <ConnectionEditor mode="edit" initial={connection} />
    </div>
  );
}
