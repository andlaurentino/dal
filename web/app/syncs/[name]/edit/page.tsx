import { notFound } from "next/navigation";
import { getSync, listConnections } from "@/lib/controlplane";
import SyncForm from "@/components/SyncForm";

export default async function EditSyncPage({
  params,
}: {
  params: Promise<{ name: string }>;
}) {
  const { name } = await params;
  const [sync, connections] = await Promise.all([getSync(name), listConnections()]);
  if (!sync) notFound();

  return (
    <div className="grid gap-6">
      <h1 className="text-2xl font-semibold tracking-tight">Edit {sync.name}</h1>
      <SyncForm mode="edit" initial={sync} connectionNames={connections.map((c) => c.name)} />
    </div>
  );
}
