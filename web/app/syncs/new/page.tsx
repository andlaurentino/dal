import { listConnections } from "@/lib/controlplane";
import SyncEditor from "@/components/SyncEditor";

export default async function NewSyncPage() {
  const connections = await listConnections();

  return (
    <div className="grid gap-6">
      <h1 className="text-2xl font-semibold tracking-tight">New sync</h1>
      <SyncEditor mode="create" connectionNames={connections.map((c) => c.name)} />
    </div>
  );
}
