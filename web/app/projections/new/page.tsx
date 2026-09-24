import { listConnections } from "@/lib/controlplane";
import ProjectionEditor from "@/components/ProjectionEditor";

export default async function NewProjectionPage() {
  const connections = await listConnections();

  return (
    <div className="grid gap-6">
      <h1 className="text-2xl font-semibold tracking-tight">New projection</h1>
      <ProjectionEditor mode="create" connectionNames={connections.map((c) => c.name)} />
    </div>
  );
}
