import { notFound } from "next/navigation";
import { getProjection, listConnections } from "@/lib/controlplane";
import ProjectionForm from "@/components/ProjectionForm";

export default async function EditProjectionPage({
  params,
}: {
  params: Promise<{ name: string }>;
}) {
  const { name } = await params;
  const [projection, connections] = await Promise.all([getProjection(name), listConnections()]);
  if (!projection) notFound();

  return (
    <div className="grid gap-6">
      <h1 className="text-2xl font-semibold tracking-tight">Edit {projection.name}</h1>
      <ProjectionForm
        mode="edit"
        initial={projection}
        connectionNames={connections.map((c) => c.name)}
      />
    </div>
  );
}
