import { notFound } from "next/navigation";
import { getProjection, listSyncs } from "@/lib/controlplane";
import ProjectionForm from "@/components/ProjectionForm";

export default async function EditProjectionPage({
  params,
}: {
  params: Promise<{ name: string }>;
}) {
  const { name } = await params;
  const [projection, syncs] = await Promise.all([getProjection(name), listSyncs()]);
  if (!projection) notFound();

  return (
    <div className="grid gap-6">
      <h1 className="text-2xl font-semibold tracking-tight">Edit {projection.name}</h1>
      <ProjectionForm mode="edit" initial={projection} syncNames={syncs.map((s) => s.name)} />
    </div>
  );
}
