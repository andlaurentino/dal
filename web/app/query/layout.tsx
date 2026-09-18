import { listProjections } from "@/lib/controlplane";
import QuerySidebar from "@/components/QuerySidebar";

export default async function QueryLayout({ children }: { children: React.ReactNode }) {
  const projections = await listProjections();

  return (
    <div className="grid grid-cols-[220px_1fr] gap-8">
      <aside className="grid gap-4">
        <h2 className="px-3 text-xs font-semibold tracking-wide text-muted-foreground uppercase">
          Projections
        </h2>
        <QuerySidebar names={projections.map((p) => p.name)} />
      </aside>
      <div>{children}</div>
    </div>
  );
}
