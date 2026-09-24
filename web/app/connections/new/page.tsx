import ConnectionEditor from "@/components/ConnectionEditor";

export default function NewConnectionPage() {
  return (
    <div className="grid gap-6">
      <h1 className="text-2xl font-semibold tracking-tight">New connection</h1>
      <ConnectionEditor mode="create" />
    </div>
  );
}
