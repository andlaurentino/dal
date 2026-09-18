import ConnectionForm from "@/components/ConnectionForm";

export default function NewConnectionPage() {
  return (
    <div className="grid gap-6">
      <h1 className="text-2xl font-semibold tracking-tight">New connection</h1>
      <ConnectionForm mode="create" />
    </div>
  );
}
