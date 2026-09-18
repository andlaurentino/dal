import { Search } from "lucide-react";

export default function QueryIndexPage() {
  return (
    <div className="flex h-64 flex-col items-center justify-center gap-2 rounded-xl border border-dashed border-border text-muted-foreground">
      <Search className="size-6" />
      <p className="text-sm">Select a projection to query its data.</p>
    </div>
  );
}
