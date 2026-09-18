"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { LayoutGrid } from "lucide-react";
import { cn } from "@/lib/utils";

export default function QuerySidebar({ names }: { names: string[] }) {
  const pathname = usePathname();

  return (
    <nav className="grid gap-1">
      {names.length === 0 && (
        <p className="px-3 py-1.5 text-sm text-muted-foreground">No projections yet.</p>
      )}
      {names.map((name) => {
        const href = `/query/${encodeURIComponent(name)}`;
        const active = pathname === href;
        return (
          <Link
            key={name}
            href={href}
            className={cn(
              "flex items-center gap-2 rounded-lg px-3 py-1.5 text-sm font-medium transition-colors",
              active
                ? "bg-accent text-accent-foreground"
                : "text-muted-foreground hover:bg-muted hover:text-foreground",
            )}
          >
            <LayoutGrid className="size-4" />
            {name}
          </Link>
        );
      })}
    </nav>
  );
}
