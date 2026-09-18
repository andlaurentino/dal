"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Database, LayoutGrid, Search, Waypoints } from "lucide-react";
import { cn } from "@/lib/utils";

const items = [
  { href: "/connections", label: "Connections", icon: Database },
  { href: "/syncs", label: "Syncs", icon: Waypoints },
  { href: "/projections", label: "Projections", icon: LayoutGrid },
  { href: "/query", label: "Query", icon: Search },
];

export default function Nav() {
  const pathname = usePathname();

  return (
    <nav className="flex items-center gap-1">
      {items.map(({ href, label, icon: Icon }) => {
        const active = pathname === href || pathname.startsWith(`${href}/`);
        return (
          <Link
            key={href}
            href={href}
            className={cn(
              "flex items-center gap-2 rounded-lg px-3 py-1.5 text-sm font-medium transition-colors",
              active
                ? "bg-accent text-accent-foreground"
                : "text-muted-foreground hover:bg-muted hover:text-foreground",
            )}
          >
            <Icon className="size-4" />
            {label}
          </Link>
        );
      })}
    </nav>
  );
}
