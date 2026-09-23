"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";

// No websocket/SSE infrastructure exists anywhere in this app yet (every
// other page is fetch-on-navigation), so live worker status starts with
// the simplest thing that works: periodically re-run the server component
// tree so its `cache: "no-store"` fetches pick up fresh data. Revisit with
// a websocket/SSE push only if this polling interval proves too coarse.
export default function AutoRefresh({ intervalMs = 5000 }: { intervalMs?: number }) {
  const router = useRouter();

  useEffect(() => {
    const id = setInterval(() => router.refresh(), intervalMs);
    return () => clearInterval(id);
  }, [router, intervalMs]);

  return null;
}
