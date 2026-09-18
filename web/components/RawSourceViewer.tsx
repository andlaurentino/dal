"use client";

import { useState } from "react";
import { Check, ChevronDown, ChevronRight, Copy } from "lucide-react";
import { Button } from "@/components/ui/button";

function formatSource(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

export default function RawSourceViewer({ raw }: { raw: string }) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  const formatted = formatSource(raw);

  async function handleCopy() {
    await navigator.clipboard.writeText(formatted);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  }

  return (
    <div>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-1.5 text-sm font-medium text-muted-foreground hover:text-foreground"
      >
        {open ? <ChevronDown className="size-4" /> : <ChevronRight className="size-4" />}
        View source
      </button>
      {open && (
        <div className="relative mt-2">
          <pre className="max-h-96 overflow-auto rounded-xl bg-muted p-4 text-xs">
            {formatted}
          </pre>
          <Button
            type="button"
            variant="outline"
            size="icon-sm"
            className="absolute top-2 right-2"
            onClick={handleCopy}
          >
            {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
          </Button>
        </div>
      )}
    </div>
  );
}
