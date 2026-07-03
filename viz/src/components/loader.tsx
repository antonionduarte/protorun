// loader.tsx — the left-hand trace loader: drag-and-drop a .jsonl anywhere,
// or pick one of the four bundled sample traces (fetched from publicDir at
// /<name>.jsonl). After a load the parent shows the run summary + seed badge.

import { useCallback, useRef, useState } from "react";
import { Upload } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export interface SampleTrace {
  file: string;
  title: string;
  description: string;
}

export const SAMPLES: SampleTrace[] = [
  {
    file: "raft-partition.jsonl",
    title: "Raft — partition",
    description: "5 nodes; a leader survives a network cut, term/commit advance.",
  },
  {
    file: "hyparview-churn.jsonl",
    title: "HyParView — churn",
    description: "12 nodes; active/passive views reshape under isolate + loss.",
  },
  {
    file: "broadcast.jsonl",
    title: "Plumtree broadcast",
    description: "8 nodes; eager/lazy tree floods a gossip over HyParView.",
  },
  {
    file: "paxos-duel.jsonl",
    title: "Paxos — duel",
    description: "5 nodes; two proposers duel for a single decree.",
  },
];

export interface LoadedTrace {
  name: string;
  text: string;
}

export function Loader({
  onLoad,
  busy,
}: {
  onLoad: (t: LoadedTrace) => void;
  busy?: boolean;
}) {
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const readFile = useCallback(
    (file: File) => {
      const reader = new FileReader();
      reader.onload = () =>
        onLoad({ name: file.name, text: String(reader.result ?? "") });
      reader.readAsText(file);
    },
    [onLoad]
  );

  const loadSample = useCallback(
    async (s: SampleTrace) => {
      const res = await fetch(`/${s.file}`);
      const text = await res.text();
      onLoad({ name: s.file, text });
    },
    [onLoad]
  );

  return (
    <div className="space-y-4">
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setDragging(false);
          const file = e.dataTransfer.files?.[0];
          if (file) readFile(file);
        }}
        className={cn(
          "flex flex-col items-center justify-center gap-2 rounded-lg border border-dashed p-6 text-center transition-colors",
          dragging ? "border-primary bg-accent" : "border-border"
        )}
      >
        <Upload className="h-5 w-5 text-muted-foreground" />
        <p className="text-sm text-muted-foreground">
          Drag a <code>.jsonl</code> trace here
        </p>
        <Button
          variant="outline"
          size="sm"
          disabled={busy}
          onClick={() => inputRef.current?.click()}
        >
          Choose file
        </Button>
        <input
          ref={inputRef}
          type="file"
          accept=".jsonl,.json,.txt"
          className="hidden"
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) readFile(file);
          }}
        />
      </div>

      <div>
        <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Sample traces
        </h3>
        <div className="space-y-2">
          {SAMPLES.map((s) => (
            <Card
              key={s.file}
              role="button"
              tabIndex={0}
              onClick={() => loadSample(s)}
              onKeyDown={(e) => e.key === "Enter" && loadSample(s)}
              className="cursor-pointer transition-colors hover:bg-accent"
            >
              <CardContent className="p-3">
                <div className="text-sm font-medium">{s.title}</div>
                <div className="text-xs text-muted-foreground">
                  {s.description}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    </div>
  );
}
