// command-palette.tsx — the ⌘K palette: jump-to-step (type a number),
// switch lens, and toggle wire-type / node filters. A checkmark-free toggle:
// hidden classes are shown struck-through and dimmed so the palette doubles as
// the filter legend.

import { useState } from "react";
import { Check, EyeOff } from "lucide-react";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import type { ParsedTrace } from "@/lib/trace";
import type { ViewFilters } from "@/lib/lens-types";
import type { Lens } from "@/lib/lens-types";
import { shortHost } from "@/lib/host";
import { cn } from "@/lib/utils";

export interface CommandPaletteProps {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  trace: ParsedTrace;
  lenses: Lens[];
  activeLens: string;
  filters: ViewFilters;
  maxStep: number;
  onJumpStep: (s: number) => void;
  onSelectLens: (id: string) => void;
  onToggleWire: (wire: string) => void;
  onToggleNode: (host: string) => void;
}

export function CommandPalette(props: CommandPaletteProps) {
  const {
    open,
    onOpenChange,
    trace,
    lenses,
    filters,
    maxStep,
    onJumpStep,
    onSelectLens,
    onToggleWire,
    onToggleNode,
  } = props;
  const [query, setQuery] = useState("");

  const asStep = Number(query.trim());
  const stepValid =
    query.trim() !== "" && Number.isInteger(asStep) && asStep >= 0;

  const close = () => {
    onOpenChange(false);
    setQuery("");
  };

  return (
    <CommandDialog open={open} onOpenChange={onOpenChange}>
      <CommandInput
        placeholder="Jump to step number, or filter lenses / wires / nodes…"
        value={query}
        onValueChange={setQuery}
      />
      <CommandList>
        <CommandEmpty>No matches.</CommandEmpty>

        {stepValid && (
          <CommandGroup heading="Jump">
            <CommandItem
              value={`go to step ${asStep}`}
              onSelect={() => {
                onJumpStep(Math.min(asStep, maxStep));
                close();
              }}
            >
              Go to step {Math.min(asStep, maxStep)}
            </CommandItem>
          </CommandGroup>
        )}

        <CommandGroup heading="Lenses">
          {lenses.map((l) => (
            <CommandItem
              key={l.id}
              value={`lens ${l.title}`}
              onSelect={() => {
                onSelectLens(l.id);
                close();
              }}
            >
              {l.title}
            </CommandItem>
          ))}
        </CommandGroup>

        <CommandGroup heading="Wire types (toggle visibility)">
          {trace.wireTypes.map((w) => {
            const hidden = filters.hiddenWires.has(w);
            return (
              <CommandItem
                key={w}
                value={`wire ${w}`}
                onSelect={() => onToggleWire(w)}
              >
                {hidden ? <EyeOff className="opacity-60" /> : <Check />}
                <span className={cn("font-mono", hidden && "line-through opacity-60")}>
                  {w}
                </span>
              </CommandItem>
            );
          })}
        </CommandGroup>

        <CommandGroup heading="Nodes (toggle visibility)">
          {trace.meta.nodes.map((h) => {
            const hidden = filters.hiddenNodes.has(h);
            return (
              <CommandItem
                key={h}
                value={`node ${h}`}
                onSelect={() => onToggleNode(h)}
              >
                {hidden ? <EyeOff className="opacity-60" /> : <Check />}
                <span className={cn("font-mono", hidden && "line-through opacity-60")}>
                  {shortHost(h)}
                </span>
              </CommandItem>
            );
          })}
        </CommandGroup>
      </CommandList>
    </CommandDialog>
  );
}
