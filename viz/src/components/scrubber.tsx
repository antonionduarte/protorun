// scrubber.tsx — the bottom transport. A Slider bound to the step range, play/
// pause with a speed select (steps-per-tick), and a fault ribbon: colored
// markers on the track where fault events occur, each with a Tooltip carrying
// the fault description. Current step + virtual time are shown alongside.
// Keyboard single-stepping (arrows / Home / End) is wired at the App level.

import {
  Pause,
  Play,
  SkipBack,
  SkipForward,
} from "lucide-react";
import { Slider } from "@/components/ui/slider";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

export interface FaultMarker {
  step: number;
  mutation: string;
  label: string;
}

export interface ScrubberProps {
  step: number;
  maxStep: number;
  virtualTime: string;
  playing: boolean;
  speed: number;
  faults: FaultMarker[];
  onStep: (s: number) => void;
  onTogglePlay: () => void;
  onSpeed: (s: number) => void;
}

const SPEEDS = [0.5, 1, 4, 16];

function faultColor(mutation: string): string {
  switch (mutation) {
    case "heal":
      return "#16a34a";
    case "cut":
    case "isolate":
      return "hsl(var(--destructive))";
    case "loss":
    case "delay":
      return "#d97706";
    default:
      return "hsl(var(--muted-foreground))";
  }
}

export function Scrubber({
  step,
  maxStep,
  virtualTime,
  playing,
  speed,
  faults,
  onStep,
  onTogglePlay,
  onSpeed,
}: ScrubberProps) {
  return (
    <div className="flex items-center gap-3 border-t bg-background px-4 py-2">
      <Button
        variant="ghost"
        size="icon"
        aria-label="Home"
        onClick={() => onStep(0)}
      >
        <SkipBack />
      </Button>
      <Button
        variant="outline"
        size="icon"
        aria-label={playing ? "Pause" : "Play"}
        onClick={onTogglePlay}
      >
        {playing ? <Pause /> : <Play />}
      </Button>
      <Button
        variant="ghost"
        size="icon"
        aria-label="End"
        onClick={() => onStep(maxStep)}
      >
        <SkipForward />
      </Button>

      <Select value={String(speed)} onValueChange={(v) => onSpeed(Number(v))}>
        <SelectTrigger className="h-8 w-20">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {SPEEDS.map((s) => (
            <SelectItem key={s} value={String(s)}>
              {s}×
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <div className="relative flex-1">
        {/* fault ribbon */}
        <div className="pointer-events-none absolute -top-1 left-0 h-2 w-full">
          {faults.map((f, i) => (
            <Tooltip key={i}>
              <TooltipTrigger asChild>
                <span
                  className="pointer-events-auto absolute top-0 h-2 w-1 -translate-x-1/2 cursor-help rounded-sm"
                  style={{
                    left: `${maxStep ? (f.step / maxStep) * 100 : 0}%`,
                    backgroundColor: faultColor(f.mutation),
                  }}
                  onClick={() => onStep(f.step)}
                />
              </TooltipTrigger>
              <TooltipContent>
                <span className="font-mono text-xs">
                  step {f.step}: {f.label}
                </span>
              </TooltipContent>
            </Tooltip>
          ))}
        </div>

        <Slider
          value={[step]}
          min={0}
          max={Math.max(1, maxStep)}
          step={1}
          onValueChange={(v) => onStep(v[0])}
        />
      </div>

      <div
        className={cn(
          "w-40 shrink-0 text-right font-mono text-xs tabular-nums text-muted-foreground"
        )}
      >
        step {step}/{maxStep}
        <span className="ml-2 text-foreground">{virtualTime}</span>
      </div>
    </div>
  );
}
