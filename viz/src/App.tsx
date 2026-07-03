import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Check,
  Copy,
  Command as CommandIcon,
  Monitor,
  Moon,
  Radio,
  Sun,
} from "lucide-react";
import {
  emptyTrace,
  parseLiveLine,
  parseTrace,
  type ParsedTrace,
} from "@/lib/trace";
import { FoldEngine } from "@/lib/fold";
import { lensesFor } from "@/lib/lenses";
import type { ViewFilters } from "@/lib/lens-types";
import { useTheme, type ThemeMode } from "@/lib/useTheme";
import { Loader, type LoadedTrace } from "@/components/loader";
import { Scrubber, type FaultMarker } from "@/components/scrubber";
import { CommandPalette } from "@/components/command-palette";
import { Inspector } from "@/lenses/inspector";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  ResizableHandle,
  ResizablePanel,
  ResizablePanelGroup,
} from "@/components/ui/resizable";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { shortHost } from "@/lib/host";

interface Loaded {
  name: string;
  trace: ParsedTrace;
  engine: FoldEngine;
  live: boolean;
}

/** Live connection lifecycle surfaced in the header. */
type ConnState = "connecting" | "live" | "lost";

const TICK_MS = 90;

export default function App() {
  const [loaded, setLoaded] = useState<Loaded | null>(null);
  const [step, setStep] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const [activeLens, setActiveLens] = useState("topology");
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [hiddenWires, setHiddenWires] = useState<Set<string>>(new Set());
  const [hiddenNodes, setHiddenNodes] = useState<Set<string>>(new Set());
  const [theme, setTheme] = useTheme();
  const [copied, setCopied] = useState(false);
  // Live mode: connection status, a follow toggle that pins the scrubber to
  // the newest step, and a version counter bumped on every append so the
  // memoized world/lenses recompute as the in-place trace grows.
  const [connState, setConnState] = useState<ConnState | null>(null);
  const [follow, setFollow] = useState(true);
  const [version, setVersion] = useState(0);
  const acc = useRef(0);
  const esRef = useRef<EventSource | null>(null);
  const followRef = useRef(true);
  const engineRef = useRef<FoldEngine | null>(null);

  useEffect(() => {
    followRef.current = follow;
  }, [follow]);

  const onLoad = useCallback((t: LoadedTrace) => {
    esRef.current?.close();
    esRef.current = null;
    const trace = parseTrace(t.text);
    const engine = new FoldEngine(trace);
    engineRef.current = engine;
    setConnState(null);
    setLoaded({ name: t.name, trace, engine, live: false });
    setStep(0);
    setPlaying(false);
    setSelectedNode(null);
    setHiddenWires(new Set());
    setHiddenNodes(new Set());
    const lenses = lensesFor(trace);
    setActiveLens(lenses[0]?.id ?? "topology");
  }, []);

  const onConnectLive = useCallback((url: string) => {
    esRef.current?.close();
    const base = url.replace(/\/$/, "");
    const trace = emptyTrace();
    const engine = new FoldEngine(trace);
    engineRef.current = engine;
    setLoaded({ name: base, trace, engine, live: true });
    setStep(0);
    setPlaying(false);
    setSelectedNode(null);
    setHiddenWires(new Set());
    setHiddenNodes(new Set());
    setActiveLens("topology");
    setFollow(true);
    followRef.current = true;
    setConnState("connecting");

    const es = new EventSource(`${base}/events`);
    esRef.current = es;
    es.onopen = () => setConnState("live");
    es.onerror = () => setConnState("lost"); // EventSource auto-reconnects
    es.onmessage = (e) => {
      const eng = engineRef.current;
      if (!eng) return;
      const { event } = parseLiveLine(e.data, eng.trace.events.length);
      if (!event) return;
      eng.append([event]);
      setVersion((v) => v + 1);
      if (followRef.current) setStep(eng.trace.maxStep);
    };
  }, []);

  useEffect(() => () => esRef.current?.close(), []);

  const lenses = useMemo(
    () => (loaded ? lensesFor(loaded.trace) : []),
    // version: live streams grow the trace's protocol/wire roster in place.
    [loaded, version]
  );
  const maxStep = loaded?.trace.maxStep ?? 0;

  const world = useMemo(
    () => (loaded ? loaded.engine.worldAtStep(step) : null),
    [loaded, step, version]
  );

  const faults = useMemo<FaultMarker[]>(() => {
    if (!loaded) return [];
    const out: FaultMarker[] = [];
    for (const ev of loaded.trace.events) {
      if (ev.kind !== "fault") continue;
      const nodes = (ev.nodes ?? []).map(shortHost).join(" — ");
      const params = ev.params ? " " + JSON.stringify(ev.params) : "";
      out.push({
        step: ev.step,
        mutation: ev.mutation ?? "fault",
        label: `${ev.mutation} ${nodes}${params}`,
      });
    }
    return out;
  }, [loaded, version]);

  // userSeek is every human-initiated scrub (slider, keyboard, palette,
  // fault marker). In live mode a seek away from the newest step disengages
  // follow; seeking back to the end re-engages it.
  const userSeek = useCallback(
    (s: number) => {
      setStep(s);
      if (loaded?.live) {
        const atLive = s >= (engineRef.current?.trace.maxStep ?? 0);
        setFollow(atLive);
        followRef.current = atLive;
      }
    },
    [loaded]
  );

  const jumpToLive = useCallback(() => {
    const m = engineRef.current?.trace.maxStep ?? 0;
    setFollow(true);
    followRef.current = true;
    setStep(m);
  }, []);

  // Playback loop.
  useEffect(() => {
    if (!playing || !loaded) return;
    const id = setInterval(() => {
      acc.current += speed;
      const advance = Math.floor(acc.current);
      if (advance >= 1) {
        acc.current -= advance;
        setStep((s) => {
          const next = s + advance;
          if (next >= maxStep) {
            setPlaying(false);
            return maxStep;
          }
          return next;
        });
      }
    }, TICK_MS);
    return () => clearInterval(id);
  }, [playing, speed, maxStep, loaded]);

  // Keyboard controls.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setPaletteOpen((o) => !o);
        return;
      }
      if (!loaded) return;
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA") return;
      switch (e.key) {
        case "ArrowLeft":
          e.preventDefault();
          userSeek(Math.max(0, step - 1));
          break;
        case "ArrowRight":
          e.preventDefault();
          userSeek(Math.min(maxStep, step + 1));
          break;
        case "Home":
          e.preventDefault();
          userSeek(0);
          break;
        case "End":
          e.preventDefault();
          userSeek(maxStep);
          break;
        case " ":
          e.preventDefault();
          setPlaying((p) => !p);
          break;
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [loaded, maxStep, step, userSeek]);

  const filters: ViewFilters = useMemo(
    () => ({ hiddenWires, hiddenNodes }),
    [hiddenWires, hiddenNodes]
  );

  const toggleWire = (w: string) =>
    setHiddenWires((prev) => {
      const next = new Set(prev);
      next.has(w) ? next.delete(w) : next.add(w);
      return next;
    });
  const toggleNode = (h: string) =>
    setHiddenNodes((prev) => {
      const next = new Set(prev);
      next.has(h) ? next.delete(h) : next.add(h);
      return next;
    });

  const copySeed = () => {
    if (!loaded) return;
    navigator.clipboard
      ?.writeText(`prototest.WithSeed(${loaded.trace.meta.seed})`)
      .then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1200);
      })
      .catch(() => {});
  };

  const ActiveLens =
    lenses.find((l) => l.id === activeLens)?.Component ??
    lenses[0]?.Component ??
    null;

  return (
    <TooltipProvider delayDuration={200}>
      <div className="flex h-full flex-col">
        {/* header */}
        <header className="flex items-center gap-3 border-b px-4 py-2">
          <span className="font-semibold">protoviz</span>
          {loaded && (
            <span className="text-xs text-muted-foreground">{loaded.name}</span>
          )}
          {loaded?.live && connState && (
            <LiveBadge state={connState} following={follow} />
          )}
          <div className="ml-auto flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              className="gap-2 text-muted-foreground"
              onClick={() => setPaletteOpen(true)}
            >
              <CommandIcon className="h-3.5 w-3.5" /> ⌘K
            </Button>
            <ThemeToggle theme={theme} setTheme={setTheme} />
          </div>
        </header>

        {!loaded || !world ? (
          <div className="flex flex-1 items-center justify-center p-8">
            <div className="w-full max-w-md">
              <h1 className="mb-1 text-lg font-semibold">
                Open a protocol trace
              </h1>
              <p className="mb-4 text-sm text-muted-foreground">
                A protoviz/1 JSONL trace from prototest, or a live stream from a
                running cluster. Scrub through a run, watch the overlay form,
                click nodes to inspect state.
              </p>
              <Loader onLoad={onLoad} onConnectLive={onConnectLive} />
            </div>
          </div>
        ) : (
          <>
            {/* lens tabs */}
            <div className="flex items-center gap-3 border-b px-4 py-1.5">
              <Tabs value={activeLens} onValueChange={setActiveLens}>
                <TabsList>
                  {lenses.map((l) => (
                    <TabsTrigger key={l.id} value={l.id}>
                      {l.title}
                    </TabsTrigger>
                  ))}
                </TabsList>
              </Tabs>
            </div>

            {/* body */}
            <div className="flex min-h-0 flex-1">
              {/* left rail */}
              <aside className="w-72 shrink-0 overflow-y-auto border-r p-3">
                <RunSummary
                  loaded={loaded}
                  copied={copied}
                  onCopySeed={copySeed}
                />
                <div className="mt-4">
                  <Loader onLoad={onLoad} onConnectLive={onConnectLive} />
                </div>
              </aside>

              {/* lens + inspector */}
              <ResizablePanelGroup direction="horizontal" className="min-w-0 flex-1">
                <ResizablePanel defaultSize={72} minSize={40}>
                  <div className="h-full min-h-0">
                    {ActiveLens && world && (
                      <ActiveLens
                        trace={loaded.trace}
                        engine={loaded.engine}
                        world={world}
                        step={step}
                        filters={filters}
                        selectedNode={selectedNode}
                        onSelectNode={setSelectedNode}
                      />
                    )}
                  </div>
                </ResizablePanel>
                <ResizableHandle withHandle />
                <ResizablePanel defaultSize={28} minSize={18}>
                  <Inspector
                    trace={loaded.trace}
                    world={world}
                    selectedNode={selectedNode}
                    onSelectNode={setSelectedNode}
                  />
                </ResizablePanel>
              </ResizablePanelGroup>
            </div>

            {/* scrubber */}
            <Scrubber
              step={step}
              maxStep={maxStep}
              virtualTime={world.current[0]?.t ?? ""}
              playing={playing}
              speed={speed}
              faults={faults}
              onStep={userSeek}
              onTogglePlay={() => setPlaying((p) => !p)}
              onSpeed={setSpeed}
              live={loaded.live}
              following={follow}
              onJumpToLive={jumpToLive}
            />
          </>
        )}

        {loaded && (
          <CommandPalette
            open={paletteOpen}
            onOpenChange={setPaletteOpen}
            trace={loaded.trace}
            lenses={lenses}
            activeLens={activeLens}
            filters={filters}
            maxStep={maxStep}
            onJumpStep={userSeek}
            onSelectLens={setActiveLens}
            onToggleWire={toggleWire}
            onToggleNode={toggleNode}
          />
        )}
      </div>
    </TooltipProvider>
  );
}

function RunSummary({
  loaded,
  copied,
  onCopySeed,
}: {
  loaded: Loaded;
  copied: boolean;
  onCopySeed: () => void;
}) {
  const { trace } = loaded;
  return (
    <div className="space-y-2">
      <div className="grid grid-cols-3 gap-2 text-center">
        <Stat label="nodes" value={trace.meta.nodes.length} />
        <Stat label="steps" value={trace.maxStep} />
        <Stat label="events" value={trace.events.length} />
      </div>
      {/* Live runs have no reproduce seed, so no seed banner. */}
      {!loaded.live && (
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              onClick={onCopySeed}
              className="flex w-full items-center justify-between rounded-md border px-2 py-1.5 text-left font-mono text-xs hover:bg-accent"
            >
              <span>prototest.WithSeed({trace.meta.seed})</span>
              {copied ? (
                <Check className="h-3.5 w-3.5 text-emerald-500" />
              ) : (
                <Copy className="h-3.5 w-3.5 text-muted-foreground" />
              )}
            </button>
          </TooltipTrigger>
          <TooltipContent>Copy the reproduce seed</TooltipContent>
        </Tooltip>
      )}
      {trace.warnings.length > 0 && (
        <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-2 py-1 text-xs text-amber-700 dark:text-amber-400">
          {trace.warnings.length} parse warning
          {trace.warnings.length > 1 ? "s" : ""} (bad lines skipped)
        </div>
      )}
    </div>
  );
}

function LiveBadge({
  state,
  following,
}: {
  state: ConnState;
  following: boolean;
}) {
  const label =
    state === "live"
      ? following
        ? "Live"
        : "Live (paused)"
      : state === "connecting"
        ? "Connecting…"
        : "Reconnecting…";
  const tone =
    state === "live"
      ? following
        ? "border-emerald-500/50 text-emerald-600 dark:text-emerald-400"
        : "border-amber-500/50 text-amber-600 dark:text-amber-400"
      : "border-muted-foreground/40 text-muted-foreground";
  return (
    <Badge variant="outline" className={`gap-1.5 ${tone}`}>
      <Radio
        className={`h-3 w-3 ${state === "live" && following ? "animate-pulse" : ""}`}
      />
      {label}
    </Badge>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border p-2">
      <div className="text-sm font-semibold tabular-nums">{value}</div>
      <div className="text-[10px] uppercase text-muted-foreground">{label}</div>
    </div>
  );
}

function ThemeToggle({
  theme,
  setTheme,
}: {
  theme: ThemeMode;
  setTheme: (m: ThemeMode) => void;
}) {
  const next: Record<ThemeMode, ThemeMode> = {
    system: "light",
    light: "dark",
    dark: "system",
  };
  const Icon = theme === "dark" ? Moon : theme === "light" ? Sun : Monitor;
  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label="Toggle theme"
      onClick={() => setTheme(next[theme])}
    >
      <Icon className="h-4 w-4" />
    </Button>
  );
}
