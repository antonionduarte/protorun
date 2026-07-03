import type { ComponentType } from "react";
import type { Host, ParsedTrace } from "./trace";
import type { FoldEngine, WorldState } from "./fold";

/** View filters driven from the ⌘K palette and legend chips. */
export interface ViewFilters {
  /** Wire types to HIDE (empty = show all). */
  hiddenWires: Set<string>;
  /** Nodes to HIDE (empty = show all). */
  hiddenNodes: Set<Host>;
}

/** Props every lens component receives. */
export interface LensProps {
  trace: ParsedTrace;
  engine: FoldEngine;
  world: WorldState;
  step: number;
  filters: ViewFilters;
  selectedNode: Host | null;
  onSelectNode: (host: Host | null) => void;
}

/** A registered lens: universal or protocol-specific. */
export interface Lens {
  id: string;
  title: string;
  /** Decide whether this lens applies to a given trace. */
  canRender: (trace: ParsedTrace) => boolean;
  Component: ComponentType<LensProps>;
}
