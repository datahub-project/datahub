import { Maybe } from 'graphql/jsutils/Maybe';
import React, { Dispatch, SetStateAction } from 'react';
import { EntityType, LineageDirection, SchemaFieldRef } from '../../types.generated';
import { GenericEntityProperties } from '../entityV2/shared/types';
import { DBT_CLOUD_URN } from '../ingest/source/builder/constants';
import { ColumnQueryData } from '../shared/EntitySidebarContext';
import { getPlatformUrnFromEntityUrn } from './lineageUtils';
import { FetchedEntityV2 } from './types';

export const TRANSITION_DURATION_MS = 200;
export const LINEAGE_FILTER_PAGINATION = 4;
type Urn = string;

/**
 * Used to determine when and what to query for extra data.
 */
export enum FetchStatus {
    UNNEEDED = 'UNNEEDED',
    UNFETCHED = 'UNFETCHED',
    LOADING = 'LOADING',
    COMPLETE = 'COMPLETE',
}

export interface Filters {
    display?: boolean; // undefined == display
    limit?: number; // undefined == no limit
    facetFilters: Map<string, Set<string>>;
}

export interface NodeBase {
    id: string;
    direction?: LineageDirection;
    parents: Set<Urn>;
    prunedParents?: Set<Urn>;
}

export interface LineageEntity extends NodeBase {
    urn: Urn;
    type: EntityType;
    entity?: FetchedEntityV2;
    fetchStatus: Record<LineageDirection, FetchStatus>;
    filters?: Record<LineageDirection, Filters>;
    backupEntity?: GenericEntityProperties; // TODO: Implement in a cleaner way
}

export const LINEAGE_FILTER_TYPE = 'lineage-filter';
export const LINEAGE_FILTER_ID_PREFIX = 'lf:';

export interface LineageFilter extends NodeBase {
    urn?: never;
    type: typeof LINEAGE_FILTER_TYPE;
    direction: LineageDirection;
    parent: Urn; // TODO: Consider removing in favor of parents
    contents: Urn[];
    shown: Set<Urn>;
    limit: number;
}

export type LineageNode = LineageEntity | LineageFilter;

const TRANSFORMATION_TYPES: string[] = [EntityType.Query, EntityType.DataJob];

export function isDbt(node: Pick<LineageNode, 'urn' | 'type'>): boolean {
    return node.type === EntityType.Dataset && !!node.urn && getPlatformUrnFromEntityUrn(node.urn) === DBT_CLOUD_URN;
}

export function isQuery(node: Pick<LineageNode, 'type'>): boolean {
    return node.type === EntityType.Query;
}

// TODO: Replace with value from search-across-lineage, once it's available
export function isTransformational(node: Pick<LineageNode, 'urn' | 'type'>): boolean {
    return TRANSFORMATION_TYPES.includes(node.type) || isDbt(node);
}

export type ColumnRef = string;

export function createColumnRef(urn: Urn, field: string): ColumnRef {
    const val = `${urn}::${field}`;
    try {
        return decodeURI(val);
    } catch (e) {
        return val;
    }
}

export function parseColumnRef(columnRef: ColumnRef): [Urn, string] {
    const [urn, field] = columnRef.split('::', 2);
    return [urn, field];
}

export const COLUMN_QUERY_ID_PREFIX = 'cq:';

export function createColumnQueryRef(
    queryUrn: Urn,
    upstreams: Maybe<SchemaFieldRef[]>,
    downstreams: Maybe<SchemaFieldRef[]>,
): ColumnRef {
    const upstreamsUrn = upstreams?.map((r) => `${r.urn}:${r.path}`).join('|');
    const downstreamsUrn = downstreams?.map((r) => `${r.urn}:${r.path}`).join('|');
    const base = COLUMN_QUERY_ID_PREFIX + [queryUrn, upstreamsUrn, downstreamsUrn].join('__');
    return createColumnRef(base, '');
}

export function parseColumnQueryRef(queryRef: ColumnRef): Urn {
    const [base] = queryRef.split('::', 2);
    const [queryUrn] = base.slice(COLUMN_QUERY_ID_PREFIX.length).split('__', 1);
    return queryUrn;
}

interface AuditStamp {
    timestamp: number;
    // TODO: Add actor
}

export interface LineageEdge {
    created?: AuditStamp;
    updated?: AuditStamp;
    isManual: boolean;
}

export interface NodeContext {
    rootUrn: string;
    nodes: Map<Urn, LineageEntity>;
    edges: Map<Urn, Map<Urn, LineageEdge>>; // Edges in direction root -> child
    nodeVersion: number;
    setNodeVersion: Dispatch<SetStateAction<number>>;
    dataVersion: number;
    setDataVersion: Dispatch<SetStateAction<number>>;
    displayVersion: [number, Urn[]];
    setDisplayVersion: Dispatch<SetStateAction<[number, Urn[]]>>;
}

export const LineageNodesContext = React.createContext<NodeContext>({
    rootUrn: '',
    nodes: new Map(),
    edges: new Map(),
    nodeVersion: 0,
    setNodeVersion: () => {},
    dataVersion: 0,
    setDataVersion: () => {},
    displayVersion: [0, []],
    setDisplayVersion: () => {},
});

export interface ColumnHighlight {
    fromSelect: boolean;
}

export type FineGrainedLineageMap = Map<ColumnRef, ColumnRef[]>;
export type FineGrainedLineage = { forward: FineGrainedLineageMap; backward: FineGrainedLineageMap };
export type HighlightedColumns = Map<Urn, Map<string, ColumnHighlight>>;

export type NeighborMap = Map<Urn, Set<Urn>>;
export type NeighborData = Record<LineageDirection, NeighborMap>;

interface DisplayContext {
    // Params
    hoveredNode: Urn | null;
    setHoveredNode: Dispatch<SetStateAction<Urn | null>>;
    hoveredColumn: ColumnRef | null;
    setHoveredColumn: Dispatch<SetStateAction<ColumnRef | null>>;
    selectedColumn: ColumnRef | null;
    setSelectedColumn: Dispatch<SetStateAction<ColumnRef | null>>;
    // Outputs
    highlightedNodes: Set<Urn>;
    highlightedColumns: HighlightedColumns;
    highlightedEdges: Set<string>;
    fineGrainedLineage: FineGrainedLineage;
    columnQueryData: Map<ColumnRef, ColumnQueryData>;
    neighborData: NeighborData;
    numNodes: number;
}

export const LineageDisplayContext = React.createContext<DisplayContext>({
    hoveredNode: null,
    setHoveredNode: () => {},
    hoveredColumn: null,
    setHoveredColumn: () => {},
    selectedColumn: null,
    setSelectedColumn: () => {},
    highlightedNodes: new Set(),
    highlightedColumns: new Map(),
    highlightedEdges: new Set(),
    fineGrainedLineage: {
        forward: new Map(),
        backward: new Map(),
    },
    columnQueryData: new Map(),
    neighborData: {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    },
    numNodes: 0,
});

export function setDefault<K, V>(map: Map<K, V>, key: K, defaultValue: V): V {
    if (!map.has(key)) {
        map.set(key, defaultValue);
    }
    return map.get(key) as V;
}

export function setDifference(setA: Set<string>, setB: Set<string>): string[] {
    return Array.from(setA).filter((x) => !setB.has(x));
}

export function onMouseDownCapturePreventSelect(event: React.MouseEvent): void {
    event.preventDefault(); // Prevents selecting node in React Flow
    event.stopPropagation(); // Prevents focusing node
}

const DATA_STORE_COLOR = '#ffae108f';
const BI_TOOL_COLOR = '#3932898f';
const DEFAULT_COLOR = '#ff10108f';

export function getNodeColor(type?: EntityType): [string, string] {
    if (type === EntityType.Chart || type === EntityType.Dashboard) {
        return [BI_TOOL_COLOR, 'Field'];
    }
    if (type === EntityType.Dataset) {
        return [DATA_STORE_COLOR, 'Column'];
    }
    return [DEFAULT_COLOR, 'Column'];
}
