import React, { Dispatch, SetStateAction } from 'react';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Entity, EntityType, LineageDirection, SchemaFieldRef } from '../../types.generated';
import EntityRegistry from '../entityV2/EntityRegistry';
import { DBT_CLOUD_URN } from '../ingest/source/builder/constants';
import { ColumnQueryData } from '../sharedV2/EntitySidebarContext';
import { getEntityTypeFromEntityUrn, getPlatformUrnFromEntityUrn } from './lineageUtils';
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
    isExpanded: boolean;
    direction?: LineageDirection;
}

export interface LineageEntity extends NodeBase {
    urn: Urn;
    type: EntityType;
    entity?: FetchedEntityV2;
    rawEntity?: Entity; // TODO: Don't store this -- waste of memory? Currently used for manual lineage modal
    fetchStatus: Record<LineageDirection, FetchStatus>;
    filters?: Record<LineageDirection, Filters>;
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
    numShown?: number; // Includes nodes in contents shown due to a different path, not through `parent`
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

export function isUrnDbt(urn: string, entityRegistry: EntityRegistry): boolean {
    const type = getEntityTypeFromEntityUrn(urn, entityRegistry);
    return type === EntityType.Dataset && getPlatformUrnFromEntityUrn(urn) === DBT_CLOUD_URN;
}

export function isUrnTransformational(urn: string, entityRegistry: EntityRegistry): boolean {
    const type = getEntityTypeFromEntityUrn(urn, entityRegistry);
    return !!type && TRANSFORMATION_TYPES.includes(type);
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
    actor?: Entity;
}

export interface LineageEdge {
    isDisplayed: boolean;
    isManual: boolean;
    created?: AuditStamp;
    updated?: AuditStamp;
    via?: Urn;
}

export type EdgeId = string;

export function createEdgeId(upstream: Urn, downstream: Urn): EdgeId {
    return `${upstream}-:-${downstream}`;
}

export function parseEdgeId(edgeId: EdgeId): [Urn, Urn] {
    const [upstream, downstream] = edgeId.split('-:-', 2);
    return [upstream, downstream];
}

export function getEdgeId(parent: Urn, child: Urn, direction: LineageDirection) {
    const upstream = direction === LineageDirection.Downstream ? parent : child;
    const downstream = direction === LineageDirection.Downstream ? child : parent;
    return createEdgeId(upstream, downstream);
}

export function reverseDirection(direction: LineageDirection): LineageDirection {
    return direction === LineageDirection.Upstream ? LineageDirection.Downstream : LineageDirection.Upstream;
}

export type NeighborMap = Map<Urn, Set<Urn>>;

export interface NodeContext {
    rootUrn: string;
    nodes: Map<Urn, LineageEntity>;
    edges: Map<EdgeId, LineageEdge>;
    adjacencyList: Record<LineageDirection, NeighborMap>;
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
    adjacencyList: {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    },
    nodeVersion: 0,
    setNodeVersion: () => {},
    dataVersion: 0,
    setDataVersion: () => {},
    displayVersion: [0, []],
    setDisplayVersion: () => {},
});

export function getParents(node: LineageNode, adjacencyList: NodeContext['adjacencyList']): string[] {
    if (node.type === LINEAGE_FILTER_TYPE) return [node.parent];
    if (!node.direction) return [];
    return Array.from(adjacencyList[reverseDirection(node.direction)].get(node.id) || []);
}

export function addToAdjacencyList(
    adjacencyList: NodeContext['adjacencyList'],
    direction: LineageDirection,
    parent: Urn,
    child: Urn,
): void {
    setDefault(adjacencyList[direction], parent, new Set()).add(child);
    setDefault(adjacencyList[reverseDirection(direction)], child, new Set()).add(parent);
}

export function removeFromAdjacencyList(
    adjacencyList: NodeContext['adjacencyList'],
    direction: LineageDirection,
    parent: Urn,
    child: Urn,
): void {
    adjacencyList[direction].get(parent)?.delete(child);
    adjacencyList[reverseDirection(direction)].get(child)?.delete(parent);
}

export function clearEdges(urn: Urn, context: Pick<NodeContext, 'edges' | 'adjacencyList'>): void {
    const { edges, adjacencyList } = context;
    adjacencyList[LineageDirection.Upstream].get(urn)?.forEach((upstream) => edges.delete(createEdgeId(upstream, urn)));
    adjacencyList[LineageDirection.Downstream]
        .get(urn)
        ?.forEach((downstream) => edges.delete(createEdgeId(urn, downstream)));
    adjacencyList[LineageDirection.Upstream].delete(urn);
    adjacencyList[LineageDirection.Downstream].delete(urn);
}

export type FineGrainedLineageMap = Map<ColumnRef, ColumnRef[]>;
export type FineGrainedLineage = { forward: FineGrainedLineageMap; backward: FineGrainedLineageMap };
export type HighlightedColumns = Map<Urn, Set<string>>;

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
    numNodes: number;
    refetchUrn: (urn: string) => void;
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
    numNodes: 0,
    refetchUrn: () => {},
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
