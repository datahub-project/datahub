import { GenericEntityProperties } from '@app/entity/shared/types';
import { LINEAGE_COLORS } from '@app/entityV2/shared/constants';
import { useAppConfig } from '@app/useAppConfig';
import { Maybe } from 'graphql/jsutils/Maybe';
import React, { Dispatch, SetStateAction } from 'react';
import { Entity, EntityType, LineageDirection, SchemaFieldRef } from '../../types.generated';
import EntityRegistry from '../entityV2/EntityRegistry';
import { DBT_CLOUD_URN } from '../ingest/source/builder/constants';
import { hashString } from '../shared/avatar/getAvatarColor';
import { FineGrainedOperation } from '../sharedV2/EntitySidebarContext';
import { getEntityTypeFromEntityUrn, getPlatformUrnFromEntityUrn } from './lineageUtils';
import { FetchedEntityV2 } from './types';

export const TRANSITION_DURATION_MS = 200;
export const LINEAGE_FILTER_PAGINATION = 4;

export const HOVER_COLOR = LINEAGE_COLORS.BLUE_2;
export const SELECT_COLOR = LINEAGE_COLORS.PURPLE_3;

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
    searchUrns?: Set<string>;
}

export interface NodeBase {
    id: string;
    isExpanded: Record<LineageDirection, boolean>;
    direction?: LineageDirection; // Root node has no direction. One day can try to support cycles in the same way.
    dragged?: boolean;
    inCycle?: boolean;
}

export interface LineageEntity extends NodeBase {
    urn: Urn;
    type: EntityType;
    entity?: FetchedEntityV2;
    rawEntity?: Entity; // TODO: Don't store this -- waste of memory? Currently used for manual lineage modal
    fetchStatus: Record<LineageDirection, FetchStatus>;
    filters: Record<LineageDirection, Filters>;
}

export const LINEAGE_FILTER_TYPE = 'lineage-filter';
export const LINEAGE_FILTER_ID_PREFIX = 'lf:';

export function createLineageFilterNodeId(urn: Urn, direction: LineageDirection): string {
    const dir = direction === LineageDirection.Upstream ? 'u:' : 'd:';
    return `${LINEAGE_FILTER_ID_PREFIX}${dir}${urn}`;
}

export interface LineageFilter extends NodeBase {
    urn?: never;
    type: typeof LINEAGE_FILTER_TYPE;
    direction: LineageDirection;
    parent: Urn; // TODO: Consider removing in favor of parents
    contents: Urn[]; // Paginated nodes in order. Includes non-transformational nodes and transformational leaves
    allChildren: Set<Urn>; // Includes all transformational children
    shown: Set<Urn>;
    limit: number;
    numShown?: number; // Includes nodes in contents shown due to a different path, not through `parent`
}

export type LineageNode = LineageEntity | LineageFilter;

const TRANSFORMATION_TYPES: string[] = [EntityType.Query, EntityType.DataJob, EntityType.DataProcessInstance];

export function useIgnoreSchemaFieldStatus(): boolean {
    return useAppConfig().config.featureFlags.schemaFieldLineageIgnoreStatus;
}

export function isGhostEntity(
    node: FetchedEntityV2 | GenericEntityProperties | undefined | null,
    ignoreSchemaFieldStatus: boolean,
): boolean {
    return (
        !!node &&
        (!node?.exists || !!node.status?.removed) &&
        node.type !== EntityType.Query &&
        !(
            ignoreSchemaFieldStatus &&
            node.type === EntityType.SchemaField &&
            !isGhostEntity(node.parent, ignoreSchemaFieldStatus)
        )
    );
}

export function isDbt(node: Pick<LineageNode, 'urn' | 'type'>): boolean {
    return (
        (node.type === EntityType.Dataset || node.type === EntityType.SchemaField) &&
        !!node.urn &&
        getPlatformUrnFromEntityUrn(node.urn) === DBT_CLOUD_URN
    );
}

export function isQuery(node: Pick<LineageNode, 'type'>): boolean {
    return node.type === EntityType.Query;
}

// TODO: Replace with value from search-across-lineage, once it's available
// Must be kept in sync with useSearchAcrossLineage
export function isTransformational(node: Pick<LineageNode, 'urn' | 'type'>): boolean {
    return TRANSFORMATION_TYPES.includes(node.type) || isDbt(node);
}

export function isUrnDbt(urn: string, entityRegistry: EntityRegistry): boolean {
    const type = getEntityTypeFromEntityUrn(urn, entityRegistry);
    return (
        (type === EntityType.Dataset || type === EntityType.SchemaField) &&
        getPlatformUrnFromEntityUrn(urn) === DBT_CLOUD_URN
    );
}

export function isUrnQuery(urn: string, entityRegistry: EntityRegistry): boolean {
    const type = getEntityTypeFromEntityUrn(urn, entityRegistry);
    return type === EntityType.Query;
}

export function isUrnDataProcessInstance(urn: string, entityRegistry: EntityRegistry): boolean {
    const type = getEntityTypeFromEntityUrn(urn, entityRegistry);
    return type === EntityType.DataProcessInstance;
}

export function isUrnTransformational(urn: string, entityRegistry: EntityRegistry): boolean {
    const type = getEntityTypeFromEntityUrn(urn, entityRegistry);
    return (!!type && TRANSFORMATION_TYPES.includes(type)) || isUrnDbt(urn, entityRegistry);
}

export type ColumnRef = string;
export type FineGrainedOperationRef = string;

export function createColumnRef(urn: Urn, field: string): ColumnRef {
    return `${urn}::${field}`;
}

export function parseColumnRef(columnRef: ColumnRef): [Urn, string] {
    const [urn, field] = columnRef.split('::', 2);
    return [urn, field];
}

export function createFineGrainedOperationRef(
    queryUrn: Urn,
    upstreams: Maybe<SchemaFieldRef[]>,
    downstreams: Maybe<SchemaFieldRef[]>,
): FineGrainedOperationRef {
    const upstreamsUrn = upstreams?.map((r) => `${r.urn}:${r.path}`).join('␞');
    const downstreamsUrn = downstreams?.map((r) => `${r.urn}:${r.path}`).join('␞');
    return createColumnRef(queryUrn, hashString([upstreamsUrn, downstreamsUrn].join('::')).toString());
}

export interface LineageAuditStamp {
    timestamp: number;
    actor?: Entity;
}

export interface LineageEdge {
    isDisplayed: boolean;
    isManual?: boolean;
    created?: LineageAuditStamp;
    updated?: LineageAuditStamp;
    via?: Urn;
}

export interface LineageTableEdgeData extends LineageEdge {
    originalId: string; // For edges to via nodes, stores table->table edge id. Otherwise, identical to edge id.
}

export type EdgeId = string;

export function createEdgeId(upstream: Urn, downstream: Urn): EdgeId {
    return `${upstream}-:-${downstream}`;
}

export function parseEdgeId(edgeId: EdgeId): [Urn, Urn] {
    const [upstream, downstream] = edgeId.split('-:-', 2);
    return [upstream, downstream];
}

export function getEdgeId(parent: Urn, child: Urn, direction: LineageDirection | string) {
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
    rootType: EntityType;
    nodes: Map<Urn, LineageEntity>;
    edges: Map<EdgeId, LineageEdge>;
    adjacencyList: Record<LineageDirection, NeighborMap>;
    nodeVersion: number;
    setNodeVersion: Dispatch<SetStateAction<number>>;
    dataVersion: number;
    setDataVersion: Dispatch<SetStateAction<number>>;
    displayVersion: [number, Urn[]];
    setDisplayVersion: Dispatch<SetStateAction<[number, Urn[]]>>;
    columnEdgeVersion: number; // Used to force recalculation of column->column edges
    setColumnEdgeVersion: Dispatch<SetStateAction<number>>;
    hideTransformations: boolean;
    setHideTransformations: (hide: boolean) => void;
    showDataProcessInstances: boolean;
    setShowDataProcessInstances: (hide: boolean) => void;
    showGhostEntities: boolean;
    setShowGhostEntities: (hide: boolean) => void;
}

export const LineageNodesContext = React.createContext<NodeContext>({
    rootUrn: '',
    rootType: EntityType.Dataset,
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
    columnEdgeVersion: 0,
    setColumnEdgeVersion: () => {},
    hideTransformations: false,
    setHideTransformations: () => {},
    showDataProcessInstances: false,
    setShowDataProcessInstances: () => {},
    showGhostEntities: false,
    setShowGhostEntities: () => {},
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

// Mapping fromRef -> toRef -> operationRef represents a column-level edge (fromRef -> toRef)
// with an operationRef attached if this is an edge to that operation's query node
export type FineGrainedLineageMap = Map<ColumnRef, Map<ColumnRef, FineGrainedOperationRef | null>>;
export type FineGrainedLineage = { downstream: FineGrainedLineageMap; upstream: FineGrainedLineageMap };
export type HighlightedColumns = Map<Urn, Set<string>>;

interface DisplayContext {
    // Params
    hoveredNode: Urn | null;
    setHoveredNode: Dispatch<SetStateAction<Urn | null>>;
    displayedMenuNode: Urn | null;
    setDisplayedMenuNode: Dispatch<SetStateAction<Urn | null>>;
    hoveredColumn: ColumnRef | null;
    setHoveredColumn: Dispatch<SetStateAction<ColumnRef | null>>;
    selectedColumn: ColumnRef | null;
    setSelectedColumn: Dispatch<SetStateAction<ColumnRef | null>>;
    // Outputs
    highlightedNodes: Set<Urn>; // TODO: Remove? Not currently used
    cllHighlightedNodes: Map<Urn, Set<FineGrainedOperationRef> | null>;
    highlightedColumns: HighlightedColumns;
    highlightedEdges: Set<string>;
    fineGrainedLineage: FineGrainedLineage;
    fineGrainedOperations: Map<FineGrainedOperationRef, FineGrainedOperation>;
    shownUrns: string[];
    refetchUrn: (urn: string) => void;
}

export const LineageDisplayContext = React.createContext<DisplayContext>({
    hoveredNode: null,
    setHoveredNode: () => {},
    displayedMenuNode: null,
    setDisplayedMenuNode: () => {},
    hoveredColumn: null,
    setHoveredColumn: () => {},
    selectedColumn: null,
    setSelectedColumn: () => {},
    highlightedNodes: new Set(),
    cllHighlightedNodes: new Map(),
    highlightedColumns: new Map(),
    highlightedEdges: new Set(),
    fineGrainedLineage: {
        downstream: new Map(),
        upstream: new Map(),
    },
    fineGrainedOperations: new Map(),
    shownUrns: [],
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

export function onClickPreventSelect(event: React.MouseEvent): true {
    event.preventDefault(); // Prevents selecting node in React Flow
    event.stopPropagation(); // Prevents focusing node
    return true;
}

const DATA_STORE_COLOR = '#ffd279';
const BI_TOOL_COLOR = '#8682a2';
const ML_COLOR = '#206de8';
const DEFAULT_COLOR = '#ff7979';

export function getNodeColor(type?: EntityType): [string, string] {
    if (type === EntityType.Chart || type === EntityType.Dashboard) {
        return [BI_TOOL_COLOR, 'Field'];
    }
    if (type === EntityType.Dataset) {
        return [DATA_STORE_COLOR, 'Column'];
    }
    if (
        type === EntityType.Mlmodel ||
        type === EntityType.MlmodelGroup ||
        type === EntityType.Mlfeature ||
        type === EntityType.MlfeatureTable ||
        type === EntityType.MlprimaryKey
    ) {
        return [ML_COLOR, ''];
    }
    return [DEFAULT_COLOR, ''];
}
