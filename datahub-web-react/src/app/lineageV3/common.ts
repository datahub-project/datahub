import { Maybe } from 'graphql/jsutils/Maybe';
import React, { Dispatch, SetStateAction } from 'react';

import { hashString } from '@components/components/Avatar/utils';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { getPlatformUrnFromEntityUrn } from '@app/entityV2/shared/utils';
import globalEntityRegistryV2 from '@app/globalEntityRegistryV2';
import { DBT_CLOUD_URN } from '@app/ingest/source/builder/constants';
import { DBT_URN } from '@app/ingestV2/source/builder/constants';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { getEntityTypeFromEntityUrn } from '@app/lineageV3/utils/lineageUtils';
import { FineGrainedOperation } from '@app/sharedV2/EntitySidebarContext';
import { useAppConfig } from '@app/useAppConfig';

import { Entity, EntityType, LineageDirection, SchemaFieldRef } from '@types';

export const TRANSITION_DURATION_MS = 250;
export const LINEAGE_FILTER_PAGINATION = 4;

// Page size for fetching/displaying a data product's members, and the initial home member limit.
export const DATA_PRODUCT_MEMBER_PAGE_SIZE = 2;

export const LINEAGE_NODE_WIDTH = 320; // Fixed width
export const LINEAGE_NODE_HEIGHT = 90; // Maximum height
export const LINEAGE_HANDLE_OFFSET = 26; // Offset from top of horizontal handles

export const VERTICAL_HANDLE = 'vertical';

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

interface NodeBase {
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
    parentDataJob?: Urn;
    /** Data products containing this entity, with whether it is an output port of each. Undefined
     * means membership is not yet known; fetched for the data product graph by `useBulkDataProductMemberships`.
     * Not fetched as part of `entity` because data product lookup requires querying the graph index. */
    dataProducts?: { urn: Urn; isOutputPort: boolean }[];
    /** For a data product rendered as a bounding box: how many of its members to fetch and display,
     * raised a page at a time by the box header's "Show more" control. Currently set on the home
     * product only; other boxes show all their connected members. */
    boundingBoxLimit?: number;
}

export const LINEAGE_FILTER_TYPE = 'lineage-filter';
const LINEAGE_FILTER_ID_PREFIX = 'lf:';

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

export interface LineageBoundingBox {
    urn: Urn;
    type: EntityType;
    entity?: FetchedEntityV2;
    dragged?: boolean;
    colorHex?: string;
    /** Number of this data product's members currently shown inside the box. Data-product graphs
     * only; undefined for data-flow bounding boxes, which have no member counter. */
    memberCount?: number;
}

export interface LineageAnnotationNode {
    urn?: never;
    label: string;
    dragged?: boolean;
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

// If the home node has one of these types, render data jobs as non-transformational
const TRANSFORMATIONAL_OVERRIDE_ROOT_TYPES = new Set([EntityType.DataFlow, EntityType.DataJob]);

export function generateIgnoreAsHops(homeType: EntityType) {
    const base = [
        {
            entityType: EntityType.Dataset,
            platforms: [DBT_URN],
        },
        {
            entityType: EntityType.SchemaField,
            platforms: [DBT_URN],
        },
        { entityType: EntityType.DataProcessInstance },
    ];

    if (TRANSFORMATIONAL_OVERRIDE_ROOT_TYPES.has(homeType)) return base;
    return [...base, { entityType: EntityType.DataJob }];
}

// TODO: Replace with value from search-across-lineage, once it's available
// Must be kept in sync with generateIgnoreAsHops
export function isTransformational(node: Pick<LineageNode, 'urn' | 'type'>, rootType: EntityType): boolean {
    if (TRANSFORMATIONAL_OVERRIDE_ROOT_TYPES.has(rootType) && node.type === EntityType.DataJob) {
        return false;
    }
    return TRANSFORMATION_TYPES.includes(node.type) || isDbt(node);
}

export function isUrnQuery(urn: string): boolean {
    const type = getEntityTypeFromEntityUrn(urn, globalEntityRegistryV2);
    return type === EntityType.Query;
}

export function isUrnDataProcessInstance(urn: string): boolean {
    const type = getEntityTypeFromEntityUrn(urn, globalEntityRegistryV2);
    return type === EntityType.DataProcessInstance;
}

export function isUrnTransformational(urn: string, rootType: EntityType): boolean {
    const type = getEntityTypeFromEntityUrn(urn, globalEntityRegistryV2);
    return !!type && isTransformational({ urn, type }, rootType);
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
    hide?: boolean;
}

export interface DataJobInputOutputEdgeData extends LineageTableEdgeData {
    direction?: LineageDirection;
    isInInterior?: boolean;
    isToDataFlow?: boolean;
}

export type LineageEdgeData = LineageTableEdgeData | DataJobInputOutputEdgeData;

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

export type GraphStoreFields = 'nodes' | 'edges' | 'adjacencyList';
export type LineageToggles = 'hideTransformations' | 'showDataProcessInstances' | 'showGhostEntities';

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
    /** Data Product Lineage */
    dataProductEntities: Map<Urn, FetchedEntityV2>;
    outputPortsOnly: boolean; // Restrict the graph to the home product's output ports and their adjacent nodes
    setOutputPortsOnly: (only: boolean) => void;
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
    dataProductEntities: new Map(),
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
    outputPortsOnly: false,
    setOutputPortsOnly: () => {},
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

/**
 * Builds the adjacency list used for hover highlighting from `edges`, restricted to `keepIds` on
 * both sides. Edges through a query/via node are routed *through* it (`upstream -> via -> downstream`)
 * rather than as a direct hop, so hovering any upstream of a query that fans into a shared downstream
 * lights up the shared query->downstream segment. A via node not in `keepIds` falls back to a direct
 * hop. See `useNodeHighlighting`, which matches rendered edges against these entity-level hops.
 */
export function buildHighlightAdjacencyList(
    edges: NodeContext['edges'],
    keepIds: Set<Urn>,
): NodeContext['adjacencyList'] {
    const adjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    edges.forEach((edge, edgeId) => {
        const [upstream, downstream] = parseEdgeId(edgeId);
        if (!keepIds.has(upstream) || !keepIds.has(downstream)) return;
        if (edge.via && keepIds.has(edge.via)) {
            addToAdjacencyList(adjacencyList, LineageDirection.Downstream, upstream, edge.via);
            addToAdjacencyList(adjacencyList, LineageDirection.Downstream, edge.via, downstream);
        } else {
            addToAdjacencyList(adjacencyList, LineageDirection.Downstream, upstream, downstream);
        }
    });
    return adjacencyList;
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
    /** Pagination state for each node and direction with filtered-out children, keyed by
     * `createLineageFilterNodeId`. Used by the expand/contract controls when lineage filter
     * nodes are not displayed. */
    lineageFilters: Map<string, LineageFilter>;
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
    lineageFilters: new Map(),
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
