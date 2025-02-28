import EntityRegistry from '@app/entityV2/EntityRegistry';
import { TENTATIVE_EDGE_NAME } from '@app/lineageV2/LineageEdge/TentativeEdge';
import { useContext, useEffect, useMemo } from 'react';
import { Edge, MarkerType, useReactFlow } from 'reactflow';
import { EntityType, LineageDirection } from '@types';
import {
    ColumnRef,
    createLineageFilterNodeId,
    FineGrainedLineage,
    FineGrainedLineageMap,
    FineGrainedOperationRef,
    HighlightedColumns,
    HOVER_COLOR,
    isTransformational,
    isUrnQuery,
    LineageNodesContext,
    NodeContext,
    parseColumnRef,
    SELECT_COLOR,
    setDefault,
    setDifference,
} from './common';
import { useEntityRegistryV2 } from '../useEntityRegistry';

export default function useColumnHighlighting(
    selectedColumn: ColumnRef | null,
    hoveredColumn: ColumnRef | null,
    fineGrainedLineage: FineGrainedLineage,
    shownUrns: string[],
): {
    cllHighlightedNodes: Map<string, Set<FineGrainedOperationRef> | null>;
    highlightedColumns: HighlightedColumns;
} {
    const entityRegistry = useEntityRegistryV2();
    const { setEdges } = useReactFlow();
    const {
        nodes,
        adjacencyList,
        edges,
        rootUrn,
        rootType,
        nodeVersion,
        columnEdgeVersion,
        hideTransformations,
        showDataProcessInstances,
    } = useContext(LineageNodesContext);

    const { cllHighlightedNodes, highlightedColumns, columnEdges } = useMemo(() => {
        const displayedNodeIds = new Set(shownUrns);
        const validQueryIds = new Set(
            Array.from(edges.values())
                .map((edge) => edge.via)
                .filter((via): via is string => !!via),
        );
        return processColumnHighlights(selectedColumn, hoveredColumn, {
            fineGrainedLineage,
            nodes,
            adjacencyList,
            displayedNodeIds,
            validQueryIds,
            entityRegistry,
            rootUrn,
            rootType,
        });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [columnEdgeVersion, selectedColumn, hoveredColumn, nodes, edges, fineGrainedLineage, shownUrns, entityRegistry]);

    useEffect(() => {
        // TODO: Figure out how to only add edges once columns are rendered? For now, just use timeout
        setTimeout(
            () =>
                setEdges((oldEdges) => {
                    const currentEdgeIds = new Set(columnEdges.keys());
                    const oldEdgeIds = new Set(oldEdges.map((edge) => edge.id));
                    const addIds = setDifference(currentEdgeIds, oldEdgeIds);
                    return [
                        ...oldEdges
                            .filter((edge) => !edge.data?.isColumnEdge || currentEdgeIds.has(edge.id))
                            .map((edge) => columnEdges.get(edge.id) || edge),
                        ...addIds.map((id) => columnEdges.get(id) as Edge),
                    ];
                }),
            0,
        );
    }, [nodeVersion, hideTransformations, showDataProcessInstances, columnEdges, setEdges]);

    return { cllHighlightedNodes, highlightedColumns };
}

interface ArgumentBundle {
    fineGrainedLineage: FineGrainedLineage;
    nodes: NodeContext['nodes'];
    adjacencyList: NodeContext['adjacencyList'];
    displayedNodeIds: Set<string>;
    validQueryIds: Set<string>;
    entityRegistry: EntityRegistry;
    rootUrn: string;
    rootType: EntityType;
}

function processColumnHighlights(
    selectedColumn: ColumnRef | null,
    hoveredColumn: ColumnRef | null,
    argumentBundle: ArgumentBundle,
) {
    if (selectedColumn) {
        return computeSingleColumnHighlights(selectedColumn, argumentBundle, SELECT_COLOR);
    }
    return computeSingleColumnHighlights(hoveredColumn, argumentBundle, HOVER_COLOR);
}

function computeSingleColumnHighlights(
    column: ColumnRef | null,
    {
        fineGrainedLineage,
        nodes,
        adjacencyList,
        displayedNodeIds,
        validQueryIds,
        entityRegistry,
        rootUrn,
        rootType,
    }: ArgumentBundle,
    stroke: string,
): {
    cllHighlightedNodes: Map<string, Set<FineGrainedOperationRef> | null>;
    highlightedColumns: HighlightedColumns;
    columnEdges: Map<string, Edge>;
} {
    const cllHighlightedNodes = new Map<string, Set<FineGrainedOperationRef> | null>();
    const highlightedColumns = new Map<string, Set<string>>();
    const columnEdges = new Map<string, Edge>();

    if (column === null) {
        return { cllHighlightedNodes, highlightedColumns, columnEdges };
    }

    const [urn, field] = parseColumnRef(column);
    cllHighlightedNodes.set(urn, null);
    highlightedColumns.set(urn, new Set([field]));

    const lineages: Array<[LineageDirection, FineGrainedLineageMap]> = [
        [LineageDirection.Downstream, fineGrainedLineage.downstream],
        [LineageDirection.Upstream, fineGrainedLineage.upstream],
    ];
    lineages.forEach(([direction, fgl]) => {
        // We want to show all CLL edges between displayed nodes,
        //   even if they go through a node that is not displayed, i.e. a missing node
        // To do this, we compute each missing node's direct parents
        //   i.e. the missing node's upstreams when searching downstream, and vice versa
        const missingNodeParents = new Map<string, Set<ColumnRef>>();

        function addEdge(ref: ColumnRef, childRef: ColumnRef, isTentative = false) {
            const fromRef = direction === LineageDirection.Downstream ? ref : childRef;
            const toRef = direction === LineageDirection.Downstream ? childRef : ref;
            const [fromUrn, fromField] = parseColumnRef(fromRef);
            const [toUrn, toField] = parseColumnRef(toRef);
            const fromDirection = nodes.get(fromUrn)?.direction;
            const toDirection = nodes.get(toUrn)?.direction;
            if (fromDirection && toDirection && fromDirection !== toDirection) {
                const isRootTransformation = isTransformational({ urn: rootUrn, type: rootType });
                const throughRoot =
                    adjacencyList.UPSTREAM.get(rootUrn)?.has(fromUrn) &&
                    adjacencyList.DOWNSTREAM.get(rootUrn)?.has(toUrn);
                if (!(isRootTransformation && throughRoot)) {
                    // Don't render edges between nodes upstream of home node and nodes downstream of home node
                    // Exception for edges through the root if it's a transformation
                    return;
                }
            }
            const id = `${fromRef}-${toRef}`;
            columnEdges.set(id, {
                id,
                source: fromUrn,
                target: toUrn,
                sourceHandle: fromField ? fromRef : undefined,
                targetHandle: toField ? toRef : undefined,
                type: isTentative ? TENTATIVE_EDGE_NAME : 'default',
                markerEnd: { type: MarkerType.ArrowClosed },
                style: { stroke, strokeWidth: 1.25 },
                data: { isColumnEdge: true }, // Used to hide column edges
            });
        }

        const seen = new Set<ColumnRef>();
        const toVisit = [column];
        while (toVisit.length) {
            const ref = toVisit.pop();
            if (ref === undefined) {
                break;
            }
            const { filterNodeRef, showFilterNodeEdge, isTentative } = addEdgeToLineageFilterNode(
                ref,
                direction,
                fgl,
                nodes,
                displayedNodeIds,
            );
            const [currentUrn] = parseColumnRef(ref);
            if (displayedNodeIds.has(currentUrn) && showFilterNodeEdge) {
                addEdge(ref, filterNodeRef, isTentative);
            }

            fgl.get(ref)?.forEach((fineGrainedOperationRef, childRef) => {
                const [childUrn, childField] = parseColumnRef(childRef);
                if (!seen.has(childRef)) {
                    seen.add(childRef);
                    toVisit.push(childRef);
                }

                const queryRefsOnChild = setDefault(cllHighlightedNodes, childUrn, null);
                if (fineGrainedOperationRef) {
                    if (queryRefsOnChild === null) {
                        cllHighlightedNodes.set(childUrn, new Set());
                    }
                    cllHighlightedNodes.get(childUrn)?.add(fineGrainedOperationRef);
                }
                setDefault(highlightedColumns, childUrn, new Set()).add(childField);

                if (displayedNodeIds.has(childUrn)) {
                    addEdge(ref, childRef);
                } else if (!isUrnQuery(childUrn, entityRegistry) || validQueryIds.has(childUrn)) {
                    // Compute parents of missing nodes; don't add any edges through them
                    setDefault(missingNodeParents, childRef, new Set()).add(ref);
                }
            });
        }

        // To handle chains of missing nodes, e.g. t1 -> m1 -> m2 -> m3 -> t2, we traverse just the missing nodes
        //   and build up the missingNodeParents map to include not just direct parents but any ancestor
        //   that can be reached exclusively through missing nodes
        // When a missing node is visited, we try to add edges from its parents to its children
        // If its children are missing as well, its parents get added to each child's `missingNodeParents` set
        // We have to traverse in topological order so that we can be confident we're passing all of a node's parents
        //   e.g. we don't want to copy m1's parents to m2, and then add more parents to m1
        const orderedMissingNodes = getTopologicalOrder(new Set(missingNodeParents.keys()), fgl);
        orderedMissingNodes.forEach((ref) => {
            fgl.get(ref)?.forEach((_, childRef) => {
                const childParents = missingNodeParents.get(childRef);
                if (childParents) {
                    // `ref` is a missing node
                    missingNodeParents.get(ref)?.forEach((parentRef) => childParents.add(parentRef));
                } else {
                    missingNodeParents.get(ref)?.forEach((parentRef) => {
                        const [parentUrn] = parseColumnRef(childRef);
                        if (displayedNodeIds.has(parentUrn)) {
                            addEdge(parentRef, childRef);
                        }
                    });
                }
            });
        });
    });

    return { cllHighlightedNodes, highlightedColumns, columnEdges };
}

function getTopologicalOrder(missingNodes: Set<ColumnRef>, fgl: FineGrainedLineageMap) {
    const topologicalOrder: ColumnRef[] = [];

    // Attempt to visit missing nodes in topological order
    // There can be cycles in the fgl graph... not sure about behavior when there is a cycle of missing nodes
    const temporaryMarkedNodes = new Set<ColumnRef>();
    const permanentMarkedNodes = new Set<ColumnRef>();
    const toVisit = Array.from(missingNodes);
    /* eslint-disable no-continue */
    while (toVisit.length) {
        const ref = toVisit.pop();
        if (ref === undefined) break;
        if (permanentMarkedNodes.has(ref)) continue;
        if (temporaryMarkedNodes.has(ref)) continue; // Cycle detected
        temporaryMarkedNodes.add(ref);
        fgl.get(ref)?.forEach((_operationRef, childRef) => {
            if (
                missingNodes.has(childRef) &&
                !permanentMarkedNodes.has(childRef) &&
                !temporaryMarkedNodes.has(childRef)
            ) {
                toVisit.push(childRef);
            }
        });
        permanentMarkedNodes.add(ref);
        topologicalOrder.push(ref);
    }
    /* eslint-disable-enable no-continue */

    topologicalOrder.reverse();
    return topologicalOrder;
}

function addEdgeToLineageFilterNode(
    ref: ColumnRef,
    direction: LineageDirection,
    fgl: FineGrainedLineageMap,
    nodes: NodeContext['nodes'],
    displayedNodeIds: Set<string>,
): {
    filterNodeRef: ColumnRef;
    showFilterNodeEdge: boolean;
    isTentative: boolean;
} {
    const [urn, field] = parseColumnRef(ref);
    const filterNodeRef = createLineageFilterNodeId(urn, direction);

    const entity = nodes.get(urn)?.entity;
    const lineageAsset = entity?.lineageAssets?.get(field);

    const cachedNumRelated =
        direction === LineageDirection.Downstream ? lineageAsset?.numDownstream : lineageAsset?.numUpstream;
    const numRelatedOnGraph = Array.from(fgl.get(ref)?.keys() || []).filter((neighbor) =>
        displayedNodeIds.has(neighbor),
    ).length;

    // Show tentative edge if we haven't fetched counts yet, even if we have cached value
    const isTentative = !lineageAsset?.lineageCountsFetched;
    return {
        filterNodeRef,
        showFilterNodeEdge: (cachedNumRelated ?? 0) > numRelatedOnGraph || isTentative,
        isTentative,
    };
}
