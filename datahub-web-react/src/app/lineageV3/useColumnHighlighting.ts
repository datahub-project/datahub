import { useContext, useEffect, useMemo } from 'react';
import { Edge, useReactFlow } from 'reactflow';
import { useTheme } from 'styled-components';

import { TENTATIVE_EDGE_NAME } from '@app/lineageV3/LineageEdge/TentativeEdge';
import {
    ColumnRef,
    FineGrainedLineage,
    FineGrainedLineageMap,
    FineGrainedOperationRef,
    HighlightedColumns,
    LineageNodesContext,
    NodeContext,
    ShownRelatedColumns,
    createColumnRef,
    createLineageFilterNodeId,
    isTransformational,
    isUrnQuery,
    isUrnTransformational,
    parseColumnRef,
    setDefault,
    setDifference,
} from '@app/lineageV3/common';
import { LINEAGE_ARROW_MARKER } from '@app/lineageV3/lineageSVGs';
import { useAppConfig } from '@app/useAppConfig';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { EntityType, LineageDirection } from '@types';

export default function useColumnHighlighting(
    selectedColumn: ColumnRef | null,
    hoveredColumn: ColumnRef | null,
    fineGrainedLineage: FineGrainedLineage,
    shownUrns: string[],
    nodeIdsByUrn: Map<string, string[]>,
): {
    cllHighlightedNodes: Map<string, Set<FineGrainedOperationRef> | null>;
    highlightedColumns: HighlightedColumns;
    shownRelatedColumns: ShownRelatedColumns;
} {
    const entityRegistry = useEntityRegistryV2();
    const theme = useTheme();
    const { setEdges } = useReactFlow();
    const { showLineageFilterNodes } = useAppConfig().config.featureFlags;
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

    const { cllHighlightedNodes, highlightedColumns, shownRelatedColumns, columnEdges } = useMemo(() => {
        const displayedNodeIds = new Set(shownUrns);
        const validQueryIds = new Set(
            Array.from(edges.values())
                .map((edge) => edge.via)
                .filter((via): via is string => !!via),
        );
        return processColumnHighlights(
            selectedColumn,
            hoveredColumn,
            {
                fineGrainedLineage,
                nodes,
                adjacencyList,
                displayedNodeIds,
                nodeIdsByUrn,
                validQueryIds,
                rootUrn,
                rootType,
                showFilterNodes: showLineageFilterNodes,
            },
            theme.colors.borderSelected,
            theme.colors.borderHover,
        );
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [
        columnEdgeVersion,
        selectedColumn,
        hoveredColumn,
        nodes,
        edges,
        fineGrainedLineage,
        shownUrns,
        nodeIdsByUrn,
        entityRegistry,
        showLineageFilterNodes,
    ]);

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

    return { cllHighlightedNodes, highlightedColumns, shownRelatedColumns };
}

interface ArgumentBundle {
    fineGrainedLineage: FineGrainedLineage;
    nodes: NodeContext['nodes'];
    adjacencyList: NodeContext['adjacencyList'];
    displayedNodeIds: Set<string>;
    /** Flow node ids for each urn, as one urn can be rendered by multiple nodes. */
    nodeIdsByUrn: Map<string, string[]>;
    validQueryIds: Set<string>;
    rootUrn: string;
    rootType: EntityType;
    /** Whether lineage filter nodes are rendered, rather than the column lineage controls. */
    showFilterNodes: boolean;
}

function processColumnHighlights(
    selectedColumn: ColumnRef | null,
    hoveredColumn: ColumnRef | null,
    argumentBundle: ArgumentBundle,
    selectColor: string,
    hoverColor: string,
) {
    if (selectedColumn) {
        return computeSingleColumnHighlights(selectedColumn, argumentBundle, selectColor);
    }
    return computeSingleColumnHighlights(hoveredColumn, argumentBundle, hoverColor);
}

export function computeSingleColumnHighlights(
    column: ColumnRef | null,
    {
        fineGrainedLineage,
        nodes,
        adjacencyList,
        displayedNodeIds,
        nodeIdsByUrn,
        validQueryIds,
        rootUrn,
        rootType,
        showFilterNodes,
    }: ArgumentBundle,
    stroke: string,
): {
    cllHighlightedNodes: Map<string, Set<FineGrainedOperationRef> | null>;
    highlightedColumns: HighlightedColumns;
    shownRelatedColumns: ShownRelatedColumns;
    columnEdges: Map<string, Edge>;
} {
    const cllHighlightedNodes = new Map<string, Set<FineGrainedOperationRef> | null>();
    const highlightedColumns = new Map<string, Set<string>>();
    const shownRelatedColumns: ShownRelatedColumns = new Map();
    const columnEdges = new Map<string, Edge>();
    const nodeIdsFor = (urn: string) => nodeIdsByUrn.get(urn) ?? [urn];

    if (column === null) {
        return { cllHighlightedNodes, highlightedColumns, shownRelatedColumns, columnEdges };
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
                const isRootTransformation = isTransformational({ urn: rootUrn, type: rootType }, rootType);
                const throughRoot =
                    adjacencyList.UPSTREAM.get(rootUrn)?.has(fromUrn) &&
                    adjacencyList.DOWNSTREAM.get(rootUrn)?.has(toUrn);
                if (!(isRootTransformation && throughRoot)) {
                    // Don't render edges between nodes upstream of home node and nodes downstream of home node
                    // Exception for edges through the root if it's a transformation
                    return;
                }
            }
            // Handle ids stay urn-based (see Column.tsx), but node ids may not be: an entity in a
            // data product renders as `<dataProductUrn>␟<urn>`, once per data product it belongs
            // to, so a single column->column edge can map to several rendered edges.
            nodeIdsFor(fromUrn).forEach((source) => {
                nodeIdsFor(toUrn).forEach((target) => {
                    const id = `${createColumnRef(source, fromField)}-${createColumnRef(target, toField)}`;
                    columnEdges.set(id, {
                        id,
                        source,
                        target,
                        sourceHandle: fromField ? fromRef : undefined,
                        targetHandle: toField ? toRef : undefined,
                        type: isTentative ? TENTATIVE_EDGE_NAME : 'default',
                        markerEnd: LINEAGE_ARROW_MARKER,
                        style: { stroke, strokeWidth: 1.25 },
                        data: { isColumnEdge: true }, // Used to hide column edges
                    });
                });
            });
        }

        const seen = new Set<ColumnRef>();
        const toVisit = [column];
        while (toVisit.length) {
            const ref = toVisit.pop();
            if (ref === undefined) {
                break;
            }
            // Every column reached in this direction reports how much of its own lineage is on the
            // graph, so each can show what it is hiding -- but only on the side we traversed, as
            // the other side of it was never explored
            const [currentUrn] = parseColumnRef(ref);
            if (displayedNodeIds.has(currentUrn)) {
                const numRelatedOnGraph = countRelatedColumnsOnGraph(ref, fgl, displayedNodeIds, rootType);
                setDefault(shownRelatedColumns, ref, {})[direction] = numRelatedOnGraph;

                if (showFilterNodes) {
                    const { filterNodeRef, showFilterNodeEdge, isTentative } = getLineageFilterNodeEdge(
                        ref,
                        direction,
                        nodes,
                        numRelatedOnGraph,
                    );
                    if (showFilterNodeEdge) {
                        addEdge(ref, filterNodeRef, isTentative);
                    }
                }
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
                } else if (!isUrnQuery(childUrn) || validQueryIds.has(childUrn)) {
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

    return { cllHighlightedNodes, highlightedColumns, shownRelatedColumns, columnEdges };
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
    /* eslint-enable no-continue */

    topologicalOrder.reverse();
    return topologicalOrder;
}

/**
 * Computes the edge from a column to the lineage filter node holding the lineage it has that isn't
 * on the graph: tentative while counts are unknown, solid once they show there is more to see, and
 * absent once everything is displayed. Only used when lineage filter nodes are rendered; otherwise
 * the column lineage controls carry these counts.
 */
function getLineageFilterNodeEdge(
    ref: ColumnRef,
    direction: LineageDirection,
    nodes: NodeContext['nodes'],
    numRelatedOnGraph: number,
): {
    filterNodeRef: ColumnRef;
    showFilterNodeEdge: boolean;
    isTentative: boolean;
} {
    const [urn, field] = parseColumnRef(ref);
    const filterNodeRef = createLineageFilterNodeId(urn, direction);
    const lineageAsset = nodes.get(urn)?.entity?.lineageAssets?.get(field);

    const cachedNumRelated =
        direction === LineageDirection.Downstream ? lineageAsset?.numDownstream : lineageAsset?.numUpstream;

    // Show tentative edge if we haven't fetched counts yet, even if we have cached value
    const isTentative = !lineageAsset?.lineageCountsFetched;
    return {
        filterNodeRef,
        showFilterNodeEdge: (cachedNumRelated ?? 0) > numRelatedOnGraph || isTentative,
        isTentative,
    };
}

/**
 * Number of columns related to `ref` that are rendered on the graph, to compare against the count
 * fetched for the column. Traverses through refs that aren't rendered as their own column -- both
 * transformations and nodes missing from the graph -- as those aren't counted for the column either.
 */
function countRelatedColumnsOnGraph(
    ref: ColumnRef,
    fgl: FineGrainedLineageMap,
    displayedNodeIds: Set<string>,
    rootType: EntityType,
): number {
    const related = new Set<ColumnRef>();
    const seen = new Set<ColumnRef>([ref]);
    const toVisit = Array.from(fgl.get(ref)?.keys() || []);
    while (toVisit.length) {
        const neighbor = toVisit.pop();
        if (neighbor !== undefined && !seen.has(neighbor)) {
            seen.add(neighbor);
            const [neighborUrn] = parseColumnRef(neighbor);
            if (displayedNodeIds.has(neighborUrn) && !isUrnTransformational(neighborUrn, rootType)) {
                related.add(neighbor);
            } else {
                toVisit.push(...(fgl.get(neighbor)?.keys() || []));
            }
        }
    }
    return related.size;
}
