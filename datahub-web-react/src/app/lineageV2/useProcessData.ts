import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import EntityRegistry from '../entityV2/EntityRegistry';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER, PLATFORM_FILTER_NAME } from '../searchV2/utils/constants';
import { FineGrainedOperation } from '../sharedV2/EntitySidebarContext';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import {
    ColumnRef,
    createColumnRef,
    createEdgeId,
    FineGrainedLineage,
    FineGrainedLineageMap,
    getEdgeId,
    getParents,
    isQuery,
    isTransformational,
    LINEAGE_FILTER_PAGINATION,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    LineageNode,
    LineageNodesContext,
    NodeContext,
    setDefault,
    createFineGrainedOperationRef,
    FineGrainedOperationRef,
    isUrnTransformational,
    parseColumnRef,
    createLineageFilterNodeId,
} from './common';
import { getFieldPathFromSchemaFieldUrn, getSourceUrnFromSchemaFieldUrn } from './lineageUtils';
import NodeBuilder, { LineageVisualizationNode } from './NodeBuilder';

interface FineGrainedLineageData {
    indirect: FineGrainedLineage;
    fineGrainedOperations: Map<FineGrainedOperationRef, FineGrainedOperation>;
}

interface ProcessedData {
    fineGrainedLineage: FineGrainedLineageData;
    flowNodes: LineageVisualizationNode[];
    flowEdges: Edge[];
}

export default function useProcessData(urn: string, type: EntityType): ProcessedData {
    const { nodes, edges, adjacencyList, nodeVersion, dataVersion, displayVersion } = useContext(LineageNodesContext);
    const entityRegistry = useEntityRegistryV2();
    const displayVersionNumber = displayVersion[0];

    const fineGrainedLineage = useMemo(
        () => {
            const fgl = getFineGrainedLineage({ nodes, edges }, entityRegistry);
            console.debug(fgl);
            return fgl;
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, dataVersion],
    );

    const [flowNodes, flowEdges] = useMemo(
        () => {
            console.debug({ nodes, edges, adjacencyList });
            const filteredNodes = filterNodes(urn, { nodes, edges, adjacencyList });
            const nodeBuilder = new NodeBuilder(urn, type, filteredNodes);
            return [nodeBuilder.createNodes(adjacencyList), nodeBuilder.createEdges(edges)];
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, edges, adjacencyList, nodeVersion, displayVersionNumber],
    );

    return { flowNodes, flowEdges, fineGrainedLineage };
}

/**
 * Filters nodes based on per-node filters.
 * @param urn The urn of the root node.
 * @param context Lineage node context.
 * @returns A list of nodes to display in rough topological order.
 */
function filterNodes(urn: string, context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges'>): LineageNode[] {
    const { nodes } = context;
    const rootNode = nodes.get(urn);
    if (!rootNode) {
        return [];
    }

    const orderedNodes = Array.from(nodes.values()).sort((a, b) => a.urn.localeCompare(b.urn));

    const displayedNodes: LineageNode[] = [rootNode];
    const seenNodes = new Set<string>([urn]);
    const queue = [urn]; // Note: uses array for queue, slow for large graphs
    while (queue.length > 0) {
        const current = queue.shift() as string; // Just checked length
        const node = nodes.get(current);
        getDirectionsToExpand(node).forEach((direction) => {
            const filteredChildren = applyFilters(current, direction, orderedNodes, context);
            filteredChildren.forEach((child) => {
                if (!seenNodes.has(child.id)) {
                    displayedNodes.push(child);
                    seenNodes.add(child.id);
                    if (!isTransformational(child) && child.isExpanded[direction]) {
                        queue.push(child.id);
                    }
                }
            });
        });
    }

    return displayedNodes;
}

function getDirectionsToExpand(node) {
    if (node?.direction) return [node.direction];
    return Object.values(LineageDirection).filter((direction) => !!node.isExpanded[direction]);
}

function applyFilters(
    urn: string,
    direction: LineageDirection,
    orderedNodes: LineageEntity[],
    context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges'>,
): LineageNode[] {
    const { adjacencyList, nodes } = context;
    const node = nodes.get(urn);
    const filters = node?.filters?.[direction];
    const children = adjacencyList[direction].get(urn);
    if (!node || !children?.size || filters?.display === false) {
        return [];
    }

    const { allChildren, childrenToFilter } = getChildrenToFilter(node, direction, context);
    let filteredChildren = orderedNodes.filter((n) => childrenToFilter?.has(n.urn));

    filters?.facetFilters?.forEach((values, facet) => {
        if (!values.size) {
            return;
        }
        if (facet === PLATFORM_FILTER_NAME) {
            filteredChildren = filteredChildren.filter((n) => {
                const platform = n.entity?.platform?.urn;
                return platform && values.has(platform);
            });
        } else if (facet === ENTITY_SUB_TYPE_FILTER_NAME) {
            filteredChildren = filteredChildren.filter((n) => {
                const subtype = n.entity?.subtype;
                const selectedSubtypes = Array.from(values).map((v) => v.split(FILTER_DELIMITER)[1]);
                return subtype && selectedSubtypes.includes(subtype);
            });
        }
    });

    const limit = filters?.limit || filteredChildren.length;
    const shownNodes = filteredChildren.slice(Math.max(0, filteredChildren.length - limit));
    const allShownNodes = [...getTransformationalNodes(node, shownNodes, direction, context), ...shownNodes];
    const result: LineageNode[] = [];
    if (childrenToFilter.size > LINEAGE_FILTER_PAGINATION && (!node?.direction || node.direction === direction)) {
        const filterNode: LineageFilter = {
            // id starts with 's' so it is always sorted first, before urn:li:...
            id: createLineageFilterNodeId(urn, direction),
            type: LINEAGE_FILTER_TYPE,
            parent: urn,
            direction,
            limit,
            isExpanded: {
                [LineageDirection.Upstream]: false,
                [LineageDirection.Downstream]: false,
            },
            allChildren,
            contents: filteredChildren.map((n) => n.urn),
            shown: new Set(allShownNodes.map((n) => n.urn)),
        };
        result.push(filterNode);
    }
    if (node) {
        result.push(...allShownNodes);
    }

    return result;
}

/**
 * Returns the set of children to filter for the given parent node.
 * This is calculated as: all adjacent non-transformational nodes and any transformational leaves.
 * Loop invariant: all nodes in `queue` are transformational.
 * @param parent The parent node, whose children are to be filtered.
 * @param direction Direction of children.
 * @param context Lineage node context.
 */
function getChildrenToFilter(
    parent: LineageEntity,
    direction: LineageDirection,
    context: Pick<NodeContext, 'adjacencyList' | 'nodes'>,
): { allChildren: Set<string>; childrenToFilter: Set<string> } {
    const { adjacencyList, nodes } = context;
    const seen = new Set<string>();
    const childrenToFilter = new Set<string>();
    const queue = [parent];
    for (let node = queue.pop(); node; node = queue.pop()) {
        const children = adjacencyList[direction].get(node.urn);
        // Include non-query transformational nodes if they have no children
        if (!children?.size && !isQuery(node)) childrenToFilter.add(node.urn);
        children?.forEach((childUrn) => {
            const child = nodes.get(childUrn);
            if (!child || seen.has(childUrn) || child.isSoftDeleted) return;

            if (isTransformational(child)) {
                queue.push(child);
                seen.add(childUrn);
            } else {
                childrenToFilter.add(childUrn);
                seen.add(childUrn);
            }
        });
    }

    return { allChildren: seen, childrenToFilter };
}

/**
 * Return all transformational nodes between `root` and `leaves`, in rough topological order.
 * @param root The node from which all `leaves` are (indirect) children.
 * @param leaves Set of non-transformational children of `parentUrn` to render.
 * @param direction Direction to search for transformational nodes.
 * @param context Lineage node context.
 */
function getTransformationalNodes(
    root: LineageEntity,
    leaves: LineageEntity[],
    direction: LineageDirection,
    context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges'>,
): LineageEntity[] {
    const { nodes, edges, adjacencyList } = context;

    const leafUrns = new Set<string>(leaves.map((leaf) => leaf.urn));
    const nodesInBetween = new Set<string>();
    const nodesToRoot = [...leaves];
    for (let node = nodesToRoot.pop(); node; node = nodesToRoot.pop()) {
        getParents(node, adjacencyList).forEach((parentUrn) => {
            const parent = nodes.get(parentUrn);
            if (parentUrn !== root.urn && !nodesInBetween.has(parentUrn) && parent && isTransformational(parent)) {
                nodesToRoot.push(parent);
                nodesInBetween.add(parentUrn);
            }
        });
    }

    // Order in rough topological order
    const result: LineageEntity[] = [];
    const nodesToLeaves = [root];
    for (let node = nodesToLeaves.shift(); node; node = nodesToLeaves.shift()) {
        const { urn } = node;
        if (urn !== root.urn) {
            result.push(node);
        }
        adjacencyList[direction].get(urn)?.forEach((child) => {
            const childNode = nodes.get(child);
            if (nodesInBetween.has(child) && childNode) {
                nodesToLeaves.push(childNode);
                nodesInBetween.delete(child);
            }
            if (nodesInBetween.has(child) || leafUrns.has(child)) {
                const edge = edges.get(getEdgeId(urn, child, direction));
                if (edge?.isDisplayed && edge?.via) {
                    const queryNode = nodes.get(edge.via);
                    if (queryNode) {
                        result.push(queryNode);
                    }
                }
            }
        });
    }

    return result;
}

interface TentativeEdge {
    upstreamRef: ColumnRef;
    downstreamRef: ColumnRef;
    queryRef?: ColumnRef;
    operationRef?: FineGrainedOperationRef;
}

function getFineGrainedLineage(
    context: Pick<NodeContext, 'nodes' | 'edges'>,
    entityRegistry: EntityRegistry,
): FineGrainedLineageData {
    const { nodes, edges } = context;

    const indirect: FineGrainedLineage = { downstream: new Map(), upstream: new Map() };
    const fineGrainedOperations: Map<FineGrainedOperationRef, FineGrainedOperation> = new Map();
    // CLL that may be deduplicated, if there exists a column-level path through transformational nodes
    const tentativeEdges: TentativeEdge[] = [];

    function processEdge(
        upstreamUrn: string,
        upstreamField: string,
        downstreamUrn: string,
        downstreamField: string,
        intermediateIsQuery?: boolean,
        intermediateRef?: ColumnRef,
        operationRef?: FineGrainedOperationRef,
    ) {
        const upstreamRef = createColumnRef(upstreamUrn, upstreamField);
        const downstreamRef = createColumnRef(downstreamUrn, downstreamField);
        // Drop ghost edges and self edges
        if (!nodes.has(upstreamUrn) || !nodes.has(downstreamUrn) || upstreamRef === downstreamRef) return;

        if (
            edges.get(createEdgeId(upstreamUrn, downstreamUrn))?.isDisplayed ||
            (!intermediateIsQuery && intermediateRef)
        ) {
            addFineGrainedEdges(indirect, upstreamRef, downstreamRef, intermediateRef, operationRef);
        } else {
            tentativeEdges.push({ upstreamRef, downstreamRef, queryRef: intermediateRef, operationRef });
        }
    }

    nodes.forEach((node) => {
        node.entity?.fineGrainedLineages?.forEach((entry) => {
            let operationRef: FineGrainedOperationRef | undefined;
            let queryRef: ColumnRef | undefined;
            const intermediateNode = entry.query || (node.entity?.type === EntityType.DataJob && node.entity.urn);
            if (intermediateNode) {
                operationRef = createFineGrainedOperationRef(intermediateNode, entry.upstreams, entry.downstreams);
                queryRef = createColumnRef(intermediateNode, operationRef);
                fineGrainedOperations.set(operationRef, {
                    inputColumns: entry.upstreams?.map((ref) => [ref.urn, ref.path]) || undefined,
                    outputColumns: entry.downstreams?.map((ref) => [ref.urn, ref.path]) || undefined,
                    transformOperation: entry.transformOperation || undefined,
                });
            }
            entry.upstreams?.forEach((upstream) => {
                entry.downstreams?.forEach((downstream) => {
                    processEdge(
                        upstream.urn,
                        upstream.path,
                        downstream.urn,
                        downstream.path,
                        !!entry.query,
                        queryRef,
                        operationRef,
                    );
                });
            });
        });
        node.entity?.inputFields?.fields?.forEach((input) => {
            // Upstream of chart's field `schemaField` comes in as `schemaFieldUrn`
            if (input?.schemaFieldUrn && input?.schemaField) {
                const upstreamUrn = getSourceUrnFromSchemaFieldUrn(input.schemaFieldUrn);
                const upstreamField = getFieldPathFromSchemaFieldUrn(input.schemaFieldUrn);
                processEdge(upstreamUrn, upstreamField, node.urn, input.schemaField.fieldPath);
            }
        });
    });

    tentativeEdges
        .filter(
            ({ upstreamRef, downstreamRef }) =>
                !isTransformationalPath(indirect.downstream, upstreamRef, downstreamRef, entityRegistry),
        )
        .forEach(({ upstreamRef, downstreamRef, queryRef, operationRef }) => {
            addFineGrainedEdges(indirect, upstreamRef, downstreamRef, queryRef, operationRef);
        });

    return {
        indirect,
        fineGrainedOperations,
    };
}

function addFineGrainedEdges(
    fgl: FineGrainedLineage,
    upstreamRef: ColumnRef,
    downstreamRef: ColumnRef,
    queryRef?: ColumnRef,
    operationRef?: FineGrainedOperationRef,
) {
    if (queryRef) {
        setDefault(fgl.upstream, downstreamRef, new Map()).set(queryRef, operationRef);
        setDefault(fgl.upstream, queryRef, new Map()).set(upstreamRef, null);
        setDefault(fgl.downstream, upstreamRef, new Map()).set(queryRef, operationRef);
        setDefault(fgl.downstream, queryRef, new Map()).set(downstreamRef, null);
    } else {
        setDefault(fgl.upstream, downstreamRef, new Map()).set(upstreamRef, null);
        setDefault(fgl.downstream, upstreamRef, new Map()).set(downstreamRef, null);
    }
}

function isTransformationalPath(
    downstreamMap: FineGrainedLineageMap,
    upstreamRef: ColumnRef,
    downstreamRef: ColumnRef,
    entityRegistry: EntityRegistry,
): boolean {
    const stack = [upstreamRef];
    const seen = new Set<string>(stack);
    for (let node = stack.pop(); node; node = stack.pop()) {
        const found = Array.from(downstreamMap.get(node)?.keys() || []).some((childRef) => {
            if (childRef === downstreamRef) return true;
            if (!seen.has(childRef)) {
                const [childUrn] = parseColumnRef(childRef);
                if (isUrnTransformational(childUrn, entityRegistry)) {
                    stack.push(childRef);
                    seen.add(childRef);
                }
            }
            return false;
        });
        if (found) {
            return true;
        }
    }
    return false;
}
