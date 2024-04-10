import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER, PLATFORM_FILTER_NAME } from '../searchV2/utils/constants';
import { ColumnQueryData } from '../sharedV2/EntitySidebarContext';
import {
    ColumnRef,
    createColumnQueryRef,
    createColumnRef,
    createEdgeId,
    FineGrainedLineage,
    FineGrainedLineageMap,
    getEdgeId,
    getParents,
    isQuery,
    isTransformational,
    LINEAGE_FILTER_ID_PREFIX,
    LINEAGE_FILTER_PAGINATION,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    LineageNode,
    LineageNodesContext,
    NodeContext,
    parseColumnRef,
    setDefault,
} from './common';
import { getFieldPathFromSchemaFieldUrn, getSourceUrnFromSchemaFieldUrn } from './lineageUtils';
import NodeBuilder, { NodeWithMetadata } from './NodeBuilder';

interface FineGrainedLineageData {
    direct: FineGrainedLineage;
    indirect: FineGrainedLineage;
    columnQueryData: Map<ColumnRef, ColumnQueryData>;
}

interface ProcessedData {
    fineGrainedLineage: FineGrainedLineageData;
    flowNodes: NodeWithMetadata[];
    flowEdges: Edge[];
}

export default function useProcessData(urn: string, type: EntityType): ProcessedData {
    const { nodes, edges, adjacencyList, nodeVersion, dataVersion, displayVersion } = useContext(LineageNodesContext);
    const displayVersionNumber = displayVersion[0];

    const fineGrainedLineage = useMemo(
        () => getFineGrainedLineage({ nodes, edges }),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, dataVersion],
    );

    const [flowNodes, flowEdges] = useMemo(
        () => {
            console.log({ nodes, edges, adjacencyList });
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
        const directionsToSearch = node?.direction ? [node.direction] : Object.values(LineageDirection);
        const filteredChildren = directionsToSearch
            .map((direction) => applyFilters(current, direction, orderedNodes, context))
            .flat();

        filteredChildren.forEach((child) => {
            if (!seenNodes.has(child.id)) {
                displayedNodes.push(child);
                seenNodes.add(child.id);
                if (!isTransformational(child) && child.isExpanded) {
                    queue.push(child.id);
                }
            }
        });
    }

    return displayedNodes;
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

    const childrenToFilter = getChildrenToFilter(node, direction, context);
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
    const result: LineageNode[] = [];
    if (childrenToFilter.size > LINEAGE_FILTER_PAGINATION && (!node?.direction || node.direction === direction)) {
        const dir = direction === LineageDirection.Upstream ? 'u:' : 'd:';
        const filterNode: LineageFilter = {
            // id starts with 's' so it is always sorted first, before urn:li:...
            id: `${LINEAGE_FILTER_ID_PREFIX}${dir}${urn}`,
            type: LINEAGE_FILTER_TYPE,
            parent: urn,
            direction,
            limit,
            isExpanded: false,
            contents: Array.from(childrenToFilter),
            shown: new Set(shownNodes.map((n) => n.urn)),
        };
        result.push(filterNode);
    }
    if (node) {
        result.push(...getTransformationalNodes(node, shownNodes, direction, context), ...shownNodes);
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
): Set<string> {
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
            if (!child || seen.has(childUrn)) return;

            if (isTransformational(child)) {
                queue.push(child);
                seen.add(childUrn);
            } else {
                childrenToFilter.add(childUrn);
            }
        });
    }

    return childrenToFilter;
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
            const edge = edges.get(getEdgeId(urn, child, direction));
            if (edge?.isDisplayed && edge?.via) {
                const queryNode = nodes.get(edge.via);
                if (queryNode) {
                    result.push(queryNode);
                }
            }
        });
    }

    return result;
}

function getFineGrainedLineage(context: Pick<NodeContext, 'nodes' | 'edges'>): FineGrainedLineageData {
    const { nodes } = context;

    // Edges through query nodes
    const forward: FineGrainedLineageMap = new Map();
    const backward: FineGrainedLineageMap = new Map();
    // Edges skip query nodes
    const forwardDirect: FineGrainedLineageMap = new Map();
    const backwardDirect: FineGrainedLineageMap = new Map();

    const columnQueryData: Map<ColumnRef, ColumnQueryData> = new Map();

    nodes.forEach((node) => {
        node.entity?.fineGrainedLineages?.forEach((entry) => {
            const queryRef = entry.query && createColumnQueryRef(entry.query, entry.upstreams, entry.downstreams);
            if (queryRef) {
                const [queryNodeId] = parseColumnRef(queryRef);
                columnQueryData.set(queryNodeId, {
                    inputColumns: entry.upstreams?.map((ref) => [ref.urn, ref.path]) || undefined,
                    outputColumns: entry.downstreams?.map((ref) => [ref.urn, ref.path]) || undefined,
                    transformOperation: entry.transformOperation || undefined,
                });
            }
            entry.upstreams?.forEach((from) => {
                const fromRef = createColumnRef(from.urn, from.path);
                entry.downstreams?.forEach((to) => {
                    if (shouldAddFineGrainedEdge(context, from.urn, to.urn)) {
                        addFineGrainedEdges(
                            forward,
                            forwardDirect,
                            fromRef,
                            createColumnRef(to.urn, to.path),
                            queryRef,
                        );
                    }
                });
            });
            entry.downstreams?.forEach((from) => {
                const fromRef = createColumnRef(from.urn, from.path);
                entry.upstreams?.forEach((to) => {
                    if (shouldAddFineGrainedEdge(context, to.urn, from.urn)) {
                        addFineGrainedEdges(
                            backward,
                            backwardDirect,
                            fromRef,
                            createColumnRef(to.urn, to.path),
                            queryRef,
                        );
                    }
                });
            });
        });
        node.entity?.inputFields?.fields?.forEach((input) => {
            if (input?.schemaFieldUrn && input?.schemaField) {
                const upstreamUrn = getSourceUrnFromSchemaFieldUrn(input.schemaFieldUrn);
                const upstreamRef = createColumnRef(upstreamUrn, getFieldPathFromSchemaFieldUrn(input.schemaFieldUrn));
                const downstreamRef = createColumnRef(node.urn, input.schemaField.fieldPath);
                if (nodes.has(upstreamUrn)) {
                    addFineGrainedEdges(forward, forwardDirect, upstreamRef, downstreamRef, undefined);
                    addFineGrainedEdges(backward, backwardDirect, downstreamRef, upstreamRef, undefined);
                }
            }
        });
    });

    return {
        indirect: { forward, backward },
        direct: { forward: forwardDirect, backward: backwardDirect },
        columnQueryData,
    };
}

function shouldAddFineGrainedEdge(
    context: Pick<NodeContext, 'nodes' | 'edges'>,
    upstreamUrn: string,
    downstreamUrn: string,
): boolean {
    const { nodes, edges } = context;
    return (
        nodes.has(upstreamUrn) &&
        nodes.has(downstreamUrn) &&
        !!edges.get(createEdgeId(upstreamUrn, downstreamUrn))?.isDisplayed
    );
}

function addFineGrainedEdges(
    map: FineGrainedLineageMap,
    directMap: FineGrainedLineageMap,
    from: ColumnRef,
    to: ColumnRef,
    query: ColumnRef | null | undefined,
) {
    if (query) {
        setDefault(map, from, []).push(query);
        setDefault(map, query, []).push(to);
    } else {
        setDefault(map, from, []).push(to);
    }
    setDefault(directMap, from, []).push(to);
}
