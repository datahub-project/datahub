import { Maybe } from 'graphql/jsutils/Maybe';
import { useContext, useMemo } from 'react';
import { Edge } from 'reactflow';
import { EntityType, LineageDirection } from '../../types.generated';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER, PLATFORM_FILTER_NAME } from '../searchV2/utils/constants';
import { ColumnQueryData } from '../shared/EntitySidebarContext';
import {
    ColumnRef,
    createColumnQueryRef,
    createColumnRef,
    FineGrainedLineage,
    FineGrainedLineageMap,
    isQuery,
    isTransformational,
    LINEAGE_FILTER_ID_PREFIX,
    LINEAGE_FILTER_PAGINATION,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    LineageNode,
    LineageNodesContext,
    NeighborData,
    NeighborMap,
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
    neighborData: NeighborData;
}

export default function useProcessData(urn: string, type: EntityType): ProcessedData {
    const { nodes, edges, nodeVersion, dataVersion, displayVersion } = useContext(LineageNodesContext);
    const displayVersionNumber = displayVersion[0];

    const fineGrainedLineage = useMemo(
        () => getFineGrainedLineage(nodes),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, dataVersion],
    );

    const [flowNodes, flowEdges, neighborData] = useMemo(
        () => {
            const neighborMaps = getNeighborMaps(nodes);
            const filteredNodes = filterNodes(urn, nodes, neighborMaps);
            const nodeBuilder = new NodeBuilder(urn, type, filteredNodes);
            return [nodeBuilder.createNodes(), nodeBuilder.createEdges(edges), neighborMaps];
        }, // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, edges, nodeVersion, displayVersionNumber],
    );

    return { flowNodes, flowEdges, fineGrainedLineage, neighborData };
}

/**
 * Filters nodes based on per-node filters.
 * @param urn The urn of the root node.
 * @param nodes The list of urns fetched for the lineage visualization.
 * @param neighborMap Per direction, an association list of urns to their neighbors in that direction.
 * @returns A list of nodes to display in rough topological order.
 */
function filterNodes(
    urn: string,
    nodes: NodeContext['nodes'],
    neighborMap: Record<LineageDirection, NeighborMap>,
): LineageNode[] {
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
            .map((direction) => applyFilters(current, direction, orderedNodes, nodes, neighborMap[direction]))
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
    nodes: NodeContext['nodes'],
    neighborMap: NeighborMap,
): LineageNode[] {
    // TODO: Only create lineage filter nodes for entity nodes (not transformational)
    const node = nodes.get(urn);
    const filters = node?.filters?.[direction];
    const children = neighborMap.get(urn);
    if (!node || !children || filters?.display === false) {
        return [];
    }

    const childrenToFilter = getChildrenToFilter(node, nodes, neighborMap);
    let filteredChildren = orderedNodes.filter((n) => childrenToFilter?.has(n.urn));

    filters?.facetFilters?.forEach((values, facet) => {
        if (!values.size) {
            return;
        }
        if (facet === PLATFORM_FILTER_NAME) {
            filteredChildren = filteredChildren.filter((n) => {
                const platform = n.entity?.platform?.urn || n.backupEntity?.platform?.urn;
                return platform && values.has(platform);
            });
        } else if (facet === ENTITY_SUB_TYPE_FILTER_NAME) {
            filteredChildren = filteredChildren.filter((n) => {
                const subtype = n.entity?.subtype || n.backupEntity?.subTypes?.typeNames?.[0];
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
            parents: new Set([urn]),
            direction,
            limit,
            isExpanded: false,
            contents: Array.from(childrenToFilter),
            shown: new Set(shownNodes.map((n) => n.urn)),
        };
        result.push(filterNode);
    }
    if (node) {
        result.push(...getTransformationalNodes(node, shownNodes, nodes, neighborMap), ...shownNodes);
    }

    return result;
}

/**
 * Returns the set of children to filter for the given parent node.
 * This is calculated as: all adjacent non-transformational nodes and any transformational leaves.
 * Loop invariant: all nodes in `queue` are transformational.
 * @param parent The parent node, whose children are to be filtered.
 * @param nodes All nodes in the lineage graph.
 * @param neighborMap Association list of edges in the appropriate direction.
 */
function getChildrenToFilter(
    parent: LineageEntity,
    nodes: NodeContext['nodes'],
    neighborMap: NeighborMap,
): Set<string> {
    const seen = new Set<string>();
    const childrenToFilter = new Set<string>();
    const queue = [parent];
    for (let node = queue.pop(); node; node = queue.pop()) {
        const children = neighborMap.get(node.urn);
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
 * @param nodes Map of all nodes in the lineage graph.
 * @param neighborMap Association list in the appropriate direction, to put nodes in topological order.
 */
function getTransformationalNodes(
    root: LineageEntity,
    leaves: LineageEntity[],
    nodes: NodeContext['nodes'],
    neighborMap: NeighborMap,
): LineageEntity[] {
    const nodesInBetween = new Set<string>();
    const nodesToRoot = [...leaves];
    for (let node = nodesToRoot.pop(); node; node = nodesToRoot.pop()) {
        node.parents.forEach((parentUrn) => {
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
        if (node.urn !== root.urn) {
            result.push(node);
        }
        neighborMap.get(node.urn)?.forEach((parent) => {
            const parentNode = nodes.get(parent);
            if (nodesInBetween.has(parent) && parentNode) {
                nodesToLeaves.push(parentNode);
                nodesInBetween.delete(parent);
            }
        });
    }

    return result;
}

function getNeighborMaps(nodes: NodeContext['nodes']): NeighborData {
    const upstreamChildren = new Map<string, Set<string>>();
    const downstreamChildren = new Map<string, Set<string>>();
    nodes.forEach((node) => {
        node.parents.forEach((parent) => {
            if (node.direction === LineageDirection.Upstream) {
                setDefault(upstreamChildren, parent, new Set()).add(node.urn);
                setDefault(downstreamChildren, node.urn, new Set()).add(parent);
            } else if (node.direction === LineageDirection.Downstream) {
                setDefault(downstreamChildren, parent, new Set()).add(node.urn);
                setDefault(upstreamChildren, node.urn, new Set()).add(parent);
            }
        });
    });
    return {
        [LineageDirection.Upstream]: upstreamChildren,
        [LineageDirection.Downstream]: downstreamChildren,
    };
}

function getFineGrainedLineage(nodes: NodeContext['nodes']): FineGrainedLineageData {
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
                    if (shouldAddFineGrainedEdge(node, LineageDirection.Upstream, nodes, from.urn, to.urn)) {
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
                    if (shouldAddFineGrainedEdge(node, LineageDirection.Downstream, nodes, from.urn, to.urn)) {
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
    node: LineageEntity,
    direction: LineageDirection,
    nodes: NodeContext['nodes'],
    fromUrn: string,
    toUrn: string,
): boolean {
    const from = nodes.get(toUrn);
    const to = nodes.get(fromUrn);
    if (!from || !to) return false;

    // !node.direction means the node is the home node
    const fromIsParent =
        (node.direction && node.direction === direction) ||
        (!node.direction && direction === LineageDirection.Upstream);
    const parent = fromIsParent ? from : to;
    const child = fromIsParent ? to : from;

    return !child.prunedParents?.has(parent.urn);
}

function addFineGrainedEdges(
    map: FineGrainedLineageMap,
    directMap: FineGrainedLineageMap,
    from: ColumnRef,
    to: ColumnRef,
    query: Maybe<ColumnRef>,
) {
    if (query) {
        setDefault(map, from, []).push(query);
        setDefault(map, query, []).push(to);
    } else {
        setDefault(map, from, []).push(to);
    }
    setDefault(directMap, from, []).push(to);
}
