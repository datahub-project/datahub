import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import {
    createLineageFilterNodeId,
    getEdgeId,
    getParents,
    isQuery,
    isTransformational,
    isUrnTransformational,
    LINEAGE_FILTER_PAGINATION,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    LineageNode,
    NodeContext,
} from '@app/lineageV2/common';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { LineageDirection } from '@types';

/**
 * Filters nodes based on per-node filters.
 * @param urn The urn of the root node.
 * @param context Lineage node context.
 * @returns A list of nodes to display in rough topological order.
 */
export default function getDisplayedNodes(
    urn: string,
    context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges'>,
): LineageNode[] {
    const { nodes } = context;
    const rootNode = nodes.get(urn);
    if (!rootNode) {
        return [];
    }

    const orderedNodes = {
        [LineageDirection.Upstream]: orderNodes(urn, LineageDirection.Upstream, context),
        [LineageDirection.Downstream]: orderNodes(urn, LineageDirection.Downstream, context),
    };

    const displayedNodes: LineageNode[] = [rootNode];
    const seenNodes = new Set<string>([urn]);
    const queue = [urn]; // Note: uses array for queue, slow for large graphs
    while (queue.length > 0) {
        const current = queue.shift() as string; // Just checked length
        const node = nodes.get(current);
        getDirectionsToExpand(node).forEach((direction) => {
            const filteredChildren = applyFilters(current, direction, orderedNodes[direction], context);
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

/**
 * Orders nodes in BFS order, starting from the root node.
 * Within a single node, children are ordered transformations last, then alphabetically.
 * Note: Transformations should be put first once show more is put at the bottom.
 * @param urn Root node urn.
 * @param direction Direction in which to perform BFS.
 * @param context Lineage node context.
 */
function orderNodes(
    urn: string,
    direction: LineageDirection,
    { nodes, adjacencyList }: Pick<NodeContext, 'adjacencyList' | 'nodes'>,
): LineageEntity[] {
    const orderedNodes: string[] = [];
    const seenNodes = new Set<string>([urn]);
    const queue = [urn]; // Note: uses array for queue, slow for large graphs
    while (queue.length > 0) {
        const current = queue.shift() as string; // Just checked length
        const children = Array.from(adjacencyList[direction].get(current) || []).sort(compareNodes);
        children?.forEach((child) => {
            if (!seenNodes.has(child)) {
                queue.push(child);
                seenNodes.add(child);
                orderedNodes.push(child);
            }
        });
    }
    return orderedNodes.map((id) => nodes.get(id)).filter((node): node is LineageEntity => !!node);
}

function compareNodes(a: string, b: string): number {
    const isATransformation = isUrnTransformational(a, globalEntityRegistryV2);
    const isBTransformation = isUrnTransformational(b, globalEntityRegistryV2);
    if (isATransformation && !isBTransformation) return 1;
    if (!isATransformation && isBTransformation) return -1;
    return a.localeCompare(b);
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
            if (!child || seen.has(childUrn) || child.entity?.status?.removed) return;

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
