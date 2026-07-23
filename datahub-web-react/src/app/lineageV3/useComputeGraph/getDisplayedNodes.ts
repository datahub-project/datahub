import { SubType } from '@app/entityV2/shared/components/subtypes';
import {
    LINEAGE_FILTER_PAGINATION,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    LineageNode,
    NodeContext,
    createLineageFilterNodeId,
    getEdgeId,
    getParents,
    isDbt,
    isQuery,
    isTransformational,
    setDefault,
} from '@app/lineageV3/common';
import { ENTITY_SUB_TYPE_FILTER_NAME, FILTER_DELIMITER, PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';

import { LineageDirection } from '@types';

interface Output {
    displayedNodes: LineageNode[];
    parents: Map<string, Set<string>>;
    /** Pagination state for each node and direction with filtered-out children, keyed by
     * `createLineageFilterNodeId`. Populated whether or not filter nodes are displayed, so
     * pagination controls can render elsewhere (e.g. on the expand/contract buttons). */
    lineageFilters: Map<string, LineageFilter>;
}

interface Options {
    /** Nodes to seed the traversal with, displayed alongside the root and traversed further only if
     * they are themselves expanded. Lets a root with no lineage of its own (e.g. a data product)
     * seed the graph with its members. */
    seedNodes?: LineageEntity[];
    /** If false, lineage filter nodes are not included in the displayed nodes;
     * their state is still returned via `lineageFilters`. Defaults to true. */
    createFilterNodes?: boolean;
}

/**
 * Filters nodes based on per-node filters.
 * @param urn The urn of the root node.
 * @param orderedNodes Nodes ordered by `orderNodes`, in BFS order.
 * @param context Lineage node context.
 * @param options Optional seed nodes and filter node toggle.
 * @returns A list of nodes to display in rough topological order,
 *          a map of nodes to their non-transformational parents,
 *          and the pagination state of each node with filtered-out children.
 */
export default function getDisplayedNodes(
    urn: string,
    orderedNodes: Record<LineageDirection, LineageEntity[]>,
    context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges' | 'rootType'>,
    options: Options = {},
): Output {
    const parents = new Map<string, Set<string>>();
    const lineageFilters = new Map<string, LineageFilter>();

    const { nodes, rootType } = context;
    const rootNode = nodes.get(urn);
    if (!rootNode) {
        return { displayedNodes: [], parents, lineageFilters };
    }

    const displayedNodes: LineageNode[] = [rootNode];
    const addedNodes = new Set<string>([urn]);

    function traverseTree(direction: LineageDirection) {
        if (!rootNode?.isExpanded[direction]) {
            return;
        }
        const seenNodes = new Set<string>([urn]);
        const queue = [urn]; // Note: uses array for queue, slow for large graphs

        // Seed the traversal, e.g. with a data product's members, since the root has no lineage of
        // its own. Seeds are traversed further only if they are themselves expanded.
        options.seedNodes?.forEach((seed) => {
            if (seenNodes.has(seed.id)) return;
            seenNodes.add(seed.id);
            if (!addedNodes.has(seed.id)) {
                addedNodes.add(seed.id);
                displayedNodes.push(seed);
            }
            if (seed.isExpanded[direction]) {
                queue.push(seed.id);
            }
        });

        while (queue.length > 0) {
            const current = queue.shift() as string; // Just checked length
            const filteredChildren = applyFilters(
                current,
                direction,
                orderedNodes[direction],
                parents,
                lineageFilters,
                options.createFilterNodes ?? true,
                context,
            );
            filteredChildren.forEach((child) => {
                if (!seenNodes.has(child.id)) {
                    seenNodes.add(child.id);

                    if (!addedNodes.has(child.id)) {
                        addedNodes.add(child.id);
                        displayedNodes.push(child);
                        if (child?.inCycle) {
                            // Set direction deterministically, to first direction detected
                            // eslint-disable-next-line no-param-reassign
                            child.direction = direction;
                        }
                    }

                    if (!isTransformational(child, rootType) && child.isExpanded[direction]) {
                        queue.push(child.id);
                    }
                }
            });
        }
    }

    traverseTree(LineageDirection.Upstream);
    traverseTree(LineageDirection.Downstream);

    return { displayedNodes, parents, lineageFilters };
}

function applyFilters(
    urn: string,
    direction: LineageDirection,
    orderedNodes: LineageEntity[],
    parents: Map<string, Set<string>>,
    lineageFilters: Map<string, LineageFilter>,
    createFilterNodes: boolean,
    context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges' | 'rootType'>,
): LineageNode[] {
    const { adjacencyList, nodes } = context;
    const node = nodes.get(urn);
    const filters = node?.filters?.[direction];
    const children = adjacencyList[direction].get(urn);
    if (!node || !children?.size || filters?.display === false) {
        return [];
    }

    const { allChildren, childrenToFilter } = getChildrenToFilter(node, direction, context);
    const contents = orderedNodes.filter((n) => childrenToFilter?.has(n.urn));
    let filteredChildren = contents.slice();

    if (filters?.searchUrns) {
        filteredChildren = filteredChildren.filter(
            (n) =>
                filters.searchUrns?.has(n.urn) ||
                // Required until search results include schema fields
                (n.entity?.parent?.urn && filters.searchUrns?.has(n.entity.parent.urn)),
        );
    }

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
    // Children are ordered highest priority first, so pagination keeps the first `limit`
    const shownNodes = filteredChildren.slice(0, limit);
    const allShownNodes = [...getTransformationalNodes(node, shownNodes, direction, context), ...shownNodes];

    // Build parent map
    allShownNodes.forEach((child) => setDefault(parents, child.urn, new Set<string>()).add(urn));

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
            contents: contents.map((n) => n.urn),
            shown: new Set(allShownNodes.map((n) => n.urn)),
        };
        lineageFilters.set(filterNode.id, filterNode);
        if (createFilterNodes) {
            result.push(filterNode);
        }
    }
    if (node) {
        result.push(...allShownNodes);
    }

    return result;
}

/**
 * Returns the set of children to filter for the given parent node.
 * This is calculated as: all adjacent non-transformational nodes and any transformational leaves.
 * Drops DBT sources that are transformational leaves, because they add no information.
 * Loop invariant: all nodes in `queue` are transformational.
 * @param parent The parent node, whose children are to be filtered.
 * @param direction Direction of children.
 * @param context Lineage node context.
 */
function getChildrenToFilter(
    parent: LineageEntity,
    direction: LineageDirection,
    context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'rootType'>,
): {
    allChildren: Set<string>;
    childrenToFilter: Set<string>;
} {
    const { adjacencyList, nodes, rootType } = context;
    const seen = new Set<string>();
    const childrenToFilter = new Set<string>();
    const queue = [parent];
    for (let node = queue.pop(); node; node = queue.pop()) {
        const children = adjacencyList[direction].get(node.urn);
        // Include non-query transformational nodes if they have no children
        // Have to also include non-query transformational nodes in cycles with their parent, because
        // those are effectively leaves as well.
        if (
            (!children?.size || (node.inCycle && children.has(parent.urn))) &&
            !isQuery(node) &&
            !(direction === LineageDirection.Downstream && isDbt(node) && node.entity?.subtype === SubType.DbtSource)
        ) {
            childrenToFilter.add(node.urn);
        }
        children?.forEach((childUrn) => {
            const child = nodes.get(childUrn);
            if (!child || seen.has(childUrn)) return;

            if (isTransformational(child, rootType)) {
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
    context: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'edges' | 'rootType'>,
): LineageEntity[] {
    const { nodes, edges, adjacencyList, rootType } = context;

    const leafUrns = new Set<string>(leaves.map((leaf) => leaf.urn));
    const nodesInBetween = new Set<string>();
    const nodesToRoot = [...leaves];
    for (let node = nodesToRoot.pop(); node; node = nodesToRoot.pop()) {
        getParents(node, adjacencyList).forEach((parentUrn) => {
            const parent = nodes.get(parentUrn);
            if (
                parentUrn !== root.urn &&
                !nodesInBetween.has(parentUrn) &&
                parent &&
                isTransformational(parent, rootType)
            ) {
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
                if (edge?.via) {
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
