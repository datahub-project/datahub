import {
    GraphStoreFields,
    LineageEntity,
    LineageFilter,
    LineageNode,
    LineageToggles,
    NodeContext,
} from '@app/lineageV3/common';
import hideNodes, { HideNodesConfig } from '@app/lineageV3/useComputeGraph/filterNodes';
import getDisplayedNodes from '@app/lineageV3/useComputeGraph/getDisplayedNodes';
import orderNodes from '@app/lineageV3/useComputeGraph/orderNodes';

import { LineageDirection } from '@types';

interface Options {
    /** Removes nodes from the graph entirely, before display rules are applied. */
    nodeFilter?: (node: LineageEntity) => boolean;
    /** Transforms the displayed nodes before they are returned, e.g. to limit nodes per level. */
    transformDisplayedNodes?: (displayedNodes: LineageNode[]) => LineageNode[];
    /** Overrides the default ordering of a node's children. Order determines priority:
     * pagination keeps the first children when there are too many to show. */
    compareNodes?: (a: string, b: string) => number;
    /** Nodes to seed the traversal with, e.g. a data product's members, since the root has no
     * lineage of its own. */
    seedNodes?: LineageEntity[];
    /** If false, lineage filter nodes are not included in the displayed nodes;
     * their state is still returned via `lineageFilters`. Defaults to true. */
    createFilterNodes?: boolean;
}

export interface LineageGraph {
    /** Graph store with hidden nodes removed, edges connected through them. */
    newGraphStore: Pick<NodeContext, GraphStoreFields | 'rootType'>;
    /** BFS order of each node from the root: positive downstream, negative upstream, 0 at the root. */
    orderIndices: Record<string, number>;
    /** The root node, if present in the graph. */
    roots: LineageEntity[];
    /** Nodes to display, including lineage filter nodes for filtered-out sections. */
    displayedNodes: LineageNode[];
    /** Each displayed node's non-transformational parents. */
    parents: Map<string, Set<string>>;
    /** Pagination state for each node and direction with filtered-out children, keyed by
     * `createLineageFilterNodeId`. Populated whether or not filter nodes are displayed. */
    lineageFilters: Map<string, LineageFilter>;
}

/**
 * Computes the nodes and edges to render for the lineage graph around a given entity:
 * 1. Orders nodes in BFS order from the root (`orderNodes`)
 * 2. Removes nodes hidden by the `LineageToggles`, connecting edges through them (`hideNodes`)
 * 3. Selects nodes to display based on each node's filters, fetchStatus, and isExpanded state,
 *    instantiating lineage filter nodes for filtered-out sections (`getDisplayedNodes`)
 *
 * Positioning is left to the caller, via a NodeBuilder over the returned graph.
 *
 * @param urn The focused or "home" entity's urn
 * @param context LineageNodesContext that represents the current state of the graph
 * @param ignoreSchemaFieldStatus Whether to ignore schema field status when computing the graph
 * @param options Optional node filter and displayed node transformation
 * @returns The graph store post-hiding, node order indices, and the root, displayed nodes, and
 *          parents with which to construct a NodeBuilder
 */
export default function computeLineageGraph(
    urn: string,
    context: Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>,
    ignoreSchemaFieldStatus: boolean,
    { nodeFilter, transformDisplayedNodes, compareNodes, seedNodes, createFilterNodes }: Options = {},
): LineageGraph {
    const { nodes, edges, adjacencyList, rootType, hideTransformations, showDataProcessInstances, showGhostEntities } =
        context;
    const graphStore: Pick<NodeContext, GraphStoreFields | 'rootType'> = { nodes, edges, adjacencyList, rootType };

    // Computed before nodes are hidden by `hideNodes`, to keep node order consistent.
    // Includes nodes that will be hidden, but they'll be filtered out by `getDisplayedNodes`.
    const orderNodesOptions = { compareNodes, seedNodes };
    const orderedNodes = {
        [LineageDirection.Upstream]: orderNodes(urn, LineageDirection.Upstream, graphStore, orderNodesOptions),
        [LineageDirection.Downstream]: orderNodes(urn, LineageDirection.Downstream, graphStore, orderNodesOptions),
    };

    const config: HideNodesConfig = {
        hideTransformations,
        hideDataProcessInstances: !showDataProcessInstances,
        hideGhostEntities: !showGhostEntities,
        ignoreSchemaFieldStatus,
    };
    const newGraphStore = { ...hideNodes(urn, rootType, config, graphStore, nodeFilter), rootType };
    console.debug(newGraphStore);

    const { displayedNodes, parents, lineageFilters } = getDisplayedNodes(urn, orderedNodes, newGraphStore, {
        seedNodes,
        createFilterNodes,
    });
    const rootNode = nodes.get(urn);
    const finalDisplayedNodes = transformDisplayedNodes ? transformDisplayedNodes(displayedNodes) : displayedNodes;

    const orderIndices = {
        [urn]: 0,
        ...Object.fromEntries(orderedNodes[LineageDirection.Downstream].map((e, idx) => [e.id, idx + 1])),
        ...Object.fromEntries(orderedNodes[LineageDirection.Upstream].map((e, idx) => [e.id, -idx - 1])),
    };
    return {
        newGraphStore,
        orderIndices,
        roots: rootNode ? [rootNode] : [],
        displayedNodes: finalDisplayedNodes,
        parents,
        lineageFilters,
    };
}
