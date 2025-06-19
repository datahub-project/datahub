import { GraphStoreFields, LineageEntity, LineageToggles, NodeContext } from '@app/lineageV3/common';
import NodeBuilder from '@app/lineageV3/useComputeGraph/NodeBuilder';
import hideNodes, { HideNodesConfig } from '@app/lineageV3/useComputeGraph/filterNodes';
import getDisplayedNodes from '@app/lineageV3/useComputeGraph/getDisplayedNodes';
import orderNodes from '@app/lineageV3/useComputeGraph/orderNodes';

import { EntityType, LineageDirection } from '@types';

/**
 * Computes an "impact analysis" graph, which shows the upstream and downstream entities of a given entity.
 *
 * Traverses the graph from the home node, upstream and downstream, deciding whether to continue based on each node's
 * properties like filters, fetchStatus, isExpanded. If nodes are filtered out, a LineageFilterNode is instantiated
 * in their place.
 *
 * Supports removing nodes from the graph, either truncating the graph or connecting the graph through the removed nodes,
 * as specified by the `LineageToggles`. Also supports removing edges from the graph, via the edge's `isDisplayed` property.
 *
 * @param urn The focused or "home" entity's urn
 * @param type The type of the home entity
 * @param context LineageNodesContext that represents the current state of the graph
 * @param ignoreSchemaFieldStatus Whether to ignore schema field status when computing the graph
 * @param prevHideTransformations Previous value of `hideTransformations` to determine if the graph should be reset
 * @param offsets Map of offsets for each direction, used to position nodes in the graph when called to render a subgraph
 * @param nodeFilter Optional filter function to apply to nodes before processing them.
 * @returns An object containing:
 *   flowNodes: Nodes for React Flow to render
 *   flowEdges: Edges for React Flow to render
 *   resetPositions: Whether the positions of existing nodes should be reset
 */
export default function computeImpactAnalysisGraph(
    urn: string,
    type: EntityType,
    context: Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>,
    ignoreSchemaFieldStatus: boolean,
    prevHideTransformations?: boolean,
    offsets: Map<LineageDirection, [number, number]> = new Map(),
    nodeFilter?: (node: LineageEntity) => boolean,
) {
    const { nodes, edges, adjacencyList, rootType, hideTransformations, showDataProcessInstances, showGhostEntities } =
        context;
    const graphStore: Pick<NodeContext, GraphStoreFields | 'rootType'> = { nodes, edges, adjacencyList, rootType };
    console.debug(graphStore);

    // Computed before nodes are hidden by `hideNodes`, to keep node order consistent.
    // Includes nodes that will be hidden, but they'll be filtered out by `getDisplayedNodes`.
    const orderedNodes = {
        [LineageDirection.Upstream]: orderNodes(urn, LineageDirection.Upstream, graphStore),
        [LineageDirection.Downstream]: orderNodes(urn, LineageDirection.Downstream, graphStore),
    };

    const config: HideNodesConfig = {
        hideTransformations,
        hideDataProcessInstances: !showDataProcessInstances,
        hideGhostEntities: !showGhostEntities,
        ignoreSchemaFieldStatus,
    };
    const newGraphStore = { ...hideNodes(urn, rootType, config, graphStore, nodeFilter), rootType };
    console.debug(newGraphStore);

    const { displayedNodes, parents } = getDisplayedNodes(urn, orderedNodes, newGraphStore);
    const rootNode = nodes.get(urn);
    const nodeBuilder = new NodeBuilder(urn, type, rootNode ? [rootNode] : [], displayedNodes, parents);

    const orderIndices = {
        [urn]: 0,
        ...Object.fromEntries(orderedNodes[LineageDirection.Downstream].map((e, idx) => [e.id, idx + 1])),
        ...Object.fromEntries(orderedNodes[LineageDirection.Upstream].map((e, idx) => [e.id, -idx - 1])),
    };
    return {
        flowNodes: nodeBuilder
            .createNodes(newGraphStore, ignoreSchemaFieldStatus, offsets)
            .sort((a, b) => (orderIndices[a.id] || 0) - (orderIndices[b.id] || 0)),
        flowEdges: nodeBuilder.createEdges(newGraphStore.edges),
        resetPositions: prevHideTransformations !== undefined && prevHideTransformations !== hideTransformations,
    };
}
