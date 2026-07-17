import {
    GraphStoreFields,
    LineageEntity,
    LineageNode,
    LineageToggles,
    NodeContext,
    buildHighlightAdjacencyList,
} from '@app/lineageV3/common';
import NodeBuilder from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeLineageGraph from '@app/lineageV3/useComputeGraph/computeLineageGraph';
import { limitNodesPerLevel } from '@app/lineageV3/useComputeGraph/limitNodes/limitNodesPerLevel';
import { LevelsInfo } from '@app/lineageV3/useComputeGraph/limitNodes/limitNodesUtils';

import { EntityType, LineageDirection } from '@types';

/**
 * Computes an "impact analysis" graph, which shows the upstream and downstream entities of a given entity.
 *
 * Delegates the graph computation to `computeLineageGraph`, positioning nodes with the standard
 * NodeBuilder, and limiting entity nodes per level in module view.
 *
 * @param urn The focused or "home" entity's urn
 * @param type The type of the home entity
 * @param context LineageNodesContext that represents the current state of the graph
 * @param ignoreSchemaFieldStatus Whether to ignore schema field status when computing the graph
 * @param prevHideTransformations Previous value of `hideTransformations` to determine if the graph should be reset
 * @param offsets Map of offsets for each direction, used to position nodes in the graph when called to render a subgraph
 * @param nodeFilter Optional filter function to apply to nodes before processing them.
 * @param isModuleView Optional boolean parameter if it is module view
 * @param showFilterNodes Whether to render lineage filter nodes; if false, their pagination state
 *        is only returned via `lineageFilters`
 * @returns An object containing:
 *   flowNodes: Nodes for React Flow to render
 *   flowEdges: Edges for React Flow to render
 *   resetPositions: Whether the positions of existing nodes should be reset
 *   lineageFilters: Pagination state of each node with filtered-out children
 */
export default function computeImpactAnalysisGraph(
    urn: string,
    type: EntityType,
    context: Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>,
    ignoreSchemaFieldStatus: boolean,
    prevHideTransformations?: boolean,
    offsets: Map<LineageDirection, [number, number]> = new Map(),
    nodeFilter?: (node: LineageEntity) => boolean,
    isModuleView?: boolean,
    showFilterNodes = true,
) {
    const { adjacencyList, rootType, hideTransformations } = context;

    let levelsInfo: LevelsInfo = {};
    let levelsMap = new Map<string, number>();
    // Limit entity nodes per level in module view
    const transformDisplayedNodes = isModuleView
        ? (displayedNodes: LineageNode[]) => {
              const result = limitNodesPerLevel({
                  nodes: displayedNodes,
                  rootUrn: urn,
                  rootType,
                  adjacencyList,
                  maxPerLevel: 2,
              });
              levelsInfo = result.levelsInfo;
              levelsMap = result.levelsMap;
              return result.limitedNodes;
          }
        : undefined;

    const { newGraphStore, orderIndices, roots, displayedNodes, parents, lineageFilters } = computeLineageGraph(
        urn,
        context,
        ignoreSchemaFieldStatus,
        { nodeFilter, transformDisplayedNodes, createFilterNodes: showFilterNodes },
    );

    const builder = new NodeBuilder(urn, type, roots, displayedNodes, parents);
    const flowNodes = builder
        .createNodes(newGraphStore, ignoreSchemaFieldStatus, offsets)
        .sort((a, b) => (orderIndices[a.id] || 0) - (orderIndices[b.id] || 0));

    return {
        flowNodes,
        flowEdges: builder.createEdges(newGraphStore.edges),
        resetPositions: prevHideTransformations !== undefined && prevHideTransformations !== hideTransformations,
        levelsInfo,
        levelsMap,
        lineageFilters,
        // For node highlighting: only shown nodes, with edges connected through toggle-hidden nodes
        adjacencyList: buildHighlightAdjacencyList(newGraphStore.edges, new Set(displayedNodes.map((n) => n.id))),
    };
}
