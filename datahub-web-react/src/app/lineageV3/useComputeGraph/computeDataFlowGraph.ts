import type { XYPosition } from '@reactflow/core/dist/esm/types';
import { Edge, Node } from 'reactflow';

import { LINEAGE_NODE_HEIGHT } from '@app/lineageV2/LineageEntityNode/useDisplayedColumns';
import {
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import {
    GraphStoreFields,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageTableEdgeData,
    LineageToggles,
    NodeContext,
    VERTICAL_HANDLE,
} from '@app/lineageV3/common';
import NodeBuilder, { LineageVisualizationNode, MAIN_X_SEP_RATIO } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeConnectedComponents from '@app/lineageV3/useComputeGraph/computeConnectedComponents';
import computeImpactAnalysisGraph from '@app/lineageV3/useComputeGraph/computeImpactAnalysisGraph';
import hideNodes, { HideNodesConfig } from '@app/lineageV3/useComputeGraph/filterNodes';

import { EntityType, LineageDirection } from '@types';

const ROOT_SEPARATION = 50 + LINEAGE_NODE_HEIGHT;
const INPUT_OUTPUT_SEPARATION = 80;

/**
 * Computes the data flow graph, in three sections:
 * 1. All data jobs of the data flow are rendered, arranged vertically
 * 2. The data flow bounding box is added, which contains all data jobs
 * 3. For each data job, we can render its upstream and downstream entities left and right,
 *    based on normal expansion rules as determined by `computeImpactAnalysisGraph`
 * @param urn urn of the data flow
 * @param type type of the data flow
 * @param context LineageNodesContext that represents the current state of the graph
 * @param ignoreSchemaFieldStatus Whether to ignore schema field status when computing the graph
 * @returns An object containing:
 *   flowNodes: Nodes for React Flow to render
 *   flowEdges: Edges for React Flow to render
 *   resetPositions: Whether the positions of existing nodes should be reset
 */
export default function computeDataFlowGraph(
    urn: string,
    type: EntityType,
    context: Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>,
    ignoreSchemaFieldStatus: boolean,
) {
    const { nodes, edges, adjacencyList, rootType, showDataProcessInstances, showGhostEntities } = context;
    const graphStore = { nodes, edges, adjacencyList };
    console.debug(graphStore);

    const config: HideNodesConfig = {
        hideTransformations: false,
        hideDataProcessInstances: !showDataProcessInstances,
        hideGhostEntities: !showGhostEntities,
        ignoreSchemaFieldStatus,
    };
    const newGraphStore = {
        ...hideNodes(urn, rootType, config, graphStore, (node) => node.parentDataJob === urn),
        rootType,
    };
    console.debug(newGraphStore);

    const { displayedNodesByRoots, parents } = computeConnectedComponents(newGraphStore);
    const flowNodes: LineageVisualizationNode[] = [];
    const flowEdges: Edge<LineageTableEdgeData>[] = [];
    const dataJobPositions = new Map<string, XYPosition>();
    const isOnLeftSideOfBox = new Map<string, boolean>();
    const isOnRightSideOfBox = new Map<string, boolean>();
    const isOnEdgeOfBox = {
        [LineageDirection.Upstream]: isOnLeftSideOfBox,
        [LineageDirection.Downstream]: isOnRightSideOfBox,
    };

    console.debug({ displayedNodesByRoots });
    displayedNodesByRoots
        .sort(([_rootsA, componentA], [_rootsB, componentB]) => componentB.length - componentA.length)
        .forEach(([roots, displayedNodes]) => {
            const maxY = flowNodes.reduce((max, node) => Math.max(max, node.position.y), 0);
            const offset: [number, number] = [0, flowNodes.length ? maxY + ROOT_SEPARATION : 0];
            const offsets = new Map([[undefined, offset]]);
            const nodeBuilder = new NodeBuilder(urn, type, roots, displayedNodes, parents, false);
            const newFlowNodes = nodeBuilder.createNodes(newGraphStore, ignoreSchemaFieldStatus, offsets, urn);
            flowNodes.push(...newFlowNodes);
            flowEdges.push(...nodeBuilder.createEdges(newGraphStore.edges, offsets, VERTICAL_HANDLE));

            newFlowNodes.forEach((node) => {
                dataJobPositions.set(node.id, node.position);
            });

            nodeBuilder.layerNodes.forEach((layer) => {
                const layerY = Array.from(layer)
                    .map((u) => nodeBuilder.nodeInformation[u].y)
                    .filter((y): y is number => y !== undefined);
                const layerMin = Math.min(...layerY);
                const layerMax = Math.max(...layerY);
                layer.forEach((u) => {
                    if (nodeBuilder.nodeInformation[u].y === layerMin) {
                        isOnLeftSideOfBox.set(u, true);
                    }
                    if (nodeBuilder.nodeInformation[u].y === layerMax) {
                        isOnRightSideOfBox.set(u, true);
                    }
                });
            });
        });

    if (flowNodes.length) {
        const boundingBox = addBoundingBoxDataFlow(flowNodes, { urn, type, entity: nodes.get(urn)?.entity });
        newGraphStore.nodes.forEach((node) => {
            const offsets: Map<LineageDirection, [number, number]> = new Map(
                Object.values(LineageDirection).map((direction) => [
                    direction,
                    computeInputOutputOffset(
                        direction,
                        dataJobPositions.get(node.urn),
                        boundingBox,
                        isOnEdgeOfBox[direction].get(node.urn) || false,
                    ),
                ]),
            );

            const { flowNodes: newFlowNodes, flowEdges: newFlowEdges } = computeImpactAnalysisGraph(
                node.urn,
                node.type,
                context,
                ignoreSchemaFieldStatus,
                undefined,
                offsets,
                true,
            );
            flowNodes.push(...newFlowNodes.filter((n) => n.id !== node.urn && nodes.get(n.id)?.parentDataJob !== urn));
            flowEdges.push(
                ...newFlowEdges.filter(
                    (e) =>
                        !nodes.get(e.source)?.parentDataJob ||
                        nodes.get(e.source)?.parentDataJob !== nodes.get(e.target)?.parentDataJob,
                ),
            );
        });
    }
    return { flowNodes, flowEdges, resetPositions: false };
}

function addBoundingBoxDataFlow(
    flowNodes: LineageVisualizationNode[],
    data: Pick<LineageBoundingBox, 'urn' | 'type' | 'entity'>,
): Node<LineageBoundingBox> {
    const maxX = Math.max(...flowNodes.map((node) => node.position.x));
    const minX = Math.min(...flowNodes.map((node) => node.position.x));
    const maxY = Math.max(...flowNodes.map((node) => node.position.y));
    const minY = Math.min(...flowNodes.map((node) => node.position.y));

    const width = LINEAGE_NODE_WIDTH + maxX - minX + BOUNDING_BOX_PADDING * 2;
    const height = LINEAGE_NODE_HEIGHT + maxY - minY + BOUNDING_BOX_PADDING * 2;

    flowNodes.forEach((node) => {
        /* eslint-disable no-param-reassign */
        node.position.x += BOUNDING_BOX_PADDING - minX;
        node.position.y += BOUNDING_BOX_PADDING;
        /* eslint-enable no-param-reassign */
    });

    const node = {
        id: data.urn,
        type: LINEAGE_BOUNDING_BOX_NODE_NAME,
        position: { x: 0, y: 0 },
        data,
        selectable: true,
        draggable: true,
        style: { width, height, zIndex: -2 },
        width,
        height,
    };
    flowNodes.unshift(node);
    return node;
}

function computeInputOutputOffset(
    direction: LineageDirection,
    position: XYPosition | undefined,
    boundingBox: Node,
    isOnEdge: boolean,
): [number, number] {
    const xOffset =
        direction === LineageDirection.Upstream
            ? LINEAGE_NODE_WIDTH * MAIN_X_SEP_RATIO - INPUT_OUTPUT_SEPARATION
            : (boundingBox?.width || 0) -
              LINEAGE_NODE_WIDTH -
              LINEAGE_NODE_WIDTH * MAIN_X_SEP_RATIO +
              INPUT_OUTPUT_SEPARATION;
    const yOffset = (ROOT_SEPARATION / 3) * (direction === LineageDirection.Upstream ? -1 : 1);

    return [xOffset, (position?.y || 0) + (isOnEdge ? 0 : yOffset)];
}
