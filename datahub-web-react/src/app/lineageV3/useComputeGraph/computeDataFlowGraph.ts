import type { XYPosition } from '@reactflow/core/dist/esm/types';
import { Edge, Node } from 'reactflow';

import { DATA_FLOW_LINEAGE_CONTROL_NODE_NAME } from '@app/lineageV3/LineageBoundingBoxNode/DataFlowLineageControlNode';
import {
    BOUNDING_BOX_HANDLE_Y,
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { CUSTOM_SMOOTH_STEP_EDGE_NAME } from '@app/lineageV3/LineageEdge/CustomSmoothStepEdge';
import { DATA_JOB_INPUT_OUTPUT_EDGE_NAME } from '@app/lineageV3/LineageEdge/DataJobInputOutputEdge';
import {
    GraphStoreFields,
    LINEAGE_HANDLE_OFFSET,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageEdgeData,
    LineageFilter,
    LineageTableEdgeData,
    LineageToggles,
    NodeContext,
    VERTICAL_HANDLE,
    buildHighlightAdjacencyList,
} from '@app/lineageV3/common';
import NodeBuilder, { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeConnectedComponents from '@app/lineageV3/useComputeGraph/computeConnectedComponents';
import computeImpactAnalysisGraph from '@app/lineageV3/useComputeGraph/computeImpactAnalysisGraph';
import hideNodes, { HideNodesConfig } from '@app/lineageV3/useComputeGraph/filterNodes';
import { EntityRegistry } from '@src/entityRegistryContext';

import { EntityType, LineageDirection } from '@types';

// Node height used for data flow graph layout. Preserved from the former lineageV2 node height
// so this layout stays unchanged after the lineageV2 removal.
const LINEAGE_NODE_HEIGHT = 80;
const ROOT_SEPARATION = 50 + LINEAGE_NODE_HEIGHT;

/**
 * Computes the data flow graph, in four sections:
 * 1. All data jobs of the data flow are rendered, arranged vertically
 * 2. The data flow bounding box is added, which contains all data jobs
 * 3. For each data job, we can render its upstream and downstream entities left and right,
 *    based on normal expansion rules as determined by `computeImpactAnalysisGraph`
 * 4. The data flow's own upstream and downstream entities are added beside the box
 * @param urn urn of the data flow
 * @param type type of the data flow
 * @param context LineageNodesContext that represents the current state of the graph
 * @param entityRegistry Resolves urns to entity types, to identify transformations and queries
 * @param ignoreSchemaFieldStatus Whether to ignore schema field status when computing the graph
 * @param showFilterNodes Whether to render lineage filter nodes
 * @returns An object containing:
 *   flowNodes: Nodes for React Flow to render
 *   flowEdges: Edges for React Flow to render
 *   resetPositions: Whether the positions of existing nodes should be reset
 */
export default function computeDataFlowGraph(
    urn: string,
    type: EntityType,
    context: Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>,
    entityRegistry: EntityRegistry,
    ignoreSchemaFieldStatus: boolean,
    showFilterNodes = true,
) {
    const lineageFilters = new Map<string, LineageFilter>();
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
        ...hideNodes(urn, rootType, entityRegistry, config, graphStore, (node) => node.parentDataJob === urn),
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

    console.debug({ isOnEdgeOfBox, displayedNodesByRoots });
    displayedNodesByRoots
        .sort(([_rootsA, componentA], [_rootsB, componentB]) => componentB.length - componentA.length)
        .forEach(([roots, displayedNodes]) => {
            const maxY = flowNodes.reduce((max, node) => Math.max(max, node.position.y), 0);
            const offset: [number, number] = [0, flowNodes.length ? maxY + ROOT_SEPARATION : 0];
            const offsets = new Map([[undefined, offset]]);
            const nodeBuilder = new NodeBuilder(urn, type, roots, displayedNodes, parents, false);
            const newFlowNodes = nodeBuilder.createNodes(newGraphStore, ignoreSchemaFieldStatus, offsets, urn);
            flowNodes.push(...newFlowNodes);
            flowEdges.push(
                ...nodeBuilder.createEdges(newGraphStore.edges, VERTICAL_HANDLE, CUSTOM_SMOOTH_STEP_EDGE_NAME),
            );

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

    const isInDataFlowGraphInterior: Map<string, Map<LineageDirection, boolean>> = new Map();
    // The flow's node must always be the box, even before child jobs load: React Flow can't switch a node
    // id from an entity node to the box, and the flow's own lineage can arrive before its jobs
    const boundingBox = addBoundingBoxDataFlow(flowNodes, { urn, type, entity: nodes.get(urn)?.entity });
    const seenNodeIds = new Set(flowNodes.map((n) => n.id));
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
        isInDataFlowGraphInterior.set(
            node.urn,
            new Map(
                Object.values(LineageDirection).map((direction) => [
                    direction,
                    !isOnEdgeOfBox[direction].get(node.urn),
                ]),
            ),
        );

        if (node.isExpanded.UPSTREAM || node.isExpanded.DOWNSTREAM) {
            const {
                flowNodes: newFlowNodes,
                flowEdges: newFlowEdges,
                lineageFilters: newLineageFilters,
            } = computeImpactAnalysisGraph(
                node.urn,
                node.type,
                context,
                entityRegistry,
                ignoreSchemaFieldStatus,
                undefined,
                offsets,
                (n) => n.urn === node.urn || n.parentDataJob !== urn,
                undefined,
                showFilterNodes,
            );
            newLineageFilters.forEach((filter, id) => lineageFilters.set(id, filter));
            newFlowNodes
                .filter((n) => n.id !== node.urn && nodes.get(n.id)?.parentDataJob !== urn && !seenNodeIds.has(n.id))
                .forEach((n) => {
                    seenNodeIds.add(n.id);
                    flowNodes.push(n);
                });
            flowEdges.push(
                ...newFlowEdges.filter(
                    (e) => !nodes.get(e.source)?.parentDataJob && !nodes.get(e.target)?.parentDataJob,
                ),
            );
        }
    });

    addFlowOwnLineage(
        { urn, type, context, entityRegistry, ignoreSchemaFieldStatus, showFilterNodes },
        { boundingBox, seenNodeIds, flowNodes, flowEdges, lineageFilters },
    );

    const extraEdgeData = (source: string, target: string): Partial<LineageEdgeData> => {
        const upstream = nodes.get(source);
        const downstream = nodes.get(target);

        if (
            !upstream ||
            !downstream ||
            (!upstream.parentDataJob && !downstream.parentDataJob) ||
            (upstream.parentDataJob && downstream.parentDataJob)
        ) {
            return { hide: true };
        }
        if (
            downstream.parentDataJob &&
            upstream.direction === LineageDirection.Upstream &&
            downstream.isExpanded[LineageDirection.Upstream]
        ) {
            // External upstream -> data flow
            return {
                isInInterior: isInDataFlowGraphInterior.get(downstream.urn)?.get(LineageDirection.Upstream),
                direction: LineageDirection.Upstream,
            };
        }
        if (
            upstream.parentDataJob &&
            downstream.direction === LineageDirection.Downstream &&
            upstream.isExpanded[LineageDirection.Downstream]
        ) {
            // Data flow -> external downstream
            return {
                isInInterior: isInDataFlowGraphInterior.get(upstream.urn)?.get(LineageDirection.Downstream),
                direction: LineageDirection.Downstream,
            };
        }
        if (
            downstream.parentDataJob &&
            upstream.direction === LineageDirection.Downstream &&
            (upstream.isExpanded[LineageDirection.Downstream] || downstream.isExpanded[LineageDirection.Upstream])
        ) {
            // External downstream -> data flow (cycle)
            return {
                direction: LineageDirection.Downstream,
                isToDataFlow: true,
            };
        }
        if (
            upstream.parentDataJob &&
            downstream.direction === LineageDirection.Upstream &&
            (upstream.isExpanded[LineageDirection.Downstream] || downstream.isExpanded[LineageDirection.Upstream])
        ) {
            // Data flow -> external upstream (cycle)
            return {
                direction: LineageDirection.Upstream,
                isToDataFlow: true,
            };
        }
        return { hide: true };
    };

    // Add edges between external upstream / downstream entities and data jobs in data flow
    // Do not add any nodes from this step
    const nodeBuilder = new NodeBuilder(urn, type, [], Array.from(nodes.values()), new Map(), false);
    flowEdges.push(
        ...nodeBuilder
            .createEdges(edges, undefined, DATA_JOB_INPUT_OUTPUT_EDGE_NAME, extraEdgeData)
            .filter((e) => !e.data?.hide),
    );
    // Note: `newGraphStore` is scoped to this flow's jobs, so the full adjacency is restricted to
    // shown nodes for highlighting instead
    return {
        flowNodes,
        flowEdges,
        resetPositions: false,
        lineageFilters,
        adjacencyList: buildHighlightAdjacencyList(edges, new Set(flowNodes.map((node) => node.id))),
    };
}

/**
 * Adds the DataFlow's own lineage beside its bounding box, in the same columns as its jobs' lineage,
 * plus the box's expand and pagination controls for it. Each side is centered on the box's title,
 * or placed above that side's job neighbors when centering would overlap them.
 */
function addFlowOwnLineage(
    graph: {
        urn: string;
        type: EntityType;
        context: Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>;
        entityRegistry: EntityRegistry;
        ignoreSchemaFieldStatus: boolean;
        showFilterNodes: boolean;
    },
    layout: {
        boundingBox: Node;
        seenNodeIds: Set<string>;
        flowNodes: LineageVisualizationNode[];
        flowEdges: Edge<LineageTableEdgeData>[];
        lineageFilters: Map<string, LineageFilter>;
    },
) {
    const { urn, type, context } = graph;
    const { boundingBox, seenNodeIds, flowNodes, flowEdges, lineageFilters } = layout;
    const directions = Object.values(LineageDirection).filter((d) => context.adjacencyList[d].get(urn)?.size);
    if (!directions.length) return;

    const own = computeImpactAnalysisGraph(
        urn,
        type,
        context,
        graph.entityRegistry,
        graph.ignoreSchemaFieldStatus,
        undefined,
        undefined,
        (n) => n.urn === urn || n.parentDataJob !== urn,
        undefined,
        graph.showFilterNodes,
    );
    own.lineageFilters.forEach((filter, id) => lineageFilters.set(id, filter));

    const rootX = own.flowNodes.find((n) => n.id === urn)?.position.x ?? 0;
    const boxWidth = boundingBox.width ?? LINEAGE_NODE_WIDTH;
    const external = own.flowNodes.filter((n) => n.id !== urn && !seenNodeIds.has(n.id));
    [true, false].forEach((isUpstream) => {
        const side = external.filter((n) => n.position.x < rootX === isUpstream);
        if (!side.length) return;

        const occupiedYs = flowNodes
            .filter((n) => n.id !== urn && !context.nodes.get(n.id)?.parentDataJob)
            .filter((n) => (isUpstream ? n.position.x < 0 : n.position.x >= boxWidth))
            .map((n) => n.position.y);
        const sideMinY = Math.min(...side.map((n) => n.position.y));
        const sideMaxY = Math.max(...side.map((n) => n.position.y));
        const dx = (isUpstream ? 0 : boxWidth - LINEAGE_NODE_WIDTH) - rootX;
        const centeredDy = BOUNDING_BOX_HANDLE_Y - LINEAGE_HANDLE_OFFSET - (sideMaxY - sideMinY) / 2 - sideMinY;
        const dy = occupiedYs.length
            ? Math.min(centeredDy, Math.min(...occupiedYs) - ROOT_SEPARATION - sideMaxY)
            : centeredDy;
        side.forEach((n) => {
            seenNodeIds.add(n.id);
            // eslint-disable-next-line no-param-reassign
            n.position = { x: n.position.x + dx, y: n.position.y + dy };
        });
        flowNodes.push(...side);
    });

    const edgeIds = new Set(flowEdges.map((e) => e.id));
    flowEdges.push(...own.flowEdges.filter((e) => !edgeIds.has(e.id)));

    directions.forEach((direction) =>
        flowNodes.push({
            id: `${urn}-lineage-control-${direction}`,
            type: DATA_FLOW_LINEAGE_CONTROL_NODE_NAME,
            parentId: urn,
            position: { x: 0, y: BOUNDING_BOX_HANDLE_Y },
            data: { urn, type, direction },
            selectable: false,
            draggable: false,
        }),
    );
}

function addBoundingBoxDataFlow(
    flowNodes: LineageVisualizationNode[],
    data: Pick<LineageBoundingBox, 'urn' | 'type' | 'entity'>,
): Node<LineageBoundingBox> {
    const xs = flowNodes.map((node) => node.position.x);
    const ys = flowNodes.map((node) => node.position.y);
    const [minX, maxX] = xs.length ? [Math.min(...xs), Math.max(...xs)] : [0, 0];
    const [minY, maxY] = ys.length ? [Math.min(...ys), Math.max(...ys)] : [0, 0];

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
    const xOffset = direction === LineageDirection.Upstream ? 0 : (boundingBox?.width || 0) - LINEAGE_NODE_WIDTH;
    const yOffset = (ROOT_SEPARATION / 3) * (direction === LineageDirection.Upstream ? -1 : 1);

    return [xOffset, (position?.y || 0) + (isOnEdge ? 0 : yOffset)];
}
