import type { XYPosition } from '@reactflow/core/dist/esm/types';
import { Edge, Node } from 'reactflow';

import {
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { CUSTOM_SMOOTH_STEP_EDGE_NAME } from '@app/lineageV3/LineageEdge/CustomSmoothStepEdge';
import { DATA_JOB_INPUT_OUTPUT_EDGE_NAME } from '@app/lineageV3/LineageEdge/DataJobInputOutputEdge';
import {
    GraphStoreFields,
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

import { EntityType, LineageDirection } from '@types';

// Node height used for data flow graph layout. Preserved from the former lineageV2 node height
// so this layout stays unchanged after the lineageV2 removal.
const LINEAGE_NODE_HEIGHT = 80;
const ROOT_SEPARATION = 50 + LINEAGE_NODE_HEIGHT;

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
        ...hideNodes(urn, rootType, config, graphStore, (node) => node.parentDataJob === urn),
        rootType,
    };
    console.debug(newGraphStore);

    if (newGraphStore.nodes.size === 0) {
        // No child DataJobs at all -- the DataFlow's own direct upstream/downstream (e.g. a linked
        // source-code Repository via repositoryLineage) IS the entire graph, rendered the same way
        // any other single entity's lineage is rendered.
        return computeRootOwnLineage(urn, type, context, ignoreSchemaFieldStatus);
    }

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
    if (flowNodes.length) {
        const boundingBox = addBoundingBoxDataFlow(flowNodes, { urn, type, entity: nodes.get(urn)?.entity });
        const seenNodeIds = new Set(flowNodes.map((n) => n.id));

        // The DataFlow itself may also have its own direct upstream/downstream (e.g. a linked
        // Repository via repositoryLineage), independent of any child job's own lineage. The loop
        // below only ever computes lineage for child jobs (`newGraphStore.nodes`), so without this,
        // a DataFlow's own edge would never render once it has at least one child job.
        //
        // Positions are computed here directly rather than via computeImpactAnalysisGraph's own
        // layer-based x positioning, which has a homeType === DataFlow special case (see
        // NodeBuilder.computeNodeX) built for laying out child jobs relative to each other, not for
        // a DataFlow's own upstream/downstream -- reusing it here places these nodes inside the
        // box's own coordinate range instead of beside it: upstream to the box's left, downstream to
        // its right, stacked vertically and centered on the box, matching how the per-job external
        // entities below are positioned relative to it.
        const { flowNodes: rootFlowNodes, flowEdges: rootFlowEdges } = computeRootOwnLineage(
            urn,
            type,
            context,
            ignoreSchemaFieldStatus,
        );
        // The box itself (id === urn) already represents the DataFlow node -- only merge in its
        // external neighbors, deduplicated against anything a child job's own lineage also reaches
        // (e.g. the same Repository linked to both the DataFlow and one of its jobs).
        const rootExternalNodes = rootFlowNodes.filter((n) => n.id !== urn && !seenNodeIds.has(n.id));
        rootExternalNodes.forEach((n) => seenNodeIds.add(n.id));
        // `.data.direction` is only ever set for cyclic nodes (see getDisplayedNodes.ts), so it's
        // undefined for an ordinary single-direction neighbor like this. Direction relative to the
        // DataFlow itself has to be read directly from the adjacency list instead.
        [LineageDirection.Upstream, LineageDirection.Downstream].forEach((direction) => {
            const neighborUrnsInDirection = adjacencyList[direction].get(urn);
            const nodesInDirection = rootExternalNodes.filter((n) => neighborUrnsInDirection?.has(n.id));
            const x =
                direction === LineageDirection.Upstream
                    ? -(LINEAGE_NODE_WIDTH + ROOT_SEPARATION)
                    : (boundingBox.width ?? LINEAGE_NODE_WIDTH) + ROOT_SEPARATION;
            const startY =
                (boundingBox.height ?? LINEAGE_NODE_HEIGHT) / 2 - ((nodesInDirection.length - 1) * ROOT_SEPARATION) / 2;
            nodesInDirection.forEach((n, i) => {
                // eslint-disable-next-line no-param-reassign
                n.position = { x, y: startY + i * ROOT_SEPARATION };
            });
        });
        flowNodes.push(...rootExternalNodes);
        flowEdges.push(...rootFlowEdges);

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
                    ignoreSchemaFieldStatus,
                    undefined,
                    offsets,
                    (n) => n.urn === node.urn || n.parentDataJob !== urn,
                    undefined,
                    showFilterNodes,
                );
                newLineageFilters.forEach((filter, id) => lineageFilters.set(id, filter));
                newFlowNodes
                    .filter(
                        (n) => n.id !== node.urn && nodes.get(n.id)?.parentDataJob !== urn && !seenNodeIds.has(n.id),
                    )
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

            // The edge's own source/target already encode the true upstream/downstream
            // orientation (see NodeBuilder#addEdge / parseEdgeId), so the common, non-cyclic case
            // doesn't need to re-derive it from `.direction` -- which is only ever set for
            // genuinely cyclic nodes (see getDisplayedNodes.ts) and otherwise undefined, which
            // silently hid every ordinary job-to-external edge here regardless of expansion state.
            if (!upstream.inCycle && !downstream.inCycle) {
                if (
                    downstream.parentDataJob &&
                    !upstream.parentDataJob &&
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
                    !downstream.parentDataJob &&
                    upstream.isExpanded[LineageDirection.Downstream]
                ) {
                    // Data flow -> external downstream
                    return {
                        isInInterior: isInDataFlowGraphInterior.get(upstream.urn)?.get(LineageDirection.Downstream),
                        direction: LineageDirection.Downstream,
                    };
                }
                return { hide: true };
            }

            // Cyclic case: an external entity reachable as both upstream and downstream of the
            // DataFlow. `.direction` is reliably set here (see getDisplayedNodes.ts), so this is
            // unchanged from before.
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
    }
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
 * Computes the DataFlow's own direct upstream/downstream (e.g. a linked Repository via
 * repositoryLineage), independent of any child job's own lineage. Shared by both the no-child-job
 * and has-child-job branches of computeDataFlowGraph above.
 *
 * Skips the traversal entirely when the DataFlow has no direct neighbors of its own (the common
 * case: most DataFlows' only lineage runs through their child jobs), rather than always paying for
 * computeImpactAnalysisGraph's full orderNodes/hideNodes/getDisplayedNodes/NodeBuilder pipeline
 * just to find nothing.
 */
function computeRootOwnLineage(
    urn: string,
    type: EntityType,
    context: Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>,
    ignoreSchemaFieldStatus: boolean,
) {
    const { adjacencyList } = context;
    const hasOwnLineage =
        !!adjacencyList[LineageDirection.Upstream].get(urn)?.size ||
        !!adjacencyList[LineageDirection.Downstream].get(urn)?.size;
    if (!hasOwnLineage) {
        return {
            flowNodes: [],
            flowEdges: [],
            resetPositions: false,
            lineageFilters: new Map<string, LineageFilter>(),
            adjacencyList: buildHighlightAdjacencyList(context.edges, new Set<string>()),
        };
    }
    return computeImpactAnalysisGraph(
        urn,
        type,
        context,
        ignoreSchemaFieldStatus,
        undefined,
        undefined,
        (n) => n.urn === urn || n.parentDataJob !== urn,
    );
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
    const xOffset = direction === LineageDirection.Upstream ? 0 : (boundingBox?.width || 0) - LINEAGE_NODE_WIDTH;
    const yOffset = (ROOT_SEPARATION / 3) * (direction === LineageDirection.Upstream ? -1 : 1);

    return [xOffset, (position?.y || 0) + (isOnEdge ? 0 : yOffset)];
}
