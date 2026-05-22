import type { XYPosition } from '@reactflow/core/dist/esm/types';
import { Edge, Node } from 'reactflow';

import { LINEAGE_NODE_HEIGHT } from '@app/lineageV2/LineageEntityNode/useDisplayedColumns';
import {
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import {
    AGGREGATED_LINEAGE_EDGE_NAME,
    AggregatedLineageEdgeData,
} from '@app/lineageV3/LineageEdge/AggregatedLineageEdge';
import { CUSTOM_SMOOTH_STEP_EDGE_NAME } from '@app/lineageV3/LineageEdge/CustomSmoothStepEdge';
import { DATA_JOB_INPUT_OUTPUT_EDGE_NAME } from '@app/lineageV3/LineageEdge/DataJobInputOutputEdge';
import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import { LINEAGE_FILTER_NODE_NAME } from '@app/lineageV3/LineageFilterNode/LineageFilterNodeBasic';
import {
    GraphStoreFields,
    LINEAGE_NODE_WIDTH,
    LineageBoundingBox,
    LineageEdge,
    LineageEdgeData,
    LineageToggles,
    NodeContext,
    parseEdgeId,
} from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import NodeBuilder, { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import computeConnectedComponents from '@app/lineageV3/useComputeGraph/computeConnectedComponents';
import computeImpactAnalysisGraph from '@app/lineageV3/useComputeGraph/computeImpactAnalysisGraph';
import hideNodes, { HideNodesConfig } from '@app/lineageV3/useComputeGraph/filterNodes';

import { EntityType, LineageDirection } from '@types';

type Urn = string;

// Minimum vertical gap between adjacent external nodes in the same column.
const EXTERNAL_NODE_MIN_SPACING = LINEAGE_NODE_HEIGHT + 50;

const ROOT_SEPARATION = 100 + LINEAGE_NODE_HEIGHT;

// Matches the default cardHeight in LineageBoundingBoxNode (rendered above the box via translateY(-100%)).
const BOUNDING_BOX_LABEL_HEIGHT = 54;

/**
 * Computes the lineage graph for a DataProduct, arranged as a bounding-box container.
 *
 * Layout:
 * 1. Member entities that have lineage relationships with each other are placed HORIZONTALLY
 *    (upstream members to the left, downstream members to the right) using the same NodeBuilder
 *    algorithm as the standard impact-analysis view. Members without intra-product connections
 *    form independent groups that are stacked vertically below connected groups.
 * 2. A DataProduct bounding box wraps all member entities.
 * 3. For each member entity, upstream/downstream entities can be expanded left/right outside the box.
 * 4. Intra-product lineage edges (member → member) are drawn using standard horizontal handles.
 * 5. External entities that belong to other DataProducts are grouped into their own bounding-box
 *    containers so neighboring pipelines are visually clear.
 *
 * A dataset in multiple data products is shown only once (nodes are keyed by URN).
 */
export default function computeDataProductGraph(
    urn: string,
    type: EntityType,
    context: Pick<NodeContext, GraphStoreFields | LineageToggles | 'rootType'>,
    ignoreSchemaFieldStatus: boolean,
) {
    const { nodes, edges, adjacencyList, rootType, showDataProcessInstances, showGhostEntities } = context;
    const graphStore = { nodes, edges, adjacencyList };

    const config: HideNodesConfig = {
        hideTransformations: false,
        hideDataProcessInstances: !showDataProcessInstances,
        hideGhostEntities: !showGhostEntities,
        ignoreSchemaFieldStatus,
    };

    const newGraphStore = {
        ...hideNodes(urn, rootType, config, graphStore, (node) => node.parentDataProduct === urn),
        rootType,
    };

    const memberUrns = new Set<Urn>(Array.from(newGraphStore.nodes.keys()));

    const { displayedNodesByRoots, parents } = computeConnectedComponents(newGraphStore);
    const flowNodes: LineageVisualizationNode[] = [];
    const flowEdges: Edge[] = [];
    const memberPositions = new Map<string, XYPosition>();
    const isOnLeftSideOfBox = new Map<string, boolean>();
    const isOnRightSideOfBox = new Map<string, boolean>();
    const isOnEdgeOfBox = {
        [LineageDirection.Upstream]: isOnLeftSideOfBox,
        [LineageDirection.Downstream]: isOnRightSideOfBox,
    };

    displayedNodesByRoots
        .sort(([_rootsA, componentA], [_rootsB, componentB]) => componentB.length - componentA.length)
        .forEach(([roots, displayedNodes]) => {
            const maxY = flowNodes.reduce((max, node) => Math.max(max, node.position.y), 0);
            const offset: [number, number] = [0, flowNodes.length ? maxY + ROOT_SEPARATION : 0];
            const offsets = new Map([[undefined, offset]]);
            const nodeBuilder = new NodeBuilder(urn, type, roots, displayedNodes, parents, true);
            const newFlowNodes = nodeBuilder.createNodes(newGraphStore, ignoreSchemaFieldStatus, offsets, urn);
            flowNodes.push(...newFlowNodes);

            newFlowNodes.forEach((node) => {
                memberPositions.set(node.id, node.position);
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

    const isInDataProductGraphInterior: Map<string, Map<LineageDirection, boolean>> = new Map();
    if (flowNodes.length) {
        const domainColorHex =
            nodes.get(urn)?.entity?.genericEntityProperties?.domain?.domain?.displayProperties?.colorHex ?? undefined;

        const boundingBox = addBoundingBoxDataProduct(flowNodes, {
            urn,
            type,
            entity: nodes.get(urn)?.entity,
            colorHex: domainColorHex,
        });

        const addedExternalIds = new Set<string>();

        newGraphStore.nodes.forEach((node) => {
            const offsets: Map<LineageDirection, [number, number]> = new Map(
                Object.values(LineageDirection).map((direction) => [
                    direction,
                    computeInputOutputOffset(
                        direction,
                        memberPositions.get(node.urn),
                        boundingBox,
                        isOnEdgeOfBox[direction].get(node.urn) || false,
                    ),
                ]),
            );
            isInDataProductGraphInterior.set(
                node.urn,
                new Map(
                    Object.values(LineageDirection).map((direction) => [
                        direction,
                        !isOnEdgeOfBox[direction].get(node.urn),
                    ]),
                ),
            );

            if (node.isExpanded.UPSTREAM || node.isExpanded.DOWNSTREAM) {
                const { flowNodes: newFlowNodes, flowEdges: newFlowEdges } = computeImpactAnalysisGraph(
                    node.urn,
                    node.type,
                    context,
                    ignoreSchemaFieldStatus,
                    undefined,
                    offsets,
                    (n) => n.urn === node.urn || n.parentDataProduct !== urn,
                );
                flowNodes.push(
                    ...newFlowNodes.filter((n) => {
                        if (n.id === node.urn) return false;
                        if (nodes.get(n.id)?.parentDataProduct === urn) return false;
                        if (addedExternalIds.has(n.id)) return false;
                        if (n.type === LINEAGE_FILTER_NODE_NAME) return false;
                        addedExternalIds.add(n.id);
                        return true;
                    }),
                );
                flowEdges.push(
                    ...newFlowEdges.filter(
                        (e) => !nodes.get(e.source)?.parentDataProduct && !nodes.get(e.target)?.parentDataProduct,
                    ),
                );
            }
        });

        resolveExternalNodeOverlaps(flowNodes, memberUrns);

        addIntraProductEdges(flowEdges, newGraphStore.edges, memberUrns);

        const extraEdgeData = buildExtraEdgeData(nodes, memberUrns, isInDataProductGraphInterior);
        const nodeBuilder = new NodeBuilder(urn, type, [], Array.from(nodes.values()), new Map(), false);
        flowEdges.push(
            ...nodeBuilder
                .createEdges(edges, undefined, DATA_JOB_INPUT_OUTPUT_EDGE_NAME, extraEdgeData)
                .filter((e) => !e.data?.hide),
        );

        addNeighborDataProductBoxes(flowNodes, flowEdges, nodes, memberUrns, urn, boundingBox);
        normalizeExternalColumns(flowNodes, memberUrns, boundingBox.width ?? (boundingBox.style?.width as number) ?? 0);

        for (let pass = 0; pass < 2; pass++) {
            resolveNodeBoxConflicts(flowNodes);
            resolveExternalNodeOverlaps(flowNodes, memberUrns);
        }
    }

    return { flowNodes, flowEdges, resetPositions: false };
}

function normalizeExternalColumns(flowNodes: LineageVisualizationNode[], memberUrns: Set<Urn>, boxWidth: number): void {
    const downstream: LineageVisualizationNode[] = [];
    const upstream: LineageVisualizationNode[] = [];

    flowNodes.forEach((node) => {
        if (node.type !== LINEAGE_ENTITY_NODE_NAME) return;
        if (memberUrns.has(node.id)) return;
        // Nodes with parentId already have relative positions — skip them.
        if (node.parentId) return;
        if (node.position.x >= boxWidth) {
            downstream.push(node);
        } else if (node.position.x < 0) {
            upstream.push(node);
        }
    });

    if (downstream.length > 1) {
        const targetX = Math.min(...downstream.map((n) => n.position.x));
        downstream.forEach((n) => {
            /* eslint-disable no-param-reassign */
            n.position = { ...n.position, x: targetX };
            /* eslint-enable no-param-reassign */
        });
    }

    if (upstream.length > 1) {
        const targetX = Math.max(...upstream.map((n) => n.position.x));
        upstream.forEach((n) => {
            /* eslint-disable no-param-reassign */
            n.position = { ...n.position, x: targetX };
            /* eslint-enable no-param-reassign */
        });
    }
}

function resolveExternalNodeOverlaps(flowNodes: LineageVisualizationNode[], memberUrns: Set<Urn>): void {
    const columnMap = new Map<number, LineageVisualizationNode[]>();
    flowNodes.forEach((node) => {
        if (node.type !== LINEAGE_ENTITY_NODE_NAME) return;
        if (memberUrns.has(node.id)) return;
        if (node.parentId) return;
        const col = Math.round(node.position.x / 25) * 25;
        const existing = columnMap.get(col);
        if (existing) {
            existing.push(node);
        } else {
            columnMap.set(col, [node]);
        }
    });

    const neighborBoxes = flowNodes.filter(
        (n) => n.type === LINEAGE_BOUNDING_BOX_NODE_NAME && (n.position.x !== 0 || n.position.y !== 0),
    );

    columnMap.forEach((colNodes, col) => {
        if (colNodes.length < 2) return;
        colNodes.sort((a, b) => a.position.y - b.position.y);

        const originalCentre = colNodes.reduce((sum, n) => sum + n.position.y, 0) / colNodes.length;
        const totalSpread = (colNodes.length - 1) * EXTERNAL_NODE_MIN_SPACING;
        const startY = originalCentre - totalSpread / 2;

        colNodes.forEach((node, i) => {
            /* eslint-disable no-param-reassign */
            node.position.y = startY + i * EXTERNAL_NODE_MIN_SPACING;
            /* eslint-enable no-param-reassign */
        });

        const nodeRight = col + LINEAGE_NODE_WIDTH;
        const colBoxes = neighborBoxes
            .filter((box) => {
                const boxLeft = box.position.x;
                const boxRight = boxLeft + (box.width || 0);
                return boxRight > col && boxLeft < nodeRight;
            })
            .sort((a, b) => a.position.y - b.position.y);

        if (colBoxes.length === 0) return;

        const boxPadding = BOUNDING_BOX_PADDING / 2;
        colNodes.sort((a, b) => a.position.y - b.position.y);

        let cursor = colNodes[0].position.y;
        let boxIdx = 0;

        colNodes.forEach((node) => {
            while (boxIdx < colBoxes.length) {
                const box = colBoxes[boxIdx];
                const boxTop = box.position.y - BOUNDING_BOX_LABEL_HEIGHT - boxPadding;
                const boxBottom = box.position.y + (box.height || 0) + boxPadding;
                if (cursor + LINEAGE_NODE_HEIGHT > boxTop) {
                    cursor = Math.max(cursor, boxBottom);
                    boxIdx++;
                } else {
                    break;
                }
            }
            /* eslint-disable no-param-reassign */
            node.position = { ...node.position, y: cursor };
            /* eslint-enable no-param-reassign */
            cursor += EXTERNAL_NODE_MIN_SPACING;
        });
    });
}

function addIntraProductEdges(flowEdges: Edge[], edges: NodeContext['edges'], memberUrns: Set<Urn>): void {
    edges.forEach((edge: LineageEdge, edgeId) => {
        if (!edge.isDisplayed) return;
        const [upstream, downstream] = parseEdgeId(edgeId);
        if (!memberUrns.has(upstream) || !memberUrns.has(downstream)) return;
        if (flowEdges.some((fe) => fe.id === edgeId)) return;

        flowEdges.push({
            id: edgeId,
            source: upstream,
            target: downstream,
            type: CUSTOM_SMOOTH_STEP_EDGE_NAME,
            data: { ...edge, originalId: edgeId },
        });
    });
}

function addNeighborDataProductBoxes(
    flowNodes: LineageVisualizationNode[],
    flowEdges: Edge[],
    nodes: NodeContext['nodes'],
    memberUrns: Set<Urn>,
    rootUrn: Urn,
    sourceBoundingBox: Node<LineageBoundingBox>,
): void {
    const groups = new Map<Urn, { name?: string; colorHex?: string; domain?: any; nodeIds: Urn[] }>();

    flowNodes.forEach((flowNode) => {
        if (flowNode.type !== LINEAGE_ENTITY_NODE_NAME) return;
        if (memberUrns.has(flowNode.id)) return;
        const lineageNode = nodes.get(flowNode.id);
        if (!lineageNode?.entity) return;

        // `genericEntityProperties.dataProduct` is the aliased `relationships(...)` result from
        // the `entityDataProduct` fragment — shape: { relationships: [{ entity: DataProduct }] }
        const dpResult = lineageNode.entity.genericEntityProperties?.dataProduct as any;
        const dpRels: any[] = dpResult?.relationships ?? [];
        if (!dpRels.length) return;

        dpRels.forEach((dpRel: any) => {
            const dpUrn: string | undefined = dpRel?.entity?.urn;
            if (!dpUrn || dpUrn === rootUrn) return;

            if (!groups.has(dpUrn)) {
                const dpEntity = dpRel?.entity;
                const colorHex: string | undefined = dpEntity?.domain?.domain?.displayProperties?.colorHex;
                groups.set(dpUrn, {
                    name: dpEntity?.properties?.name as string | undefined,
                    colorHex,
                    domain: dpEntity?.domain,
                    nodeIds: [],
                });
            }
            groups.get(dpUrn)!.nodeIds.push(flowNode.id);
        });
    });

    groups.forEach((dpInfo, dpUrn) => {
        const nodeIdSet = new Set(dpInfo.nodeIds);
        const groupNodes = flowNodes.filter((n) => nodeIdSet.has(n.id));
        if (!groupNodes.length) return;

        const padding = BOUNDING_BOX_PADDING / 2;
        const minX = Math.min(...groupNodes.map((n) => n.position.x));
        const maxX = Math.max(...groupNodes.map((n) => n.position.x));
        const minY = Math.min(...groupNodes.map((n) => n.position.y));
        const maxY = Math.max(...groupNodes.map((n) => n.position.y));

        const boxX = minX - padding;
        const boxY = minY - padding;
        const width = LINEAGE_NODE_WIDTH + maxX - minX + padding * 2;
        const height = LINEAGE_NODE_HEIGHT + maxY - minY + padding * 2;

        groupNodes.forEach((n) => {
            /* eslint-disable no-param-reassign */
            n.position.x -= boxX;
            n.position.y -= boxY;
            n.parentId = dpUrn;
            n.extent = 'parent';
            n.draggable = false;
            /* eslint-enable no-param-reassign */
        });

        const domainEntity = dpInfo.domain?.domain;
        const neighborEntity: FetchedEntityV2 | undefined = dpInfo.name
            ? {
                  urn: dpUrn,
                  type: EntityType.DataProduct,
                  name: dpInfo.name,
                  exists: true,
                  genericEntityProperties: {
                      type: EntityType.DataProduct,
                      domain: dpInfo.domain ?? undefined,
                      parentDomains: domainEntity ? { domains: [domainEntity], count: 1 } : undefined,
                  } as any,
              }
            : undefined;

        const assetCount = nodeIdSet.size;
        const boxNode: Node<LineageBoundingBox> = {
            id: dpUrn,
            type: LINEAGE_BOUNDING_BOX_NODE_NAME,
            position: { x: boxX, y: boxY },
            data: {
                urn: dpUrn,
                type: EntityType.DataProduct,
                displayName: dpInfo.name,
                subtitle: `${assetCount} ${assetCount === 1 ? 'asset' : 'assets'}`,
                colorHex: dpInfo.colorHex,
                entity: neighborEntity,
            },
            selectable: true,
            draggable: true,
            style: { width, height, zIndex: -2 },
            width,
            height,
        };
        flowNodes.unshift(boxNode);

        flowEdges.push(buildNeighborDpAggregatedEdge(rootUrn, dpUrn, assetCount, boxNode, sourceBoundingBox));
    });
}

// Decides the lineage direction of an aggregated edge between the source DataProduct box and a
// neighbour DataProduct box by comparing their horizontal positions: neighbours laid out to
// the left of the source represent upstream lineage; those to the right are downstream.
function buildNeighborDpAggregatedEdge(
    rootUrn: Urn,
    neighbourDpUrn: Urn,
    assetCount: number,
    neighbourBox: Node<LineageBoundingBox>,
    sourceBox: Node<LineageBoundingBox>,
): Edge<AggregatedLineageEdgeData> {
    const sourceCentre = sourceBox.position.x + (sourceBox.width ?? 0) / 2;
    const neighbourCentre = neighbourBox.position.x + (neighbourBox.width ?? 0) / 2;
    const isUpstream = neighbourCentre < sourceCentre;
    const edgeSource = isUpstream ? neighbourDpUrn : rootUrn;
    const edgeTarget = isUpstream ? rootUrn : neighbourDpUrn;
    return {
        id: `aggregated::${edgeSource}::${edgeTarget}`,
        source: edgeSource,
        target: edgeTarget,
        type: AGGREGATED_LINEAGE_EDGE_NAME,
        data: {
            memberMatchCount: assetCount,
        },
    };
}

function resolveNodeBoxConflicts(flowNodes: LineageVisualizationNode[]): void {
    const padding = BOUNDING_BOX_PADDING / 2;

    const neighborBoxes = flowNodes.filter(
        (n) => n.type === LINEAGE_BOUNDING_BOX_NODE_NAME && (n.position.x !== 0 || n.position.y !== 0),
    );
    if (!neighborBoxes.length) return;

    const standaloneNodes = flowNodes.filter((n) => n.type === LINEAGE_ENTITY_NODE_NAME && !n.parentId);

    neighborBoxes.forEach((box) => {
        const boxLeft = box.position.x;
        const boxRight = boxLeft + (box.width || 0);
        const boxTop = box.position.y;
        const boxBottom = boxTop + (box.height || 0);

        standaloneNodes.forEach((node) => {
            const nodeLeft = node.position.x;
            const nodeRight = nodeLeft + LINEAGE_NODE_WIDTH;
            const nodeTop = node.position.y;
            const nodeBottom = nodeTop + LINEAGE_NODE_HEIGHT;

            if (nodeRight <= boxLeft || nodeLeft >= boxRight) return;
            if (nodeBottom <= boxTop || nodeTop >= boxBottom) return;

            const moveDownShift = boxBottom + padding - nodeTop;
            const moveUpShift = nodeTop + LINEAGE_NODE_HEIGHT + padding - boxTop;
            /* eslint-disable no-param-reassign */
            if (moveDownShift <= moveUpShift) {
                node.position = { ...node.position, y: boxBottom + padding };
            } else {
                node.position = {
                    ...node.position,
                    y: boxTop - LINEAGE_NODE_HEIGHT - BOUNDING_BOX_LABEL_HEIGHT - padding,
                };
            }
            /* eslint-enable no-param-reassign */
        });
    });
}

function buildExtraEdgeData(
    nodes: NodeContext['nodes'],
    memberUrns: Set<Urn>,
    interior: Map<Urn, Map<LineageDirection, boolean>>,
) {
    return (source: string, target: string): Partial<LineageEdgeData> => {
        const upstream = nodes.get(source);
        const downstream = nodes.get(target);

        if (
            !upstream ||
            !downstream ||
            (!upstream.parentDataProduct && !downstream.parentDataProduct) ||
            (upstream.parentDataProduct && downstream.parentDataProduct)
        ) {
            return { hide: true };
        }
        if (memberUrns.has(source) && memberUrns.has(target)) {
            return { hide: true };
        }
        if (
            downstream.parentDataProduct &&
            upstream.direction === LineageDirection.Upstream &&
            downstream.isExpanded[LineageDirection.Upstream]
        ) {
            return {
                isInInterior: interior.get(downstream.urn)?.get(LineageDirection.Upstream),
                direction: LineageDirection.Upstream,
            };
        }
        if (
            upstream.parentDataProduct &&
            downstream.direction === LineageDirection.Downstream &&
            upstream.isExpanded[LineageDirection.Downstream]
        ) {
            return {
                isInInterior: interior.get(upstream.urn)?.get(LineageDirection.Downstream),
                direction: LineageDirection.Downstream,
            };
        }
        if (
            downstream.parentDataProduct &&
            upstream.direction === LineageDirection.Downstream &&
            (upstream.isExpanded[LineageDirection.Downstream] || downstream.isExpanded[LineageDirection.Upstream])
        ) {
            return { direction: LineageDirection.Downstream, isToDataFlow: true };
        }
        if (
            upstream.parentDataProduct &&
            downstream.direction === LineageDirection.Upstream &&
            (upstream.isExpanded[LineageDirection.Downstream] || downstream.isExpanded[LineageDirection.Upstream])
        ) {
            return { direction: LineageDirection.Upstream, isToDataFlow: true };
        }
        return { hide: true };
    };
}

function addBoundingBoxDataProduct(
    flowNodes: LineageVisualizationNode[],
    data: Pick<LineageBoundingBox, 'urn' | 'type' | 'entity' | 'colorHex'>,
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
        node.position.y += BOUNDING_BOX_PADDING - minY;
        /* eslint-enable no-param-reassign */
    });

    const node: Node<LineageBoundingBox> = {
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
            ? BOUNDING_BOX_PADDING
            : (boundingBox?.width || 0) - LINEAGE_NODE_WIDTH - BOUNDING_BOX_PADDING;
    const yOffset = (ROOT_SEPARATION / 3) * (direction === LineageDirection.Upstream ? -1 : 1);
    return [xOffset, (position?.y || 0) + (isOnEdge ? 0 : yOffset)];
}
