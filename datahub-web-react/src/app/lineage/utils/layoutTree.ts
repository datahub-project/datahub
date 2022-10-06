import { SchemaField } from '../../../types.generated';
import {
    COLUMN_HEIGHT,
    CURVE_PADDING,
    EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT,
    HORIZONTAL_SPACE_PER_LAYER,
    NUM_COLUMNS_PER_PAGE,
    VERTICAL_SPACE_BETWEEN_NODES,
    width as nodeWidth,
} from '../constants';
import { Direction, NodeData, VizEdge, VizNode } from '../types';
import { getTitleHeight, nodeHeightFromTitleLength } from './titleUtils';

type ProcessArray = {
    parent: VizNode | null;
    node: NodeData;
}[];

const INSIDE_NODE_SHIFT = nodeWidth / 2 - 19;

const HEADER_HEIGHT = 125;
const UPSTREAM_X_MODIFIER = -1;
const UPSTREAM_DIRECTION_SHIFT = -20;
const COLUMN_HEIGHT_BUFFER = 1.2;

function layoutNodesForOneDirection(
    data: NodeData,
    direction: Direction,
    draggedNodes: Record<string, { x: number; y: number }>,
    canvasHeight: number,
    expandTitles: boolean,
    showColumns: boolean,
    collapsedColumnsNodes: any,
    nodesToRender: VizNode[],
    edgesToRender: VizEdge[],
) {
    const nodesByUrn: Record<string, VizNode> = {};
    const xModifier = direction === Direction.Downstream ? 1 : UPSTREAM_X_MODIFIER;
    const directionShift = direction === Direction.Downstream ? 0 : UPSTREAM_DIRECTION_SHIFT;

    let numInCurrentLayer = 0;
    let nodesInCurrentLayer: ProcessArray = [{ parent: null, node: data }];
    let nodesInNextLayer: ProcessArray = [];

    while (nodesInCurrentLayer.length > 0) {
        // if we've already added a node to the viz higher up dont add it again
        const urnsToAddInCurrentLayer = Array.from(new Set(nodesInCurrentLayer.map(({ node }) => node.urn || '')));
        const nodesToAddInCurrentLayer = urnsToAddInCurrentLayer
            .filter((urn, pos) => urnsToAddInCurrentLayer.indexOf(urn) === pos)
            .filter((urn) => !nodesByUrn[urn || '']);

        const filteredNodesInCurrentLayer = nodesInCurrentLayer
            .filter(({ node }) => nodesToAddInCurrentLayer.indexOf(node.urn || '') > -1)
            .filter(({ node }) => node.status?.removed !== true);

        const layerSize = filteredNodesInCurrentLayer.length;

        // approximate the starting position assuming each node has a 1 line title (its ok to be a bit off here)
        let currentXPosition =
            -(
                (nodeHeightFromTitleLength(undefined, undefined, showColumns, false) + VERTICAL_SPACE_BETWEEN_NODES) *
                (layerSize - 1)
            ) /
                2 +
            canvasHeight / 2 +
            HEADER_HEIGHT;

        // eslint-disable-next-line @typescript-eslint/no-loop-func
        nodesInCurrentLayer.forEach(({ node, parent }) => {
            if (!node.urn) return;

            // don't show edges to soft deleted entities
            if (node.status?.removed) return;

            let vizNodeForNode: VizNode;

            if (nodesByUrn[node.urn]) {
                vizNodeForNode = nodesByUrn[node.urn];
            } else {
                vizNodeForNode =
                    node.urn in draggedNodes
                        ? {
                              data: node,
                              x: draggedNodes[node.urn].x,
                              y: draggedNodes[node.urn].y,
                              direction,
                          }
                        : {
                              data: node,
                              x: currentXPosition,
                              y: HORIZONTAL_SPACE_PER_LAYER * numInCurrentLayer * xModifier,
                              direction,
                          };
                currentXPosition +=
                    nodeHeightFromTitleLength(
                        expandTitles ? node.expandedName || node.name : undefined,
                        node.schemaMetadata,
                        showColumns,
                        !!collapsedColumnsNodes[node?.urn || 'no-op'], // avoid indexing on undefined if node is undefined
                    ) + VERTICAL_SPACE_BETWEEN_NODES;

                nodesByUrn[node.urn] = vizNodeForNode;
                nodesToRender.push(vizNodeForNode);
                nodesInNextLayer = [
                    ...nodesInNextLayer,
                    ...(node.children?.map((child) => ({
                        parent: vizNodeForNode,
                        node: child,
                    })) || []),
                ];
            }

            if (parent) {
                const parentIsHigher = parent.x > vizNodeForNode.x;
                const parentIsBehindChild =
                    direction === Direction.Downstream
                        ? parent.y < vizNodeForNode.y - nodeWidth
                        : parent.y > vizNodeForNode.y + nodeWidth;

                // if the nodes are inverted, we want to draw the edge slightly differently
                const curve = parentIsBehindChild
                    ? [
                          { x: parent.x, y: parent.y + INSIDE_NODE_SHIFT * xModifier + directionShift },
                          { x: parent.x, y: parent.y + (INSIDE_NODE_SHIFT + CURVE_PADDING) * xModifier },
                          { x: vizNodeForNode.x, y: vizNodeForNode.y - (nodeWidth / 2 + CURVE_PADDING) * xModifier },
                          { x: vizNodeForNode.x, y: vizNodeForNode.y - (nodeWidth / 2) * xModifier + directionShift },
                      ]
                    : [
                          { x: parent.x, y: parent.y + INSIDE_NODE_SHIFT * xModifier + directionShift },
                          { x: parent.x, y: parent.y + (INSIDE_NODE_SHIFT + CURVE_PADDING) * xModifier },
                          {
                              x: parent.x + CURVE_PADDING * (parentIsHigher ? -1 : 1),
                              y: parent.y + (INSIDE_NODE_SHIFT + CURVE_PADDING) * xModifier,
                          },
                          {
                              x: vizNodeForNode.x + CURVE_PADDING * (parentIsHigher ? 1 : -1),
                              y: vizNodeForNode.y - (nodeWidth / 2 + CURVE_PADDING) * xModifier,
                          },
                          { x: vizNodeForNode.x, y: vizNodeForNode.y - (nodeWidth / 2 + CURVE_PADDING) * xModifier },
                          { x: vizNodeForNode.x, y: vizNodeForNode.y - (nodeWidth / 2) * xModifier + directionShift },
                      ];

                const vizEdgeForPair = {
                    source: parent,
                    target: vizNodeForNode,
                    curve,
                };
                edgesToRender.push(vizEdgeForPair);
            }
        });

        nodesInCurrentLayer = nodesInNextLayer;
        nodesInNextLayer = [];
        numInCurrentLayer++;
    }
    return { numInCurrentLayer, nodesByUrn };
}

interface DrawColumnEdgeProps {
    targetNode?: VizNode;
    currentNode?: VizNode;
    targetField: string;
    targetFields: SchemaField[];
    targetTitleHeight: number;
    collapsedColumnsNodes: any;
    sourceFieldX: number;
    sourceFieldY: number;
    edgesToRender: VizEdge[];
    sourceField: string;
    entityUrn: string;
    targetUrn: string;
    visibleColumnsByUrn: Record<string, Set<string>>;
}

function drawColumnEdge({
    targetNode,
    currentNode,
    targetField,
    targetFields,
    targetTitleHeight,
    collapsedColumnsNodes,
    sourceFieldX,
    sourceFieldY,
    edgesToRender,
    sourceField,
    entityUrn,
    targetUrn,
    visibleColumnsByUrn,
}: DrawColumnEdgeProps) {
    const targetFieldIndex = targetFields.findIndex((candidate) => candidate.fieldPath === targetField) || 0;
    const targetFieldY = targetNode?.y || 0 + 1;
    let targetFieldX = (targetNode?.x || 0) + 35 + targetTitleHeight;
    if (!collapsedColumnsNodes[targetNode?.data.urn || 'no-op']) {
        if (!visibleColumnsByUrn[targetUrn]?.has(targetField)) {
            targetFieldX =
                (targetNode?.x || 0) +
                targetTitleHeight +
                EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT +
                (NUM_COLUMNS_PER_PAGE + COLUMN_HEIGHT_BUFFER) * COLUMN_HEIGHT +
                1;
        } else {
            targetFieldX =
                (targetNode?.x || 0) +
                targetTitleHeight +
                EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT +
                ((targetFieldIndex % NUM_COLUMNS_PER_PAGE) + COLUMN_HEIGHT_BUFFER) * COLUMN_HEIGHT +
                1;
        }
    }

    if (currentNode && targetNode && sourceFieldX && sourceFieldY && targetFieldX && targetFieldY) {
        const curve = [
            {
                x: sourceFieldX,
                y: sourceFieldY - INSIDE_NODE_SHIFT * UPSTREAM_X_MODIFIER + UPSTREAM_DIRECTION_SHIFT,
            },
            {
                x: sourceFieldX,
                y: sourceFieldY - (INSIDE_NODE_SHIFT + CURVE_PADDING) * UPSTREAM_X_MODIFIER,
            },
            {
                x: targetFieldX,
                y: targetFieldY + (nodeWidth / 2 + CURVE_PADDING) * UPSTREAM_X_MODIFIER,
            },
            {
                x: targetFieldX,
                y: targetFieldY + (nodeWidth / 2 - 15) * UPSTREAM_X_MODIFIER + UPSTREAM_DIRECTION_SHIFT,
            },
        ];

        const vizEdgeForPair = {
            source: currentNode,
            target: targetNode,
            sourceField,
            targetField,
            curve,
        };

        if (
            !edgesToRender.find(
                (edge) =>
                    edge.source.data.urn === entityUrn &&
                    edge.sourceField === sourceField &&
                    edge.target.data.urn === targetUrn &&
                    edge.targetField === targetField,
            )
        ) {
            edgesToRender.push(vizEdgeForPair);
        }
    }
}

function layoutColumnTree(
    fineGrainedMap: any,
    showColumns: boolean,
    nodesToRender: VizNode[],
    expandTitles: boolean,
    collapsedColumnsNodes: any,
    edgesToRender: VizEdge[],
    visibleColumnsByUrn: Record<string, Set<string>>,
    columnsByUrn: Record<string, SchemaField[]>,
) {
    const forwardEdges = fineGrainedMap.forward;
    if (showColumns) {
        Object.keys(forwardEdges).forEach((entityUrn) => {
            const fieldPathToEdges = forwardEdges[entityUrn];
            Object.keys(fieldPathToEdges).forEach((sourceField) => {
                const fieldForwardEdges = fieldPathToEdges[sourceField];

                const currentNode = nodesToRender.find((node) => node.data.urn === entityUrn);
                const fields = columnsByUrn[currentNode?.data.urn || ''] || [];
                const fieldIndex = fields.findIndex((candidate) => candidate.fieldPath === sourceField) || 0;

                const sourceTitleHeight = getTitleHeight(
                    expandTitles ? currentNode?.data.expandedName || currentNode?.data.name : undefined,
                );

                const sourceFieldY = currentNode?.y || 0 + 1;
                let sourceFieldX = (currentNode?.x || 0) + 30 + sourceTitleHeight;
                if (!collapsedColumnsNodes[currentNode?.data.urn || 'no-op']) {
                    if (!visibleColumnsByUrn[entityUrn]?.has(sourceField)) {
                        sourceFieldX =
                            (currentNode?.x || 0) +
                            sourceTitleHeight +
                            EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT +
                            (NUM_COLUMNS_PER_PAGE + COLUMN_HEIGHT_BUFFER) * COLUMN_HEIGHT +
                            1;
                    } else {
                        sourceFieldX =
                            (currentNode?.x || 0) +
                            sourceTitleHeight +
                            EXPAND_COLLAPSE_COLUMNS_TOGGLE_HEIGHT +
                            ((fieldIndex % NUM_COLUMNS_PER_PAGE) + COLUMN_HEIGHT_BUFFER) * COLUMN_HEIGHT +
                            1;
                    }
                }

                Object.keys(fieldForwardEdges || {}).forEach((targetUrn) => {
                    const targetNode = nodesToRender.find((node) => node.data.urn === targetUrn);
                    const targetFields = columnsByUrn[targetNode?.data.urn || ''] || [];
                    const targetTitleHeight = getTitleHeight(
                        expandTitles ? targetNode?.data.expandedName || targetNode?.data.name : undefined,
                    );

                    (fieldForwardEdges[targetUrn] || []).forEach((targetField) => {
                        if (
                            (visibleColumnsByUrn[entityUrn]?.has(sourceField) ||
                                visibleColumnsByUrn[targetUrn]?.has(targetField)) &&
                            targetFields.find((field) => field.fieldPath === targetField) &&
                            fields.find((field) => field.fieldPath === sourceField)
                        ) {
                            drawColumnEdge({
                                targetNode,
                                currentNode,
                                targetField,
                                targetFields,
                                targetTitleHeight,
                                collapsedColumnsNodes,
                                sourceFieldX,
                                sourceFieldY,
                                edgesToRender,
                                sourceField,
                                entityUrn,
                                targetUrn,
                                visibleColumnsByUrn,
                            });
                        }
                    });
                });
            });
        });
    }
}

export default function layoutTree(
    upstreamData: NodeData,
    downstreamData: NodeData,
    draggedNodes: Record<string, { x: number; y: number }>,
    canvasHeight: number,
    expandTitles: boolean,
    showColumns: boolean,
    collapsedColumnsNodes: any,
    fineGrainedMap: any,
    visibleColumnsByUrn: Record<string, Set<string>>,
    columnsByUrn: Record<string, SchemaField[]>,
): {
    nodesToRender: VizNode[];
    edgesToRender: VizEdge[];
    nodesByUrn: Record<string, VizNode>;
    layers: number;
} {
    const nodesToRender: VizNode[] = [];
    const edgesToRender: VizEdge[] = [];

    const { numInCurrentLayer: numUpstream, nodesByUrn: upstreamNodesByUrn } = layoutNodesForOneDirection(
        upstreamData,
        Direction.Upstream,
        draggedNodes,
        canvasHeight,
        expandTitles,
        showColumns,
        collapsedColumnsNodes,
        nodesToRender,
        edgesToRender,
    );

    const { numInCurrentLayer: numDownstream, nodesByUrn: downstreamNodesByUrn } = layoutNodesForOneDirection(
        downstreamData,
        Direction.Downstream,
        draggedNodes,
        canvasHeight,
        expandTitles,
        showColumns,
        collapsedColumnsNodes,
        nodesToRender,
        edgesToRender,
    );

    const nodesByUrn = { ...upstreamNodesByUrn, ...downstreamNodesByUrn };

    layoutColumnTree(
        fineGrainedMap,
        showColumns,
        nodesToRender,
        expandTitles,
        collapsedColumnsNodes,
        edgesToRender,
        visibleColumnsByUrn,
        columnsByUrn,
    );

    return { nodesToRender, edgesToRender, layers: numUpstream + numDownstream - 1, nodesByUrn };
}
