import { CURVE_PADDING, HORIZONTAL_SPACE_PER_LAYER, VERTICAL_SPACE_BETWEEN_NODES } from '../constants';
import { width as nodeWidth } from '../LineageEntityNode';
import { Direction, NodeData, VizEdge, VizNode } from '../types';
import { nodeHeightFromTitleLength } from './nodeHeightFromTitleLength';

type ProcessArray = {
    parent: VizNode | null;
    node: NodeData;
}[];

const INSIDE_NODE_SHIFT = nodeWidth / 2 - 19;

const HEADER_HEIGHT = 125;
const UPSTREAM_X_MODIFIER = -1;
const UPSTREAM_DIRECTION_SHIFT = -20;

export default function layoutTree(
    data: NodeData,
    direction: Direction,
    draggedNodes: Record<string, { x: number; y: number }>,
    canvasHeight: number,
    expandTitles: boolean,
    showColumns: boolean,
    expandedNodes: any,
    fineGrainedMap: any,
): {
    nodesToRender: VizNode[];
    edgesToRender: VizEdge[];
    nodesByUrn: Record<string, VizNode>;
    height: number;
    layers: number;
} {
    const nodesToRender: VizNode[] = [];
    const edgesToRender: VizEdge[] = [];
    let maxHeight = 0;

    const nodesByUrn: Record<string, VizNode> = {};
    const xModifier = direction === Direction.Downstream ? 1 : UPSTREAM_X_MODIFIER;
    const directionShift = direction === Direction.Downstream ? 0 : UPSTREAM_DIRECTION_SHIFT;

    let currentLayer = 0;
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

        const layerHeight = filteredNodesInCurrentLayer
            .map(({ node }) =>
                nodeHeightFromTitleLength(
                    expandTitles ? node.expandedName || node.name : undefined,
                    node.schemaMetadata,
                    showColumns,
                    !!expandedNodes[node?.urn || 'no-op'],
                ),
            )
            .reduce((acc, height) => acc + height, 0);

        maxHeight = Math.max(maxHeight, layerHeight);

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
                          }
                        : {
                              data: node,
                              x: currentXPosition,
                              y: HORIZONTAL_SPACE_PER_LAYER * currentLayer * xModifier,
                          };
                currentXPosition +=
                    nodeHeightFromTitleLength(
                        expandTitles ? node.expandedName || node.name : undefined,
                        node.schemaMetadata,
                        showColumns,
                        !!expandedNodes[node?.urn || 'no-op'],
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
        currentLayer++;
    }

    const forwardEdges = fineGrainedMap.forward;
    if (showColumns) {
        Object.keys(forwardEdges).forEach((entityUrn) => {
            const fieldPathToEdges = forwardEdges[entityUrn];
            Object.keys(fieldPathToEdges).forEach((sourceField) => {
                const fieldForwardEdges = fieldPathToEdges[sourceField];

                const currentNode = nodesToRender.find((node) => node.data.urn === entityUrn);
                const fieldIndex =
                    currentNode?.data.schemaMetadata?.fields.findIndex(
                        (candidate) => candidate.fieldPath === sourceField,
                    ) || 0;
                const hoveredFieldX = (currentNode?.x || 0) + (fieldIndex + 1.1) * 30 + 1;
                const hoveredFieldY = (currentNode?.y || 0) + 1;

                Object.keys(fieldForwardEdges || {}).forEach((targetUrn) => {
                    const targetNode = nodesToRender.find((node) => node.data.urn === targetUrn);
                    (fieldForwardEdges[targetUrn] || []).forEach((targetField) => {
                        const targetFieldIndex =
                            targetNode?.data.schemaMetadata?.fields.findIndex(
                                (candidate) => candidate.fieldPath === targetField,
                            ) || 0;
                        const targetFieldX = (targetNode?.x || 0) + (targetFieldIndex + 1.3) * 30 + 1;
                        const targetFieldY = targetNode?.y || 0 + 1;
                        if (
                            currentNode &&
                            targetNode &&
                            hoveredFieldX &&
                            hoveredFieldY &&
                            targetFieldX &&
                            targetFieldY
                        ) {
                            const curve = [
                                {
                                    x: hoveredFieldX,
                                    y:
                                        hoveredFieldY -
                                        INSIDE_NODE_SHIFT * UPSTREAM_X_MODIFIER +
                                        UPSTREAM_DIRECTION_SHIFT,
                                },
                                {
                                    x: hoveredFieldX,
                                    y: hoveredFieldY - (INSIDE_NODE_SHIFT + CURVE_PADDING) * UPSTREAM_X_MODIFIER,
                                },
                                {
                                    x: targetFieldX,
                                    y: targetFieldY + (nodeWidth / 2 + CURVE_PADDING) * UPSTREAM_X_MODIFIER,
                                },
                                {
                                    x: targetFieldX,
                                    y:
                                        targetFieldY +
                                        (nodeWidth / 2 - 15) * UPSTREAM_X_MODIFIER +
                                        UPSTREAM_DIRECTION_SHIFT,
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
                    });
                });
            });
        });
    }

    return { nodesToRender, edgesToRender, height: maxHeight, layers: currentLayer - 1, nodesByUrn };
}
