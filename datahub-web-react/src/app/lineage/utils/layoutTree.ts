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

export default function layoutTree(
    data: NodeData,
    direction: Direction,
    draggedNodes: Record<string, { x: number; y: number }>,
    canvasHeight: number,
    expandTitles: boolean,
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
    const xModifier = direction === Direction.Downstream ? 1 : -1;
    const directionShift = direction === Direction.Downstream ? 0 : -20;

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
            .map(({ node }) => nodeHeightFromTitleLength(expandTitles ? node.expandedName || node.name : undefined))
            .reduce((acc, height) => acc + height, 0);

        maxHeight = Math.max(maxHeight, layerHeight);

        // approximate the starting position assuming each node has a 1 line title (its ok to be a bit off here)
        let currentXPosition =
            -((nodeHeightFromTitleLength(undefined) + VERTICAL_SPACE_BETWEEN_NODES) * (layerSize - 1)) / 2 +
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
                    nodeHeightFromTitleLength(expandTitles ? node.expandedName || node.name : undefined) +
                    VERTICAL_SPACE_BETWEEN_NODES;

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

    return { nodesToRender, edgesToRender, height: maxHeight, layers: currentLayer - 1, nodesByUrn };
}
