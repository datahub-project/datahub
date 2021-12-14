import { CURVE_PADDING, HORIZONTAL_SPACE_PER_LAYER, VERTICAL_SPACE_PER_NODE } from '../constants';
import { Direction, NodeData, VizEdge, VizNode } from '../types';
import { width as nodeWidth } from '../LineageEntityNode';

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
): {
    nodesToRender: VizNode[];
    edgesToRender: VizEdge[];
    nodesByUrn: Record<string, VizNode>;
    height: number;
    layers: number;
} {
    const nodesToRender: VizNode[] = [];
    const edgesToRender: VizEdge[] = [];
    let maxHeight = VERTICAL_SPACE_PER_NODE;

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

        const layerSize = nodesToAddInCurrentLayer.length;
        const layerHeight = VERTICAL_SPACE_PER_NODE * (layerSize - 1);

        maxHeight = Math.max(maxHeight, layerHeight);

        let addedIndex = 0;
        // eslint-disable-next-line @typescript-eslint/no-loop-func
        nodesInCurrentLayer.forEach(({ node, parent }) => {
            if (!node.urn) return;

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
                              x:
                                  VERTICAL_SPACE_PER_NODE * addedIndex -
                                  (VERTICAL_SPACE_PER_NODE * (layerSize - 1)) / 2 +
                                  canvasHeight / 2 +
                                  HEADER_HEIGHT,
                              y: HORIZONTAL_SPACE_PER_LAYER * currentLayer * xModifier,
                          };
                nodesByUrn[node.urn] = vizNodeForNode;
                nodesToRender.push(vizNodeForNode);
                nodesInNextLayer = [
                    ...nodesInNextLayer,
                    ...(node.children?.map((child) => ({
                        parent: vizNodeForNode,
                        node: child,
                    })) || []),
                ];
                addedIndex++;
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
