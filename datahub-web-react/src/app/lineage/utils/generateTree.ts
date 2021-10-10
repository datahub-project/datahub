import { CURVE_PADDING, HORIZONTAL_SPACE_PER_LAYER, INNER_CURVE_PADDING, VERTICAL_SPACE_PER_NODE } from '../constants';
import { Direction, NodeData, VizEdge, VizNode } from '../types';

type ProcessArray = {
    parent: VizNode | null;
    node: NodeData;
}[];

export default function generateTree(
    data: NodeData,
    direction: Direction,
    draggedNodes: Record<string, { x: number; y: number }>,
): {
    nodesToRender: VizNode[];
    edgesToRender: VizEdge[];
    nodesByUrn: Record<string, VizNode>;
    height: number;
    layers: number;
} {
    console.log(draggedNodes);
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
                vizNodeForNode = {
                    data: node,
                    x: VERTICAL_SPACE_PER_NODE * addedIndex - (VERTICAL_SPACE_PER_NODE * (layerSize - 1)) / 2,
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
                const parentIsBehind = Math.abs(parent.y) < Math.abs(vizNodeForNode.y);

                const curve = parentIsBehind
                    ? [
                          { x: parent.x, y: parent.y + 90 * xModifier + directionShift },
                          { x: parent.x, y: parent.y + (90 + CURVE_PADDING) * xModifier },
                          { x: vizNodeForNode.x, y: vizNodeForNode.y - (105 + CURVE_PADDING) * xModifier },
                          { x: vizNodeForNode.x, y: vizNodeForNode.y - 105 * xModifier + directionShift },
                      ]
                    : [
                          { x: parent.x, y: parent.y + 90 * xModifier + directionShift },
                          { x: parent.x, y: parent.y + (90 + CURVE_PADDING) * xModifier },
                          {
                              x: parent.x + INNER_CURVE_PADDING * (parentIsHigher ? -1 : 1),
                              y: parent.y + (90 + CURVE_PADDING) * xModifier,
                          },
                          {
                              x: vizNodeForNode.x + INNER_CURVE_PADDING * (parentIsHigher ? 1 : -1),
                              y: vizNodeForNode.y - (105 + CURVE_PADDING) * xModifier,
                          },
                          { x: vizNodeForNode.x, y: vizNodeForNode.y - (105 + CURVE_PADDING) * xModifier },
                          { x: vizNodeForNode.x, y: vizNodeForNode.y - 105 * xModifier + directionShift },
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
