import { HORIZONTAL_SPACE_PER_LAYER, VERTICAL_SPACE_PER_NODE } from '../constants';
import { Direction, NodeData, VizEdge, VizNode } from '../types';

type ProcessArray = {
    parent: VizNode | null;
    node: NodeData;
}[];

export default function generateTree(
    data: NodeData,
    direction: Direction,
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
                const vizEdgeForPair = {
                    source: parent,
                    target: vizNodeForNode,
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
