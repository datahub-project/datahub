import { useContext, useMemo } from 'react';
import { Node, useReactFlow } from 'reactflow';
import { LineageDirection } from '../../types.generated';
import { createEdgeId, LineageNode, LineageNodesContext, NodeContext } from './common';

export default function useNodeHighlighting(hoveredNode: string | null): {
    highlightedNodes: Set<string>;
    highlightedEdges: Set<string>;
} {
    const { adjacencyList } = useContext(LineageNodesContext);
    const { getNode } = useReactFlow<LineageNode>();
    const { highlightedNodes, highlightedEdges } = useMemo(() => {
        const node = hoveredNode ? getNode(hoveredNode) : null;
        return computeHighlights(node, adjacencyList);
    }, [hoveredNode, adjacencyList, getNode]);

    return { highlightedNodes, highlightedEdges };
}

/** Compute highlighted nodes and table->table edges. */
function computeHighlights(
    node: Node<LineageNode> | undefined | null,
    adjacencyList: NodeContext['adjacencyList'],
): {
    highlightedNodes: Set<string>;
    highlightedEdges: Set<string>;
} {
    const highlightedNodes = new Set<string>();
    const highlightedEdges = new Set<string>();
    if (!node) {
        return { highlightedNodes, highlightedEdges };
    }
    Object.entries(adjacencyList).forEach(([direction, neighborMap]) => {
        const seen = new Set<string>();
        const toVisit = [node.id];
        while (toVisit.length) {
            const urn = toVisit.pop();
            if (urn === undefined) {
                break;
            }
            highlightedNodes.add(urn);
            neighborMap.get(urn)?.forEach((childUrn) => {
                if (!seen.has(childUrn)) {
                    seen.add(childUrn);
                    toVisit.push(childUrn);
                }
                if (direction === LineageDirection.Downstream) {
                    highlightedEdges.add(createEdgeId(urn, childUrn));
                } else {
                    highlightedEdges.add(createEdgeId(childUrn, urn));
                }
            });
        }
    });

    return { highlightedNodes, highlightedEdges };
}
