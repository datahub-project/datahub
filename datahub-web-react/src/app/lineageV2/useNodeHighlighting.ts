import { useContext, useMemo } from 'react';
import { Node, useReactFlow } from 'reactflow';
import { LineageDirection } from '../../types.generated';
import {
    COLUMN_QUERY_ID_PREFIX,
    createEdgeId,
    LineageFilter,
    LineageNode,
    LineageNodesContext,
    NeighborMap,
} from './common';

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
    childMaps: Record<LineageDirection, NeighborMap>,
): {
    highlightedNodes: Set<string>;
    highlightedEdges: Set<string>;
} {
    const highlightedNodes = new Set<string>();
    const highlightedEdges = new Set<string>();
    if (!node) {
        return { highlightedNodes, highlightedEdges };
    }
    if (node.id.startsWith(COLUMN_QUERY_ID_PREFIX)) {
        (node as Node<LineageFilter>).data.contents.forEach((urn) => {
            highlightedNodes.add(urn);
        });
        return { highlightedNodes, highlightedEdges };
    }

    Object.entries(childMaps).forEach(([direction, childMap]) => {
        const seen = new Set<string>();
        const toVisit = [node.id];
        while (toVisit.length) {
            const urn = toVisit.pop();
            if (urn === undefined) {
                break;
            }
            highlightedNodes.add(urn);
            childMap.get(urn)?.forEach((childUrn) => {
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
