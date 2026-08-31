import { useMemo } from 'react';

import { NodeContext, createEdgeId } from '@app/lineageV3/common';

import { LineageDirection } from '@types';

/**
 * Computes the nodes and edges to highlight when hovering a node: everything reachable from the
 * hovered entity, in each direction.
 * @param hoveredNode The hovered entity's urn.
 * @param adjacencyList Adjacency list of the computed graph: only shown nodes, with edges
 *        connected through toggle-hidden nodes.
 */
export default function useNodeHighlighting(
    hoveredNode: string | null,
    adjacencyList: NodeContext['adjacencyList'],
): {
    highlightedNodes: Set<string>;
    highlightedEdges: Set<string>;
} {
    const { highlightedNodes, highlightedEdges } = useMemo(
        // Note: hover state stores the entity urn, which for data product members differs from
        // their node id. Traversal is at the entity level, so start from the urn directly;
        // member edges still highlight, matching on their entity-level `originalId`.
        () => computeHighlights(hoveredNode, adjacencyList),
        [hoveredNode, adjacencyList],
    );

    return { highlightedNodes, highlightedEdges };
}

/** Compute highlighted nodes and table->table edges. */
export function computeHighlights(
    hoveredUrn: string | null,
    adjacencyList: NodeContext['adjacencyList'],
): {
    highlightedNodes: Set<string>;
    highlightedEdges: Set<string>;
} {
    const highlightedNodes = new Set<string>();
    const highlightedEdges = new Set<string>();
    if (!hoveredUrn) {
        return { highlightedNodes, highlightedEdges };
    }
    Object.entries(adjacencyList).forEach(([direction, neighborMap]) => {
        const seen = new Set<string>();
        const toVisit = [hoveredUrn];
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
