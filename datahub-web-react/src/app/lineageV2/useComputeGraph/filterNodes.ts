import {
    addToAdjacencyList,
    EdgeId,
    getEdgeId,
    isTransformational,
    LineageAuditStamp,
    LineageEdge,
    NodeContext,
} from '@app/lineageV2/common';
import { LineageDirection } from '@types';

export interface HideNodesConfig {
    hideTransformations: boolean;
}

type ContextSubset = Pick<NodeContext, 'nodes' | 'edges' | 'adjacencyList'>;

/**
 * Hide nodes from the graph, connecting edges through the removed nodes.
 */
export default function hideNodes(
    rootUrn: string,
    config: HideNodesConfig,
    { nodes, edges, adjacencyList }: ContextSubset,
): ContextSubset {
    let newNodes = nodes;
    let newEdges = edges;
    let newAdjacencyList = adjacencyList;
    if (config.hideTransformations) {
        newNodes = new Map(Array.from(newNodes).filter(([urn, node]) => urn === rootUrn || !isTransformational(node)));
        ({ newEdges, newAdjacencyList } = computeEdges(rootUrn, { nodes: newNodes, edges, adjacencyList }));
    }

    return { nodes: newNodes, edges: newEdges, adjacencyList: newAdjacencyList };
}

function computeEdges(rootUrn: string, { nodes, edges, adjacencyList }: ContextSubset) {
    const seen = new Set<string>();
    const newAdjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    const newEdges = new Map<EdgeId, LineageEdge>();

    function buildNewAdjacencyList(id: string, direction: LineageDirection): Set<string> | undefined {
        if (seen.has(id)) {
            return newAdjacencyList[direction].get(id);
        }
        seen.add(id);

        adjacencyList[direction].get(id)?.forEach((neighbor) => {
            if (nodes.has(neighbor)) {
                addToAdjacencyList(newAdjacencyList, direction, id, neighbor);
                const edgeId = getEdgeId(id, neighbor, direction);
                // isDisplayed always true -- only set to false right now to deduplicate edges through dbt
                newEdges.set(edgeId, { isManual: false, ...edges.get(edgeId), via: undefined, isDisplayed: true });
                buildNewAdjacencyList(neighbor, direction);
            } else {
                buildNewAdjacencyList(neighbor, direction)?.forEach((child) => {
                    addToAdjacencyList(newAdjacencyList, direction, id, child);
                    const edgeId = getEdgeId(id, child, direction);
                    const firstEdge = edges.get(getEdgeId(id, neighbor, direction));
                    const secondEdge = edges.get(getEdgeId(neighbor, child, direction));
                    newEdges.set(edgeId, {
                        isManual: (firstEdge?.isManual || secondEdge?.isManual) ?? false,
                        created: getLatestTimestamp(firstEdge?.created, secondEdge?.created),
                        updated: getLatestTimestamp(firstEdge?.updated, secondEdge?.updated),
                        isDisplayed: true,
                    });
                });
            }
        });
        return newAdjacencyList[direction].get(id);
    }

    buildNewAdjacencyList(rootUrn, LineageDirection.Upstream);
    seen.clear();
    buildNewAdjacencyList(rootUrn, LineageDirection.Downstream);
    return { newAdjacencyList, newEdges };
}

function getLatestTimestamp(
    a: LineageAuditStamp | undefined,
    b: LineageAuditStamp | undefined,
): LineageAuditStamp | undefined {
    if (a?.timestamp && b?.timestamp) {
        return a.timestamp > b.timestamp ? a : b;
    }
    return a ?? b;
}
