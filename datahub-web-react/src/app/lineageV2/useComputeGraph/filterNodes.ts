import {
    addToAdjacencyList,
    EdgeId,
    getEdgeId,
    isGhostEntity,
    isTransformational,
    LineageAuditStamp,
    LineageEdge,
    NodeContext,
    parseEdgeId,
    setDefault,
} from '@app/lineageV2/common';
import { LineageDirection } from '@types';

export interface HideNodesConfig {
    hideTransformations: boolean;
    hideGhostEntities: boolean;
    ignoreSchemaFieldStatus: boolean;
}

type ContextSubset = Pick<NodeContext, 'nodes' | 'edges' | 'adjacencyList'>;

/**
 * Hide nodes from the graph, connecting edges through the removed nodes.
 */
export default function hideNodes(
    rootUrn: string,
    { hideTransformations, hideGhostEntities, ignoreSchemaFieldStatus }: HideNodesConfig,
    { nodes, edges, adjacencyList }: ContextSubset,
): ContextSubset {
    let newNodes = nodes;
    let newEdges = edges;
    let newAdjacencyList = adjacencyList;
    if (hideGhostEntities) {
        newNodes = new Map(
            Array.from(newNodes).filter(
                ([urn, node]) => urn === rootUrn || !isGhostEntity(node.entity, ignoreSchemaFieldStatus),
            ),
        );
        ({ newEdges, newAdjacencyList } = pruneEdges({
            nodes: newNodes,
            edges: newEdges,
            adjacencyList: newAdjacencyList,
        }));
    }
    if (hideTransformations) {
        newNodes = new Map(Array.from(newNodes).filter(([urn, node]) => urn === rootUrn || !isTransformational(node)));
        ({ newEdges, newAdjacencyList } = connectEdges(rootUrn, {
            nodes: newNodes,
            edges: newEdges,
            adjacencyList: newAdjacencyList,
        }));
    }

    return { nodes: newNodes, edges: newEdges, adjacencyList: newAdjacencyList };
}

/**
 * Return new adjacency list and edge map, with edges pruned to only connect nodes that are still present.
 */
function pruneEdges({ nodes, edges }: ContextSubset) {
    const newEdges = new Map<EdgeId, LineageEdge>();
    const newAdjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };

    edges.forEach((edge, edgeId) => {
        const [upstream, downstream] = parseEdgeId(edgeId);
        if (nodes.has(upstream) && nodes.has(downstream)) {
            newEdges.set(edgeId, edge);
            addToAdjacencyList(newAdjacencyList, LineageDirection.Upstream, downstream, upstream);
            addToAdjacencyList(newAdjacencyList, LineageDirection.Downstream, upstream, downstream);
            if (edge.via) {
                setDefault(newAdjacencyList[LineageDirection.Upstream], edge.via, new Set()).add(upstream);
                setDefault(newAdjacencyList[LineageDirection.Downstream], edge.via, new Set()).add(downstream);
            }
        }
    });

    return { newEdges, newAdjacencyList };
}

/**
 * Return new adjacency list and edge map, connecting edges through the removed nodes.
 */
function connectEdges(rootUrn: string, { nodes, edges, adjacencyList }: ContextSubset) {
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
                newEdges.set(edgeId, { ...edges.get(edgeId), via: undefined, isDisplayed: true });
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
