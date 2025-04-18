import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import {
    addToAdjacencyList,
    EdgeId,
    getEdgeId,
    isGhostEntity,
    isTransformational,
    isUrnQuery,
    LineageAuditStamp,
    LineageEdge,
    NodeContext,
    parseEdgeId,
    setDefault,
} from '@app/lineageV2/common';
import { EntityType, LineageDirection } from '@types';

export interface HideNodesConfig {
    hideTransformations: boolean;
    hideDataProcessInstances: boolean;
    hideGhostEntities: boolean;
    ignoreSchemaFieldStatus: boolean;
}

type ContextSubset = Pick<NodeContext, 'nodes' | 'edges' | 'adjacencyList'>;

/**
 * Hide nodes from the graph, connecting edges through the removed nodes.
 */
export default function hideNodes(
    rootUrn: string,
    { hideTransformations, hideDataProcessInstances, hideGhostEntities, ignoreSchemaFieldStatus }: HideNodesConfig,
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
    if (hideDataProcessInstances) {
        // Note: Will only pick one query node if there is lineage t1 -> q1 -> dpi1 -> q2 -> t2
        // Currently data process instances can't have lineage to queries so this is fine
        newNodes = new Map(
            Array.from(newNodes).filter(
                ([urn, node]) => urn === rootUrn || node?.entity?.type !== EntityType.DataProcessInstance,
            ),
        );
        ({ newEdges, newAdjacencyList } = connectEdges(rootUrn, {
            nodes: newNodes,
            edges: newEdges,
            adjacencyList: newAdjacencyList,
        }));
    }
    ({ newEdges, newAdjacencyList } = removeHiddenEdges({
        nodes: newNodes,
        adjacencyList: newAdjacencyList,
        edges: newEdges,
    }));

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
            if (isUrnQuery(neighbor, globalEntityRegistryV2)) {
                return;
            }
            if (nodes.has(neighbor)) {
                addToAdjacencyList(newAdjacencyList, direction, id, neighbor);
                const edgeId = getEdgeId(id, neighbor, direction);
                const existingEdge = newEdges.get(edgeId);
                newEdges.set(edgeId, mergeEdges(edges.get(edgeId), existingEdge));
                buildNewAdjacencyList(neighbor, direction);
            } else {
                buildNewAdjacencyList(neighbor, direction)?.forEach((child) => {
                    addToAdjacencyList(newAdjacencyList, direction, id, child);
                    const edgeId = getEdgeId(id, child, direction);
                    const firstEdge = edges.get(getEdgeId(id, neighbor, direction));
                    const secondEdge = newEdges.get(getEdgeId(neighbor, child, direction));
                    const existingEdge = newEdges.get(edgeId);
                    const newEdge = {
                        isManual: (firstEdge?.isManual || secondEdge?.isManual) ?? false,
                        created: getLatestTimestamp(firstEdge?.created, secondEdge?.created),
                        updated: getLatestTimestamp(firstEdge?.updated, secondEdge?.updated),
                        isDisplayed: (firstEdge?.isDisplayed && secondEdge?.isDisplayed) ?? false,
                        via: firstEdge?.via || secondEdge?.via,
                    };
                    newEdges.set(edgeId, mergeEdges(newEdge, existingEdge));
                });
            }
        });
        return newAdjacencyList[direction].get(id);
    }

    buildNewAdjacencyList(rootUrn, LineageDirection.Upstream);
    seen.clear();
    buildNewAdjacencyList(rootUrn, LineageDirection.Downstream);

    newEdges.forEach((edge, edgeId) => {
        const [upstream, downstream] = parseEdgeId(edgeId);
        if (edge.via && nodes.has(edge.via) && nodes.get(edge.via)?.type === EntityType.Query) {
            setDefault(newAdjacencyList[LineageDirection.Upstream], edge.via, new Set()).add(upstream);
            setDefault(newAdjacencyList[LineageDirection.Downstream], edge.via, new Set()).add(downstream);
        }
    });

    return { newAdjacencyList, newEdges };
}

/** Merge two edges, each representing a different path between two nodes. */
function mergeEdges(edgeA?: LineageEdge, edgeB?: LineageEdge): LineageEdge {
    return {
        isManual: edgeA?.isManual && edgeB?.isManual,
        created: getLatestTimestamp(edgeA?.created, edgeB?.created),
        updated: getLatestTimestamp(edgeA?.updated, edgeB?.updated),
        isDisplayed: (edgeA?.isDisplayed || edgeB?.isDisplayed) ?? false,
        via: edgeA?.via || edgeB?.via,
    };
}

function removeHiddenEdges({ edges }: ContextSubset) {
    const newEdges = new Map<EdgeId, LineageEdge>();
    const newAdjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };

    edges.forEach((edge, edgeId) => {
        const [upstream, downstream] = parseEdgeId(edgeId);
        if (edge.isDisplayed) {
            addToAdjacencyList(newAdjacencyList, LineageDirection.Upstream, downstream, upstream);
            newEdges.set(edgeId, edge);
            if (edge.via) {
                setDefault(newAdjacencyList[LineageDirection.Upstream], edge.via, new Set()).add(upstream);
                setDefault(newAdjacencyList[LineageDirection.Downstream], edge.via, new Set()).add(downstream);
            }
        }
    });
    return { newEdges, newAdjacencyList };
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
