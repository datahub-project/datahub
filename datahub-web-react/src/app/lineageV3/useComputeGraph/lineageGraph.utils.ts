import { LINEAGE_FILTER_TYPE, LineageNode, NodeContext } from '@app/lineageV3/common';

import { LineageDirection } from '@types';

/**
 * Returns a set of node IDs that have at least one lineage edge (either upstream or downstream)
 * to another node. Nodes without any connections are considered orphaned.
 */
export function getNodesWithLineage(
    nodeIds: (string | undefined)[],
    adjacencyList: NodeContext['adjacencyList'],
): Set<string> {
    const nodesWithLineage = new Set<string>();

    nodeIds.forEach((nodeId) => {
        if (!nodeId) return;

        const hasUpstream = (adjacencyList[LineageDirection.Upstream].get(nodeId)?.size ?? 0) > 0;
        const hasDownstream = (adjacencyList[LineageDirection.Downstream].get(nodeId)?.size ?? 0) > 0;

        if (hasUpstream || hasDownstream) {
            nodesWithLineage.add(nodeId);
        }
    });

    return nodesWithLineage;
}

/**
 * Hides nodes known to have no lineage. A node's lineage is known once its bulk lineage fetch has set
 * `entity`; unfetched nodes stay shown, since the bulk fetch only requests displayed nodes. Nothing is
 * hidden until a home member is known to have lineage, so the home box and its "Show more" control stay
 * visible, and orphans aren't hidden then re-shown while a page of members loads.
 */
export function hideOrphanedNodes(
    shownNodes: LineageNode[],
    rootUrn: string,
    homeMemberUrns: Set<string>,
    nodes: NodeContext['nodes'],
    adjacencyList: NodeContext['adjacencyList'],
): LineageNode[] {
    const nodesWithLineage = getNodesWithLineage(
        shownNodes.map((node) => node.id),
        adjacencyList,
    );
    if (!shownNodes.some((node) => homeMemberUrns.has(node.id) && nodesWithLineage.has(node.id))) {
        return shownNodes;
    }
    return shownNodes.filter(
        (node) =>
            node.id === rootUrn ||
            node.type === LINEAGE_FILTER_TYPE ||
            !nodes.get(node.id)?.entity ||
            nodesWithLineage.has(node.id),
    );
}
