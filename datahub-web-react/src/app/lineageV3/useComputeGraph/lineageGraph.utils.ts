import { NodeContext } from '@app/lineageV3/common';

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
