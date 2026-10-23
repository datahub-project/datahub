import { FetchStatus, LINEAGE_FILTER_TYPE, LineageEntity, LineageNode, NodeContext } from '@app/lineageV3/common';

import { LineageDirection } from '@types';

/**
 * Hides nodes known to have no lineage: fetched, with no lineage update in flight, and no edges.
 * Hides nothing until a home member has lineage, so the home box and "Show more" stay visible.
 */
export function hideOrphanedNodes(
    shownNodes: LineageNode[],
    rootUrn: string,
    homeMemberUrns: Set<string>,
    nodes: NodeContext['nodes'],
    adjacencyList: NodeContext['adjacencyList'],
): LineageNode[] {
    const hasLineage = (id: string) =>
        !!adjacencyList[LineageDirection.Upstream].get(id)?.size ||
        !!adjacencyList[LineageDirection.Downstream].get(id)?.size;

    if (!shownNodes.some((node) => homeMemberUrns.has(node.id) && hasLineage(node.id))) {
        return shownNodes;
    }
    return shownNodes.filter(
        (node) =>
            node.id === rootUrn ||
            node.type === LINEAGE_FILTER_TYPE ||
            !isLineageKnown(nodes.get(node.id)) ||
            hasLineage(node.id),
    );
}

/** Whether a node's lineage has been fetched and no lineage update is in flight. */
export function isLineageKnown(node: LineageEntity | undefined): boolean {
    return (
        !!node?.entity &&
        node.fetchStatus[LineageDirection.Upstream] !== FetchStatus.LOADING &&
        node.fetchStatus[LineageDirection.Downstream] !== FetchStatus.LOADING
    );
}
