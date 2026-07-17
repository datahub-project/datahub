import { LineageEntity, NodeContext, isUrnTransformational } from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

export interface OrderNodesOptions {
    /** Overrides the default ordering of a node's children. Order determines priority: pagination
     * keeps the first children when there are too many to show. */
    compareNodes?: (a: string, b: string) => number;
    /** Nodes to seed the traversal with, ordered directly after the root and traversed like its
     * children. Lets a root with no lineage of its own (e.g. a data product) seed the graph with
     * its members. */
    seedNodes?: LineageEntity[];
}

/**
 * Orders nodes in BFS order, starting from the root node.
 * Within a single node, children are ordered transformations last, then alphabetically.
 * Note: Transformations should be put first once show more is put at the bottom.
 * @param urn Root node urn.
 * @param direction Direction in which to perform BFS.
 * @param context Lineage node context.
 * @param options Optional child comparator and seed nodes.
 */
export default function orderNodes(
    urn: string,
    direction: LineageDirection,
    { nodes, adjacencyList, rootType }: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'rootType'>,
    options: OrderNodesOptions = {},
): LineageEntity[] {
    const compareNodes = options.compareNodes ?? generateCompareNodesFunction(rootType);
    const orderedNodes: string[] = [];
    const seenNodes = new Set<string>([urn]);
    const queue = [urn]; // Note: uses array for queue, slow for large graphs
    const visit = (child: string) => {
        if (!seenNodes.has(child)) {
            queue.push(child);
            seenNodes.add(child);
            orderedNodes.push(child);
        }
    };
    options.seedNodes?.forEach((seed) => visit(seed.urn));
    while (queue.length > 0) {
        const current = queue.shift() as string; // Just checked length
        Array.from(adjacencyList[direction].get(current) || [])
            .sort(compareNodes)
            .forEach(visit);
    }
    return orderedNodes.map((id) => nodes.get(id)).filter((node): node is LineageEntity => !!node);
}

export function generateCompareNodesFunction(rootType: EntityType) {
    return function compareNodes(a: string, b: string): number {
        const isATransformation = isUrnTransformational(a, rootType);
        const isBTransformation = isUrnTransformational(b, rootType);
        if (isATransformation && !isBTransformation) return 1;
        if (!isATransformation && isBTransformation) return -1;
        return a.localeCompare(b);
    };
}
