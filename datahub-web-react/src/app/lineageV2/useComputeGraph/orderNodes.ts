import { isUrnTransformational, LineageEntity, NodeContext } from '@app/lineageV2/common';
import { EntityType, LineageDirection } from '@types';

/**
 * Orders nodes in BFS order, starting from the root node.
 * Within a single node, children are ordered transformations last, then alphabetically.
 * Note: Transformations should be put first once show more is put at the bottom.
 * @param urn Root node urn.
 * @param direction Direction in which to perform BFS.
 * @param context Lineage node context.
 */
export default function orderNodes(
    urn: string,
    direction: LineageDirection,
    { nodes, adjacencyList, rootType }: Pick<NodeContext, 'adjacencyList' | 'nodes' | 'rootType'>,
): LineageEntity[] {
    const compareNodes = generateCompareNodesFunction(rootType);
    const orderedNodes: string[] = [];
    const seenNodes = new Set<string>([urn]);
    const queue = [urn]; // Note: uses array for queue, slow for large graphs
    while (queue.length > 0) {
        const current = queue.shift() as string; // Just checked length
        const children = Array.from(adjacencyList[direction].get(current) || []).sort(compareNodes);
        children?.forEach((child) => {
            if (!seenNodes.has(child)) {
                queue.push(child);
                seenNodes.add(child);
                orderedNodes.push(child);
            }
        });
    }
    return orderedNodes.map((id) => nodes.get(id)).filter((node): node is LineageEntity => !!node);
}

function generateCompareNodesFunction(rootType: EntityType) {
    return function compareNodes(a: string, b: string): number {
        const isATransformation = isUrnTransformational(a, rootType);
        const isBTransformation = isUrnTransformational(b, rootType);
        if (isATransformation && !isBTransformation) return 1;
        if (!isATransformation && isBTransformation) return -1;
        return a.localeCompare(b);
    };
}
