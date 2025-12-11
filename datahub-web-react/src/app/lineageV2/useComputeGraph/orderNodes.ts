/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { globalEntityRegistryV2 } from '@app/EntityRegistryProvider';
import { LineageEntity, NodeContext, isUrnTransformational } from '@app/lineageV2/common';

import { LineageDirection } from '@types';

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
    { nodes, adjacencyList }: Pick<NodeContext, 'adjacencyList' | 'nodes'>,
): LineageEntity[] {
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

function compareNodes(a: string, b: string): number {
    const isATransformation = isUrnTransformational(a, globalEntityRegistryV2);
    const isBTransformation = isUrnTransformational(b, globalEntityRegistryV2);
    if (isATransformation && !isBTransformation) return 1;
    if (!isATransformation && isBTransformation) return -1;
    return a.localeCompare(b);
}
