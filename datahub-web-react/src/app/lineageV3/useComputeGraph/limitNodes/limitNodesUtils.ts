import { LINEAGE_FILTER_NODE_NAME } from '@app/lineageV3/LineageFilterNode/LineageFilterNodeBasic';
import { LineageFilter, LineageNode, isTransformational } from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

export interface LevelsInfo {
    [level: number]: {
        totalEntities: number;
        shownEntities: number;
        hiddenEntities: number;
    };
}

// If the lineage node is an entity node
export const isEntityNode = (node: LineageNode, rootType: EntityType, rootUrn?: string) => {
    // Always consider root node
    if (rootUrn && node.id === rootUrn) {
        return true;
    }
    return !isTransformational(node, rootType) && node.type !== LINEAGE_FILTER_NODE_NAME;
};

// Build adjacency that excludes transform nodes and collapses them into direct edges
export function buildAdjacency(
    directionalAdjacency: Map<string, Set<string>>,
    nodes: LineageNode[],
    rootType: EntityType,
    rootUrn: string,
    visibleNodeIds: Set<string>,
    nodeById: Map<string, LineageNode>,
): Map<string, Set<string>> {
    const map = new Map<string, Set<string>>();

    nodes.forEach((node) => {
        const shouldInclude = isEntityNode(node, rootType, rootUrn) || node.type === LINEAGE_FILTER_NODE_NAME;
        if (!shouldInclude) return;

        const collapsedTargets = collapseTransformPath(
            node.id,
            directionalAdjacency,
            visibleNodeIds,
            nodeById,
            rootType,
            rootUrn,
        );

        map.set(node.id, collapsedTargets);
    });

    return map;
}

// Collapse transform nodes until next entity node is reached
export function collapseTransformPath(
    startId: string,
    adjacency: Map<string, Set<string>>,
    visibleNodeIds: Set<string>,
    nodeById: Map<string, LineageNode>,
    rootType: EntityType,
    rootUrn: string,
): Set<string> {
    const result = new Set<string>();
    const stack: string[] = [startId];

    while (stack.length > 0) {
        const currentId = stack.pop();

        if (currentId) {
            const neighbors = adjacency.get(currentId);

            if (neighbors) {
                neighbors.forEach((neighborId) => {
                    if (neighborId === rootUrn) {
                        result.add(neighborId);
                        return;
                    }
                    const node = nodeById.get(neighborId);
                    if (!node || !visibleNodeIds.has(neighborId)) return;

                    if (isTransformational(node, rootType)) {
                        stack.push(neighborId);
                    } else {
                        result.add(neighborId);
                    }
                });
            }
        }
    }

    return result;
}

// BFS assigning levels for entity nodes
export function computeEntityLevels(
    rootUrn: string,
    outgoing: Map<string, Set<string>>,
    incoming: Map<string, Set<string>>,
    entityNodeIds: Set<string>,
): Map<string, number> {
    const levelById = new Map<string, number>();
    const queue: string[] = [];

    levelById.set(rootUrn, 0);
    queue.push(rootUrn);

    const enqueueNeighbors = (neighbors: Set<string> | undefined, nextLevel: number) => {
        if (!neighbors) return;

        neighbors.forEach((childId) => {
            if (!entityNodeIds.has(childId)) return;
            if (levelById.has(childId)) return;

            levelById.set(childId, nextLevel);
            queue.push(childId);
        });
    };

    while (queue.length > 0) {
        const currentId = queue.shift();

        if (currentId) {
            const currentLevel = levelById.get(currentId) ?? 0;

            enqueueNeighbors(outgoing.get(currentId), currentLevel + 1);
            enqueueNeighbors(incoming.get(currentId), currentLevel - 1);
        }
    }

    return levelById;
}

// Assign levels to filter nodes based on their parent
export function assignLevelsToFilterNodes(nodes: LineageNode[], levelsMap: Map<string, number>): void {
    nodes.forEach((node) => {
        if (levelsMap.has(node.id)) return;
        if (node.type !== LINEAGE_FILTER_NODE_NAME) return;

        const parentLevel = levelsMap.get(node.parent);
        if (parentLevel == null) return;

        const filterLevel = node.direction === LineageDirection.Upstream ? parentLevel - 1 : parentLevel + 1;

        levelsMap.set(node.id, filterLevel);
    });
}

// Group nodes by assigned levels
export function groupNodesByLevel(nodes: LineageNode[], levelsMap: Map<string, number>): Record<number, LineageNode[]> {
    return nodes.reduce<Record<number, LineageNode[]>>((acc, node) => {
        const level = levelsMap.get(node.id);
        if (level == null) return acc;

        if (!acc[level]) acc[level] = [];
        acc[level].push(node);

        return acc;
    }, {});
}

// Compute nodes with limited entity nodes and their level info
export function computeLimitedLevels(
    nodesByLevel: Record<number, LineageNode[]>,
    maxPerLevel: number,
    rootType: EntityType,
): { allowedNodeIds: Set<string>; levelsInfo: LevelsInfo } {
    const allowed = new Set<string>();
    const levelsInfo: LevelsInfo = {};

    Object.keys(nodesByLevel)
        .map(Number)
        .sort((a, b) => a - b)
        .forEach((level) => {
            const levelNodes = nodesByLevel[level];
            const entityNodes = levelNodes.filter((n) => isEntityNode(n, rootType));
            const filterNode = levelNodes.find((n) => n.type === LINEAGE_FILTER_NODE_NAME) as LineageFilter | undefined;
            const transformNodes = levelNodes.filter((node) => isTransformational(node, rootType));

            const limitedEntityNodes = entityNodes.slice(0, maxPerLevel);
            limitedEntityNodes.forEach((node) => allowed.add(node.id));

            if (!filterNode) {
                if (entityNodes.length > 0) {
                    const totalEntities = entityNodes.length + transformNodes.length;
                    const shownEntities = limitedEntityNodes.length + transformNodes.length;
                    const hiddenEntities = totalEntities - shownEntities;

                    levelsInfo[level] = {
                        totalEntities,
                        shownEntities,
                        hiddenEntities,
                    };
                }
                return;
            }

            // When a filter node exists
            const totalChildren = filterNode.allChildren?.size ?? 0;
            const shownChildren = filterNode.shown?.size ?? 0;

            const shownEntityNodes = limitedEntityNodes.length;
            const entityIdsShownByFilter = entityNodes.filter((node) => filterNode.shown?.has(node.id));
            const shownEntityNodesFromFilter = entityIdsShownByFilter.length;

            const shownTransformNodes = shownChildren - shownEntityNodesFromFilter;
            const shownEntities = shownEntityNodes + shownTransformNodes;
            const hiddenEntities = Math.max(0, totalChildren - shownEntities);

            levelsInfo[level] = {
                totalEntities: totalChildren,
                shownEntities,
                hiddenEntities,
            };
        });

    return { allowedNodeIds: allowed, levelsInfo };
}

// Assign transform nodes the level of their nearest downstream entity child
export function mergeTransformNodesIntoLevels(
    nodesByLevel: Record<number, LineageNode[]>,
    allNodes: LineageNode[],
    levelsMap: Map<string, number>,
    rootType: EntityType,
    adjacency: Map<string, Set<string>>,
    rootUrn: string,
): Record<number, LineageNode[]> {
    const result: Record<number, LineageNode[]> = Object.fromEntries(
        Object.entries(nodesByLevel).map(([level, nodes]) => [Number(level), [...nodes]]),
    );

    allNodes
        .filter((node) => {
            const isTransform = isTransformational(node, rootType);
            const isFilter = node.type === LINEAGE_FILTER_NODE_NAME;

            return isTransform && !isFilter && node.id !== rootUrn; // Only the non-root transform nodes
        })
        .forEach((node) => {
            const childIds = adjacency.get(node.id);

            if (!childIds || childIds.size === 0) {
                return;
            }

            const childArray = [...childIds];

            // Find first child entity that has a level
            const childLevel = childArray.map((childId) => levelsMap.get(childId)).find((level) => level !== undefined);

            if (childLevel === undefined) {
                return;
            }

            if (!result[childLevel]) {
                result[childLevel] = [];
            }

            result[childLevel].push(node);
        });

    return result;
}
