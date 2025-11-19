import { LINEAGE_FILTER_NODE_NAME } from '@app/lineageV3/LineageFilterNode/LineageFilterNodeBasic';
import { LineageEntity, LineageNode, isTransformational } from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

export interface LevelsInfo {
    [level: number]: {
        totalEntities: number;
        shownEntities: number;
        hiddenEntities: number;
    };
}

export interface LimitedGraphResult {
    limitedNodes: LineageNode[];
    levelsInfo: LevelsInfo;
    levelsMap: Map<string, number>;
}

interface Props {
    displayedNodes: LineageNode[];
    originalNodes: LineageEntity[];
    rootUrn: string;
    adjacencyList: Record<LineageDirection, Map<string, Set<string>>>;
    maxPerLevel?: number;
}

export function limitEntityNodesPerLevel({
    displayedNodes,
    originalNodes,
    rootUrn,
    adjacencyList,
    maxPerLevel = 2,
}: Props): LimitedGraphResult {
    const outgoing = adjacencyList[LineageDirection.Downstream];
    const incoming = adjacencyList[LineageDirection.Upstream];

    const levelsMap = new Map<string, number>();

    // BFS downstream
    const downstreamQueue: string[] = [rootUrn];
    levelsMap.set(rootUrn, 0);

    while (downstreamQueue.length) {
        const current = downstreamQueue.shift();
        if (current !== undefined) {
            const level = levelsMap.get(current) ?? 0;
            const children = Array.from(outgoing.get(current) ?? []);

            children.forEach((child) => {
                if (!levelsMap.has(child)) {
                    levelsMap.set(child, level + 1);
                    downstreamQueue.push(child);
                }
            });
        }
    }

    // BFS upstream
    const upstreamQueue: string[] = [rootUrn];
    levelsMap.set(rootUrn, 0);

    while (upstreamQueue.length) {
        const current = upstreamQueue.shift();
        if (current !== undefined) {
            const level = levelsMap.get(current) ?? 0;
            const parents = Array.from(incoming.get(current) ?? []);

            parents.forEach((parent) => {
                if (!levelsMap.has(parent)) {
                    levelsMap.set(parent, level - 1);
                    upstreamQueue.push(parent);
                }
            });
        }
    }

    // Group displayed nodes by level
    const nodesByLevel: Record<number, LineageNode[]> = {};
    displayedNodes.forEach((node) => {
        const level = levelsMap.get(node.id);
        if (level === undefined) return;
        if (!nodesByLevel[level]) nodesByLevel[level] = [];
        nodesByLevel[level].push(node);
    });

    // Group original nodes by level
    const originalByLevel: Record<number, LineageNode[]> = {};
    originalNodes.forEach((node) => {
        const level = levelsMap.get(node.id);
        if (level === undefined) return;
        if (!originalByLevel[level]) originalByLevel[level] = [];
        originalByLevel[level].push(node);
    });

    const allowedUrns = new Set<string>();
    const levelsInfo: LevelsInfo = {};

    Object.keys(originalByLevel)
        .map(Number)
        .sort((a, b) => a - b)
        .forEach((level) => {
            const allOriginalNodes = originalByLevel[level] ?? [];
            const allDisplayedNodes = nodesByLevel[level] ?? [];

            const originalEntities = allOriginalNodes.filter(
                (node) => !isTransformational(node, node.type as EntityType) && node.type !== LINEAGE_FILTER_NODE_NAME,
            );

            const displayedEntities = allDisplayedNodes.filter(
                (node) => !isTransformational(node, node.type as EntityType) && node.type !== LINEAGE_FILTER_NODE_NAME,
            );

            // Limit entity nodes to max per level, keeping transform nodes and removing filter nodes
            const shownEntities = displayedEntities.slice(0, maxPerLevel);
            const hiddenEntities = Math.max(originalEntities.length - shownEntities.length, 0);

            const transformNodes = allDisplayedNodes.filter((node) =>
                isTransformational(node, node.type as EntityType),
            );

            transformNodes.forEach((node) => allowedUrns.add(node.id));
            shownEntities.forEach((node) => allowedUrns.add(node.id));

            levelsInfo[level] = {
                totalEntities: originalEntities.length,
                shownEntities: shownEntities.length,
                hiddenEntities,
            };
        });

    // Filter nodes
    const limitedNodes = displayedNodes.filter((node) => allowedUrns.has(node.id));

    return {
        limitedNodes,
        levelsInfo,
        levelsMap,
    };
}
