import { LINEAGE_FILTER_NODE_NAME } from '@app/lineageV3/LineageFilterNode/LineageFilterNodeBasic';
import { LineageNode, isTransformational } from '@app/lineageV3/common';

import { EntityType, LineageDirection } from '@types';

export interface LevelsInfo {
    [level: number]: {
        totalEntities: number;
        shownEntities: number;
        hiddenEntities: number;
    };
}

export interface LimitedGraphResult {
    nodes: LineageNode[];
    levelsInfo: LevelsInfo;
    levelsMap: Map<string, number>;
}

interface Props {
    nodes: LineageNode[];
    rootUrn: string;
    adjacencyList: Record<LineageDirection, Map<string, Set<string>>>;
    maxPerLevel?: number;
}

export function limitEntityNodesPerLevel({
    nodes,
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

    // Group nodes by level
    const nodesByLevel: Record<number, LineageNode[]> = {};
    nodes.forEach((node) => {
        const level = levelsMap.get(node.id);
        if (level === undefined) return;
        if (!nodesByLevel[level]) nodesByLevel[level] = [];
        nodesByLevel[level].push(node);
    });

    const allowed = new Set<string>();
    const levelsInfo: LevelsInfo = {};

    Object.keys(nodesByLevel)
        .map(Number)
        .sort((a, b) => a - b)
        .forEach((level) => {
            const allNodes = nodesByLevel[level];

            // Separate nodes by type
            const transformNodes = allNodes.filter((node) => isTransformational(node, node.type as EntityType));
            const entityNodes = allNodes.filter(
                (node) => !isTransformational(node, node.type as EntityType) && node.type !== LINEAGE_FILTER_NODE_NAME,
            );

            // Limit entity nodes to max per level, keeping transform nodes and removing filter nodes
            const shownEntities = entityNodes.slice(0, maxPerLevel);
            const hiddenEntities = Math.max(entityNodes.length - shownEntities.length, 0);

            transformNodes.forEach((node) => allowed.add(node.id));
            shownEntities.forEach((node) => allowed.add(node.id));

            levelsInfo[level] = {
                totalEntities: entityNodes.length,
                shownEntities: shownEntities.length,
                hiddenEntities,
            };
        });

    // Filter nodes
    const filteredNodes = nodes.filter((node) => allowed.has(node.id));

    return {
        nodes: filteredNodes,
        levelsInfo,
        levelsMap,
    };
}
