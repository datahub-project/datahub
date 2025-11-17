import { useCallback } from 'react';
import { Edge } from 'reactflow';

import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import { LINEAGE_TRANSFORMATION_NODE_NAME } from '@app/lineageV3/LineageTransformationNode/LineageTransformationNode';
import type { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';

export interface LevelsInfo {
    [level: number]: {
        totalEntities: number;
        shownEntities: number;
        hiddenEntities: number;
    };
}

export interface LimitedGraphResult {
    nodes: LineageVisualizationNode[];
    edges: Edge[];
    levelsInfo: LevelsInfo;
    levelsMap: Map<string, number>;
}

export function useLimitNodesPerLevel() {
    return useCallback(
        (nodes: LineageVisualizationNode[], edges: Edge[], rootUrn: string, maxPerLevel = 2): LimitedGraphResult => {
            // Build adjacency maps
            const outgoing = new Map<string, string[]>();
            const incoming = new Map<string, string[]>();

            edges.forEach(({ source, target }) => {
                const sourceId = String(source);
                const targetId = String(target);

                const outgoingList = outgoing.get(sourceId) ?? [];
                outgoing.set(sourceId, [...outgoingList, targetId]);

                const incomingList = incoming.get(targetId) ?? [];
                incoming.set(targetId, [...incomingList, sourceId]);
            });

            // BFS downstream
            const downLevel = new Map<string, number>();
            downLevel.set(rootUrn, 0);
            const downstreamQueue: string[] = [rootUrn];

            while (downstreamQueue.length > 0) {
                const current = downstreamQueue.shift();

                if (current !== undefined) {
                    const level = downLevel.get(current) ?? 0;
                    const children = outgoing.get(current) ?? [];

                    children.forEach((child) => {
                        if (!downLevel.has(child)) {
                            downLevel.set(child, level + 1);
                            downstreamQueue.push(child);
                        }
                    });
                }
            }

            // BFS upstream
            const upLevel = new Map<string, number>();
            upLevel.set(rootUrn, 0);
            const upstreamQueue: string[] = [rootUrn];

            while (upstreamQueue.length > 0) {
                const current = upstreamQueue.shift();

                if (current !== undefined) {
                    const level = upLevel.get(current) ?? 0;
                    const parents = incoming.get(current) ?? [];

                    parents.forEach((parent) => {
                        if (!upLevel.has(parent)) {
                            upLevel.set(parent, level - 1);
                            upstreamQueue.push(parent);
                        }
                    });
                }
            }

            // Create levelsMap
            const levelsMap = new Map<string, number>();
            upLevel.forEach((v, k) => levelsMap.set(k, v));
            downLevel.forEach((v, k) => levelsMap.set(k, v));

            // Group nodes by level
            const nodesByLevel: Record<number, LineageVisualizationNode[]> = {};
            nodes.forEach((node) => {
                const level = levelsMap.get(node.id);
                if (level === undefined) return;
                if (!nodesByLevel[level]) nodesByLevel[level] = [];
                nodesByLevel[level].push(node);
            });

            const allowed = new Set<string>();
            const levelsInfo: LevelsInfo = {};

            const sortedLevels = Object.keys(nodesByLevel)
                .map(Number)
                .sort((a, b) => a - b);

            sortedLevels.forEach((level) => {
                const all = nodesByLevel[level] ?? [];
                const entityNodes = all.filter((n) => n.type === LINEAGE_ENTITY_NODE_NAME);
                const transformationNodes = all.filter((n) => n.type === LINEAGE_TRANSFORMATION_NODE_NAME);

                const shownEntities = entityNodes.slice(0, maxPerLevel);
                const hiddenEntities = Math.max(entityNodes.length - shownEntities.length, 0);

                shownEntities.forEach((n) => allowed.add(n.id));
                transformationNodes.forEach((n) => allowed.add(n.id));

                levelsInfo[level] = {
                    totalEntities: entityNodes.length,
                    shownEntities: shownEntities.length,
                    hiddenEntities,
                };
            });

            // Filter nodes & edges
            const filteredNodes = nodes.filter((node) => allowed.has(node.id));
            const filteredEdges = edges.filter(
                (edge) => allowed.has(String(edge.source)) && allowed.has(String(edge.target)),
            );

            return {
                nodes: filteredNodes,
                edges: filteredEdges,
                levelsInfo,
                levelsMap,
            };
        },
        [],
    );
}

export default useLimitNodesPerLevel;
