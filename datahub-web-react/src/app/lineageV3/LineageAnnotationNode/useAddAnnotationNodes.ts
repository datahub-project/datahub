import { useCallback } from 'react';
import { Edge, Node } from 'reactflow';

import { LINEAGE_ANNOTATION_NODE } from '@app/lineageV3/LineageAnnotationNode/LineageAnnotationNode';
import type { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import { LevelsInfo } from '@app/lineageV3/useComputeGraph/limitNodesPerLevel';

export function useAddAnnotationNodes() {
    return useCallback(
        (
            filteredNodes: LineageVisualizationNode[],
            filteredEdges: Edge[],
            levelsInfo: LevelsInfo,
            levelsMap: Map<string, number>,
        ) => {
            const nodesWithLevel = filteredNodes.map((node) => {
                const urn = node.id;
                const nodeLevel = levelsMap.get(urn) ?? 0;

                return {
                    ...node,
                    data: {
                        ...node.data,
                        level: nodeLevel,
                    },
                };
            });

            const annotationNodes: Node[] = [];

            Object.entries(levelsInfo).forEach(([levelString, infoValue]) => {
                const level = Number(levelString);
                const levelInfo = infoValue as LevelsInfo[number];

                if (levelInfo.hiddenEntities <= 0) return;

                const nodesAtLevel = nodesWithLevel.filter((node) => node.data?.level === level);
                if (nodesAtLevel.length === 0) return;

                const minX = Math.min(...nodesAtLevel.map((node) => node.position.x));
                const minY = Math.min(...nodesAtLevel.map((node) => node.position.y));

                annotationNodes.push({
                    id: `annotation-${level}`,
                    type: LINEAGE_ANNOTATION_NODE,
                    position: { x: minX, y: minY - 40 },
                    data: {
                        label: `${levelInfo.shownEntities} of ${levelInfo.totalEntities} shown`,
                    },
                    draggable: false,
                    selectable: false,
                    connectable: false,
                });
            });

            return {
                nodes: [...nodesWithLevel, ...annotationNodes],
                edges: filteredEdges,
                levelsInfo,
            };
        },
        [],
    );
}

export default useAddAnnotationNodes;
