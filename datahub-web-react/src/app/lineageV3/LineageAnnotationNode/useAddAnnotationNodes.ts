import { useCallback } from 'react';
import { Node } from 'reactflow';

import { LINEAGE_ANNOTATION_NODE } from '@app/lineageV3/LineageAnnotationNode/LineageAnnotationNode';
import { LINEAGE_ENTITY_NODE_NAME } from '@app/lineageV3/LineageEntityNode/LineageEntityNode';
import type { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';
import { LevelsInfo } from '@app/lineageV3/useComputeGraph/limitNodes/limitNodesUtils';

export function useAddAnnotationNodes() {
    return useCallback(
        (filteredNodes: LineageVisualizationNode[], levelsInfo: LevelsInfo, levelsMap: Map<string, number>) => {
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

                const entityNodesAtLevel = nodesWithLevel.filter(
                    (node) => node.data?.level === level && node.type === LINEAGE_ENTITY_NODE_NAME,
                );

                if (!entityNodesAtLevel || entityNodesAtLevel.length === 0) {
                    return;
                }

                // Compute annotation position over the entity nodes only
                const xs = entityNodesAtLevel.map((node) => node.position.x);
                const ys = entityNodesAtLevel.map((node) => node.position.y);

                const minX = Math.min(...xs);
                const minY = Math.min(...ys);

                const annotationX = minX;
                const annotationY = minY - 40;

                annotationNodes.push({
                    id: `annotation-${level}`,
                    type: LINEAGE_ANNOTATION_NODE,
                    position: { x: annotationX, y: annotationY },
                    data: {
                        label: `${levelInfo.shownEntities} of ${levelInfo.totalEntities} shown`,
                    },
                    selectable: false,
                    connectable: false,
                });
            });

            return [...nodesWithLevel, ...annotationNodes];
        },
        [],
    );
}

export default useAddAnnotationNodes;
