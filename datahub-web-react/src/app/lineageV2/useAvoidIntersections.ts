import { useCallback, useEffect, useState } from 'react';
import { Node, useReactFlow } from 'reactflow';
import { NodeBase } from './common';
import { LINEAGE_NODE_WIDTH } from './LineageEntityNode/useDisplayedColumns';

interface ReturnType {
    onNodeHeightChange: (id: string, height: number) => void;
}

/**
 * Pushes down nodes when node height changes, to avoid overlapping nodes.
 * Returns function onNodeHeightChange(id, y, height) to be called when a node's height changes.
 * TODO: Consider using over per-node useAvoidIntersections for performance. Not complete.
 */
export default function useAvoidIntersections(vizVersion: number): ReturnType {
    const { getNodes, setNodes } = useReactFlow<NodeBase & { offset?: number }>();

    // Build up tree of dependencies, where nodes a -> b means b may need to be pushed down if a's height increases
    const [dependencies] = useState(new Map<string, Set<string>>());

    useEffect(() => {
        // Reminder: Could optimize by calculating minYIntervals: [number, number][]
        // of shape [(x, y)] that states the minimum y value for each x interval
        // to run in O(n) time instead of O(n^2)
        const nodes = getNodes().sort((a, b) => a.position.y - b.position.y);
        nodes.forEach((node, i) => {
            const overlaps = nodes.slice(i + 1).filter((n) => overlapsX(node, n));
            dependencies.set(node.id, new Set(overlaps.map((n) => n.id)));
        });
    }, [vizVersion, dependencies, getNodes]);

    const onNodeHeightChange = useCallback(
        (id: string, height: number) => {
            const allNodes = getNodes();
            const y = allNodes.find((n) => n.id === id)?.position.y || 0;
            const mayOverlapNodes = allNodes
                .filter((n) => dependencies.get(id)?.has(n.id))
                .sort((a, b) => a.position.y - b.position.y);
            let currentY = y + height;

            const offsets = new Map<string, number>();
            mayOverlapNodes.forEach((node) => {
                const distance = pushDownDistance(currentY, node.position.y);
                if (distance) {
                    currentY = node.position.y + distance + (node.height || 0);
                }
                offsets.set(node.id, distance);
            });

            setNodes((oldNodes) =>
                oldNodes.map((node) => {
                    const newOffset = offsets.get(node.id) || 0;
                    const changeInY = newOffset - (node.data.offset || 0);
                    if (changeInY) {
                        return {
                            ...node,
                            position: {
                                ...node.position,
                                y: node.position.y + changeInY,
                            },
                            data: {
                                ...node.data,
                                offset: newOffset,
                            },
                        };
                    }
                    return node;
                }),
            );
        },
        [dependencies, getNodes, setNodes],
    );
    return { onNodeHeightChange };
}

const MIN_Y_SEP = 10;

function pushDownDistance(newY: number, nodeY: number): number {
    return Math.max(0, newY + MIN_Y_SEP - nodeY);
}

function overlapsX(a: Node, b: Node): boolean {
    return (
        Math.min(a.position.x + (a.width || LINEAGE_NODE_WIDTH), b.position.x + (b.width || LINEAGE_NODE_WIDTH)) >
        Math.max(a.position.x, b.position.x)
    );
}
