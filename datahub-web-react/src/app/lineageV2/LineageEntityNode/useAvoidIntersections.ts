import { ReactFlowInstance } from '@reactflow/core/dist/esm/types';
import { useEffect } from 'react';
import { Node, useReactFlow } from 'reactflow';
import { TRANSFORMATION_TYPES } from '../common';
import { LINEAGE_NODE_HEIGHT, LINEAGE_NODE_WIDTH } from './useDisplayedColumns';

export default function useAvoidIntersections(urn: string, expandHeight: number) {
    const { getNode, getNodes, setNodes } = useReactFlow();

    useEffect(() => {
        const self = getNode(urn);
        if (!self) {
            return () => {};
        }

        const nodesToMove: Map<string, number> = new Map();
        // Iterate nodes top down
        const nodes = getNodes()
            .filter((node) => !TRANSFORMATION_TYPES.includes(node.data.type))
            .filter((node) => node.id !== self.id && node.position.y >= self.position.y && overlapsX(self, node));
        nodes.sort((a, b) => a.position.y - b.position.y);

        let newY = self.position.y + expandHeight;
        // eslint-disable-next-line no-restricted-syntax -- so I can break
        for (const node of nodes) {
            const distance = pushDownDistance(newY, node.position.y);
            if (pushDownDistance(newY, node.position.y)) {
                nodesToMove.set(node.id, distance);
                newY = node.position.y + distance + (node.height || LINEAGE_NODE_HEIGHT);
            } else {
                break;
            }
        }
        if (nodesToMove.size) {
            moveNodes(setNodes, nodesToMove, true);
            return function moveBack() {
                moveNodes(setNodes, nodesToMove, false);
            };
        }
        return () => {};
    }, [urn, expandHeight, getNode, getNodes, setNodes]);
}

function overlapsX(a: Node, b: Node): boolean {
    return (
        Math.min(a.position.x + (a.width || LINEAGE_NODE_WIDTH), b.position.x + (b.width || LINEAGE_NODE_WIDTH)) >
        Math.max(a.position.x, b.position.x)
    );
}

const MIN_SEPARATION = 10;

function pushDownDistance(newY: number, nodeY: number): number {
    return Math.max(0, newY + MIN_SEPARATION - nodeY);
}

function moveNodes(setNodes: ReactFlowInstance['setNodes'], nodesToMove: Map<string, number>, down: boolean) {
    setNodes((nodes) =>
        nodes.map((node) => {
            const moveAmount = nodesToMove.get(node.id);
            // TODO: Improve interaction with selected nodes? Lacking transition
            if (moveAmount) {
                return {
                    ...node,
                    position: {
                        ...node.position,
                        y: node.position.y + (down ? moveAmount : -moveAmount),
                    },
                };
            }
            return node;
        }),
    );
}
