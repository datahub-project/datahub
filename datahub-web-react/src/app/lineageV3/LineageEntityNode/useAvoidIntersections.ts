import { ReactFlowInstance } from '@reactflow/core/dist/esm/types';
import { useContext, useEffect } from 'react';
import { Node, XYPosition, useReactFlow } from 'reactflow';

import {
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageNode,
    LineageNodesContext,
    isTransformational,
} from '@app/lineageV3/common';

import { EntityType } from '@types';

export default function useAvoidIntersections(id: string, expandHeight: number, rootType: EntityType, skip = false) {
    const { getNode, getNodes, setNodes } = useReactFlow();

    useEffect(() => {
        if (skip) return undefined;
        // Returned as the effect cleanup, so nodes move back on retract (or unmount)
        return avoidIntersections({ id, expandHeight, rootType, getNode, getNodes, setNodes });
    }, [id, expandHeight, rootType, getNode, getNodes, setNodes, skip]);
}

// Required because NodeBuilder cannot properly place Lineage Filter nodes
// TODO: Find a cleaner way to do this
export function useAvoidIntersectionsOften(id: string, expandHeight: number, rootType: EntityType, skip = false) {
    const { getNode, getNodes, setNodes } = useReactFlow();
    const { nodeVersion, displayVersion } = useContext(LineageNodesContext);

    const displayVersionNumber = displayVersion[0];
    useEffect(() => {
        if (!skip) {
            const timeout = setTimeout(
                () => avoidIntersections({ id, expandHeight, rootType, getNode, getNodes, setNodes }),
                0,
            );
            return () => clearTimeout(timeout);
        }
        return () => {};
    }, [id, expandHeight, rootType, getNode, getNodes, setNodes, nodeVersion, displayVersionNumber, skip]);
}

type Arguments = { id: string; expandHeight: number; rootType: EntityType } & Pick<
    ReactFlowInstance<LineageNode>,
    'getNode' | 'getNodes' | 'setNodes'
>;

function avoidIntersections({ id, expandHeight, rootType, getNode, getNodes, setNodes }: Arguments) {
    const self = getNode(id);
    if (!self) {
        return () => {};
    }
    const selfPosition = absolutePosition(self);

    const nodesToMove: Map<string, number> = new Map();
    // Iterate nodes top down, comparing absolute positions so nodes inside bounding boxes are
    // comparable to top-level nodes. Children of other bounding boxes are skipped: they move with
    // their parent box; children of the same box move individually. The node's own bounding box is
    // never pushed: it resizes to fit its contents instead, and pushing it would move the node
    // itself (and double-move its pushed siblings) along with it.
    const nodes = getNodes()
        .filter((node) => !isTransformational(node.data, rootType))
        .filter((node) => (!node.parentId || node.parentId === self.parentId) && node.id !== self.parentId)
        .filter((node) => node.id !== self.id && absolutePosition(node).y >= selfPosition.y && overlapsX(self, node));
    nodes.sort((a, b) => absolutePosition(a).y - absolutePosition(b).y);

    let newY = selfPosition.y + expandHeight;
    // eslint-disable-next-line no-restricted-syntax -- so I can break
    for (const node of nodes) {
        const nodeY = absolutePosition(node).y;
        const distance = pushDownDistance(newY, nodeY);
        if (distance) {
            nodesToMove.set(node.id, distance);
            newY = nodeY + distance + (node.height || LINEAGE_NODE_HEIGHT);
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
}

function absolutePosition(node: Node): XYPosition {
    return node.positionAbsolute ?? node.position;
}

function overlapsX(a: Node, b: Node): boolean {
    const aPosition = absolutePosition(a);
    const bPosition = absolutePosition(b);
    return (
        Math.min(aPosition.x + (a.width || LINEAGE_NODE_WIDTH), bPosition.x + (b.width || LINEAGE_NODE_WIDTH)) >
        Math.max(aPosition.x, bPosition.x)
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
