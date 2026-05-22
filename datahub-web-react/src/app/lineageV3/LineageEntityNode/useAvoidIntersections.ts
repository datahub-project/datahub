import { ReactFlowInstance } from '@reactflow/core/dist/esm/types';
import { useContext, useEffect } from 'react';
import { Node, useReactFlow } from 'reactflow';

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
        if (!skip) {
            avoidIntersections({ id, expandHeight, rootType, getNode, getNodes, setNodes });
        }
    }, [id, expandHeight, rootType, getNode, getNodes, setNodes, skip]);
}

/**
 * For nodes that live inside a parent container (e.g. assets inside a DataProduct that's itself
 * inside a Domain bbox), expanding their contents must propagate up the full ancestor chain:
 *
 *   - Siblings below the node within its immediate parent shift down.
 *   - The immediate parent's height grows so the expanded content stays inside its bbox.
 *   - The same applies one level up — the parent's siblings (e.g. other DPs / direct-Domain
 *     assets stacked below it inside the Domain bbox) shift down, and the grandparent (Domain)
 *     grows. This continues until we reach a root node without a parent.
 *
 * Previously this only walked one level up, which meant the Domain bbox stayed at its layout-time
 * height even when an asset inside one of its DPs expanded — content overflowed and DPs below the
 * expanded one overlapped instead of shifting.
 *
 * The cleanup reverses every shift / grow in the same chain so the layout is stable across
 * mount/unmount cycles and collapse toggles.
 */
export function useExpandContainer(id: string, expandHeight: number, skip = false) {
    const { getNode, getNodes, setNodes } = useReactFlow();

    useEffect(() => {
        if (skip) return () => {};

        const self = getNode(id);
        if (!self?.parentId) return () => {};

        const extraHeight = expandHeight - LINEAGE_NODE_HEIGHT;
        if (extraHeight <= 0) return () => {};

        const siblingIdSet = new Set<string>();
        const parentIdSet = new Set<string>();
        let current: ReturnType<typeof getNode> = self;
        while (current?.parentId) {
            const { parentId } = current;
            const currentNode = current;
            getNodes()
                .filter(
                    (n) => n.parentId === parentId && n.id !== currentNode.id && n.position.y > currentNode.position.y,
                )
                .forEach((n) => siblingIdSet.add(n.id));
            parentIdSet.add(parentId);
            current = getNode(parentId);
        }

        setNodes((nodes) =>
            nodes.map((node) => {
                if (siblingIdSet.has(node.id)) {
                    return { ...node, position: { ...node.position, y: node.position.y + extraHeight } };
                }
                if (parentIdSet.has(node.id)) {
                    const newHeight = (node.height || 0) + extraHeight;
                    return {
                        ...node,
                        height: newHeight,
                        style: { ...node.style, height: newHeight, transition: 'height 0.3s ease-in-out' },
                    };
                }
                return node;
            }),
        );

        return () => {
            setNodes((nodes) =>
                nodes.map((node) => {
                    if (siblingIdSet.has(node.id)) {
                        return { ...node, position: { ...node.position, y: node.position.y - extraHeight } };
                    }
                    if (parentIdSet.has(node.id)) {
                        const newHeight = (node.height || 0) - extraHeight;
                        return {
                            ...node,
                            height: newHeight,
                            style: { ...node.style, height: newHeight, transition: 'height 0.3s ease-in-out' },
                        };
                    }
                    return node;
                }),
            );
        };
    }, [id, expandHeight, getNode, getNodes, setNodes, skip]);
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

    // Nodes nested inside a parent container (e.g. members of a DataProduct bounding box) are
    // laid out relative to the parent — pushing other absolute-positioned nodes around would
    // double-count their offset. The parent container itself is grown by useExpandContainer.
    if (self.parentId) {
        return () => {};
    }

    const nodesToMove: Map<string, number> = new Map();
    // Iterate nodes top down
    const nodes = getNodes()
        .filter((node) => !isTransformational(node.data, rootType))
        .filter((node) => !node.parentId)
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
