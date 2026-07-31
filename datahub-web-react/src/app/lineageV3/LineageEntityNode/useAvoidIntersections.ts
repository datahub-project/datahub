import { ReactFlowInstance } from '@reactflow/core/dist/esm/types';
import { MutableRefObject, useContext, useEffect, useRef } from 'react';
import { Node, XYPosition, useReactFlow } from 'reactflow';

import {
    BOUNDING_BOX_LABEL_HEIGHT,
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import {
    LINEAGE_NODE_HEIGHT,
    LINEAGE_NODE_WIDTH,
    LineageDisplayContext,
    LineageEntity,
    LineageNode,
    LineageNodesContext,
    isTransformational,
    parseColumnRef,
} from '@app/lineageV3/common';
import { MAIN_Y_SEP_RATIO } from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType } from '@types';

const MIN_SEPARATION = 10;

/**
 * The urn of the entity whose column is hovered, which must be left where it is: pushing it out
 * from under the cursor ends the hover, which moves it back, which starts the hover again.
 *
 * Selecting a column pins the highlight regardless of where the pointer is, so there is no hover to
 * disturb and nodes can move freely -- hence `undefined` while a column is selected.
 *
 * Kept in a ref so that merely hovering does not re-run every node's effect. It is read whenever an
 * effect runs for its own reasons, which is exactly when a hover has resized some node.
 */
function useHoveredColumnPin() {
    const { selectedColumn, hoveredColumn } = useContext(LineageDisplayContext);
    const urn = !selectedColumn && hoveredColumn ? parseColumnRef(hoveredColumn)[0] : undefined;
    const ref = useRef(urn);
    ref.current = urn;
    // Returned so effects can depend on selection: on select the pin lifts and the layout settles
    return { pinnedUrn: ref, selectedColumn };
}

export default function useAvoidIntersections(id: string, expandHeight: number, rootType: EntityType, skip = false) {
    const { getNode, getNodes, setNodes } = useReactFlow();
    const { pinnedUrn, selectedColumn } = useHoveredColumnPin();

    useEffect(() => {
        if (skip) return undefined;
        // Returned as the effect cleanup, so nodes move back on retract (or unmount)
        return avoidIntersections({
            id,
            expandHeight,
            rootType,
            pinnedUrn,
            getNode,
            getNodes,
            setNodes,
        });
    }, [id, expandHeight, rootType, getNode, getNodes, setNodes, skip, selectedColumn, pinnedUrn]);
}

// Required because NodeBuilder cannot properly place Lineage Filter nodes
// TODO: Find a cleaner way to do this
export function useAvoidIntersectionsOften(id: string, expandHeight: number, rootType: EntityType, skip = false) {
    const { getNode, getNodes, setNodes } = useReactFlow();
    const { nodeVersion, displayVersion } = useContext(LineageNodesContext);
    const { pinnedUrn, selectedColumn } = useHoveredColumnPin();

    const displayVersionNumber = displayVersion[0];
    useEffect(() => {
        if (!skip) {
            const timeout = setTimeout(
                () =>
                    avoidIntersections({
                        id,
                        expandHeight,
                        rootType,
                        pinnedUrn,
                        getNode,
                        getNodes,
                        setNodes,
                    }),
                0,
            );
            return () => clearTimeout(timeout);
        }
        return () => {};
    }, [
        id,
        expandHeight,
        rootType,
        getNode,
        getNodes,
        setNodes,
        nodeVersion,
        displayVersionNumber,
        skip,
        selectedColumn,
        pinnedUrn,
    ]);
}

type Arguments = {
    id: string;
    expandHeight: number;
    rootType: EntityType;
    /** Entity whose node must not be moved; see `useHoveredColumnPin`. */
    pinnedUrn: MutableRefObject<string | undefined>;
} & Pick<ReactFlowInstance<LineageNode>, 'getNode' | 'getNodes' | 'setNodes'>;

export function avoidIntersections({ id, expandHeight, rootType, pinnedUrn, getNode, getNodes, setNodes }: Arguments) {
    const self = getNode(id);
    if (!self) {
        return () => {};
    }
    const selfPosition = absolutePosition(self);
    const allNodes = getNodes();
    const pinnedIds = pinnedNodeIds(allNodes, pinnedUrn.current);

    // Compare absolute positions so nodes inside bounding boxes are comparable to top-level nodes.
    // Children of other bounding boxes are skipped: they move with their parent box; children of
    // the same box move individually. The node's own bounding box is never pushed: it resizes to
    // fit its contents instead, and pushing it would move the node itself (and double-move its
    // pushed siblings) along with it.
    const candidates = allNodes
        .filter((node) => !isTransformational(node.data, rootType))
        .filter((node) => (!node.parentId || node.parentId === self.parentId) && node.id !== self.parentId)
        .filter((node) => node.id !== self.id && absolutePosition(node).y >= selfPosition.y && overlapsX(self, node))
        .filter((node) => !pinnedIds.has(node.id))
        .sort((a, b) => absolutePosition(a).y - absolutePosition(b).y);

    const selfBottom = selfPosition.y + expandHeight;
    const box = self.parentId ? getNode(self.parentId) : undefined;

    const nodesToMove: Map<string, number> = new Map();
    let previousBottom = -Infinity;
    // Push each node below both the node above it and the edge it has to clear, stopping at the
    // first one that already has room, as everything below it does too.
    // eslint-disable-next-line no-restricted-syntax -- so I can break
    for (const node of candidates) {
        const nodeY = absolutePosition(node).y;
        // Nodes sharing self's container only have to clear self's own expanded bottom edge. Nodes
        // outside self's bounding box have to clear the box, which grows to wrap its members as
        // they expand or get pushed down (see `useFitToContents`) -- clearing self alone would
        // leave the box overlapping them. Recomputed per node so it picks up siblings pushed above.
        const floor =
            box && node.parentId !== self.parentId
                ? boundingBoxBottom(box, allNodes, self, expandHeight, nodesToMove)
                : selfBottom;
        const distance = Math.max(0, Math.max(previousBottom, floor) + separationAbove(node) - nodeY);
        if (!distance) break;
        nodesToMove.set(node.id, distance);
        previousBottom = nodeY + distance + (node.height || LINEAGE_NODE_HEIGHT);
    }

    if (nodesToMove.size) {
        moveNodes(setNodes, nodesToMove, true);
        return function moveBack() {
            moveNodes(setNodes, nodesToMove, false);
        };
    }
    return () => {};
}

/**
 * Nodes rendering `pinnedUrn`, plus the bounding boxes holding them, as a member moves with its box.
 * An entity in several data products renders once per product, so all of its nodes are pinned.
 */
function pinnedNodeIds(allNodes: Node[], pinnedUrn: string | undefined): Set<string> {
    const pinned = new Set<string>();
    if (!pinnedUrn) return pinned;
    allNodes.forEach((node) => {
        if ((node.data as LineageEntity)?.urn !== pinnedUrn) return;
        pinned.add(node.id);
        if (node.parentId) pinned.add(node.parentId);
    });
    return pinned;
}

/**
 * Gap to leave above a node. Bounding boxes render a label above their top edge and are spaced out
 * more than plain nodes, so they keep the separation the initial layout gives them
 * (see `BoundingBoxNodeBuilder`), rather than sliding up under the node above.
 */
function separationAbove(node: Node): number {
    return node.type === LINEAGE_BOUNDING_BOX_NODE_NAME
        ? LINEAGE_NODE_HEIGHT * MAIN_Y_SEP_RATIO + BOUNDING_BOX_LABEL_HEIGHT
        : MIN_SEPARATION;
}

/**
 * Where a bounding box's bottom edge lands once it resizes to wrap its members, given `self`'s
 * pending expansion to `expandHeight` and any members already queued to move down.
 * Mirrors the height computed by `useFitToContents`.
 */
function boundingBoxBottom(
    box: Node,
    allNodes: Node[],
    self: Node,
    expandHeight: number,
    nodesToMove: Map<string, number>,
): number {
    const memberBottoms = allNodes
        .filter((node) => node.parentId === box.id)
        .map(
            (member) =>
                absolutePosition(member).y +
                (nodesToMove.get(member.id) ?? 0) +
                (member.id === self.id ? expandHeight : member.height || LINEAGE_NODE_HEIGHT),
        );
    return Math.max(...memberBottoms) + BOUNDING_BOX_PADDING;
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
