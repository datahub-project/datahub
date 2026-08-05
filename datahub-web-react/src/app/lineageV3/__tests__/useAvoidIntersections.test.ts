import { Node } from 'reactflow';

import {
    BOUNDING_BOX_LABEL_HEIGHT,
    BOUNDING_BOX_PADDING,
    LINEAGE_BOUNDING_BOX_NODE_NAME,
} from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import { avoidIntersections } from '@app/lineageV3/LineageEntityNode/useAvoidIntersections';
import { LINEAGE_NODE_HEIGHT, LINEAGE_NODE_WIDTH } from '@app/lineageV3/common';
import { MAIN_Y_SEP_RATIO } from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType } from '@types';

const BOX = 'urn:li:dataProduct:box';
const OTHER_BOX = 'urn:li:dataProduct:otherBox';
const MEMBER = 'urn:li:dataset:member';
const SIBLING = 'urn:li:dataset:sibling';
const BELOW = 'urn:li:dataset:below';

const EXPAND_HEIGHT = 400; // Height of the member once its columns are expanded
const MIN_SEPARATION = 10; // Gap kept above a plain node
const BOX_SEPARATION = LINEAGE_NODE_HEIGHT * MAIN_Y_SEP_RATIO + BOUNDING_BOX_LABEL_HEIGHT;

function entityNode(id: string, y: number, parentId?: string): Node {
    return {
        id,
        position: { x: 0, y },
        positionAbsolute: { x: 0, y },
        width: LINEAGE_NODE_WIDTH,
        height: LINEAGE_NODE_HEIGHT,
        parentId,
        data: { id, urn: id, type: EntityType.Dataset },
    } as Node;
}

function boxNode(id: string, y: number, height: number): Node {
    return {
        id,
        type: LINEAGE_BOUNDING_BOX_NODE_NAME,
        position: { x: 0, y },
        positionAbsolute: { x: 0, y },
        width: LINEAGE_NODE_WIDTH + 2 * BOUNDING_BOX_PADDING,
        height,
        data: { id, urn: id, type: EntityType.DataProduct },
    } as Node;
}

/** Runs the real push-down against a fixed node set, returning each node's y afterwards. */
function run(nodes: Node[], pinnedUrn?: string): Map<string, number> {
    const positions = new Map(nodes.map((node) => [node.id, node.position.y]));
    avoidIntersections({
        id: MEMBER,
        expandHeight: EXPAND_HEIGHT,
        rootType: EntityType.DataProduct,
        pinnedUrn: { current: pinnedUrn },
        getNode: (id: string) => nodes.find((node) => node.id === id),
        getNodes: () => nodes,
        setNodes: (update: any) => {
            update(nodes).forEach((node: Node) => positions.set(node.id, node.position.y));
        },
    } as any);
    return positions;
}

/** A box wrapping one member, with `below` underneath it. */
function boxWithMember(belowNode: Node) {
    const box = boxNode(BOX, 0, LINEAGE_NODE_HEIGHT + 2 * BOUNDING_BOX_PADDING);
    const member = entityNode(MEMBER, BOUNDING_BOX_PADDING, BOX);
    return [box, member, belowNode];
}

describe('avoidIntersections with bounding boxes', () => {
    it('pushes outside nodes clear of the bounding box, not just of the expanded member', () => {
        const below = entityNode(BELOW, LINEAGE_NODE_HEIGHT + 2 * BOUNDING_BOX_PADDING + 20);
        const nodes = boxWithMember(below);

        const positions = run(nodes);

        const memberBottom = BOUNDING_BOX_PADDING + EXPAND_HEIGHT;
        const boxBottom = memberBottom + BOUNDING_BOX_PADDING;
        // The point of the fix: clearing the member alone would leave the box overlapping
        expect(positions.get(BELOW)).toEqual(boxBottom + MIN_SEPARATION);
        expect(positions.get(BELOW)).toBeGreaterThan(memberBottom);
        // The box itself is never moved; it resizes to fit its contents
        expect(positions.get(BOX)).toEqual(0);
    });

    it('leaves another bounding box the separation it gets on initial layout', () => {
        // A box's label renders above its top edge, so it needs more room than a plain node
        const other = boxNode(OTHER_BOX, LINEAGE_NODE_HEIGHT + 2 * BOUNDING_BOX_PADDING + 100, LINEAGE_NODE_HEIGHT);
        const positions = run(boxWithMember(other));

        const boxBottom = BOUNDING_BOX_PADDING + EXPAND_HEIGHT + BOUNDING_BOX_PADDING;
        expect(positions.get(OTHER_BOX)).toEqual(boxBottom + BOX_SEPARATION);
        // Its label sits BOUNDING_BOX_LABEL_HEIGHT above its top edge, still clear of the box above
        expect((positions.get(OTHER_BOX) as number) - BOUNDING_BOX_LABEL_HEIGHT).toBeGreaterThan(boxBottom);
    });

    it('pushes same-box siblings only clear of the member itself', () => {
        const box = boxNode(BOX, 0, 2 * LINEAGE_NODE_HEIGHT + 2 * BOUNDING_BOX_PADDING);
        const member = entityNode(MEMBER, BOUNDING_BOX_PADDING, BOX);
        const sibling = entityNode(SIBLING, BOUNDING_BOX_PADDING + LINEAGE_NODE_HEIGHT + 20, BOX);

        const positions = run([box, member, sibling]);

        // A sibling shares the box, so it does not need to clear the box's padding
        expect(positions.get(SIBLING)).toEqual(BOUNDING_BOX_PADDING + EXPAND_HEIGHT + MIN_SEPARATION);
    });

    it('is unchanged for nodes with no bounding box', () => {
        const self = entityNode(MEMBER, 0);
        const below = entityNode(BELOW, LINEAGE_NODE_HEIGHT + 20);

        const positions = run([self, below]);

        // Only self's expanded bottom matters when there is no box to clear
        expect(positions.get(BELOW)).toEqual(EXPAND_HEIGHT + MIN_SEPARATION);
    });
});

describe('avoidIntersections leaves the hovered column’s node alone', () => {
    it('does not move the pinned node', () => {
        const self = entityNode(MEMBER, 0);
        const below = entityNode(BELOW, LINEAGE_NODE_HEIGHT + 20);

        expect(run([self, below], BELOW).get(BELOW)).toEqual(below.position.y);
        // ...but does once nothing is pinned, i.e. once a column is selected
        expect(run([self, below]).get(BELOW)).toBeGreaterThan(below.position.y);
    });

    it('pins the bounding box around the hovered node, which would carry it along', () => {
        const other = boxNode(OTHER_BOX, LINEAGE_NODE_HEIGHT + 2 * BOUNDING_BOX_PADDING + 100, LINEAGE_NODE_HEIGHT);
        const hovered = entityNode(BELOW, other.position.y + BOUNDING_BOX_PADDING, OTHER_BOX);
        const nodes = [...boxWithMember(other), hovered];

        const positions = run(nodes, BELOW);

        expect(positions.get(OTHER_BOX)).toEqual(other.position.y);
        expect(positions.get(BELOW)).toEqual(hovered.position.y);
    });
});
