import { BOUNDING_BOX_PADDING } from '@app/lineageV3/LineageBoundingBoxNode/LineageBoundingBoxNode';
import {
    FetchStatus,
    LINEAGE_NODE_HEIGHT,
    LineageEntity,
    NodeContext,
    addToAdjacencyList,
    createEdgeId,
} from '@app/lineageV3/common';
import { BoundingBoxGroup, GraphStore } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxes.types';
import layoutBoundingBoxInterior from '@app/lineageV3/useComputeGraph/boundingBoxes/layoutBoundingBoxInterior';

import { EntityType, LineageDirection } from '@types';

const DP = 'urn:li:dataProduct:DP';
const A = 'urn:li:dataset:A';
const B = 'urn:li:dataset:B';
const Q = 'urn:li:query:Q';

function node(urn: string, type: EntityType): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    };
}

/** A -> B, routed through query Q, with A and B members of DP and Q placed inside its box. */
function setUp(): { group: BoundingBoxGroup; graphStore: GraphStore } {
    const nodes = new Map([
        [A, node(A, EntityType.Dataset)],
        [Q, node(Q, EntityType.Query)],
        [B, node(B, EntityType.Dataset)],
    ]);
    const edges: NodeContext['edges'] = new Map([[createEdgeId(A, B), { isDisplayed: true, via: Q }]]);
    const adjacencyList: NodeContext['adjacencyList'] = {
        [LineageDirection.Upstream]: new Map(),
        [LineageDirection.Downstream]: new Map(),
    };
    addToAdjacencyList(adjacencyList, LineageDirection.Downstream, A, B);

    return {
        group: { urn: DP, type: EntityType.DataProduct, memberUrns: new Set([A, B]), queryUrns: new Set([Q]) },
        graphStore: { nodes, edges, adjacencyList, rootType: EntityType.DataProduct },
    };
}

describe('layoutBoundingBoxInterior', () => {
    it('lays out a query placed in the box between the members it connects', () => {
        const { group, graphStore } = setUp();
        const box = layoutBoundingBoxInterior(group, graphStore, false);

        const positionOf = (urn: string) =>
            box?.memberNodes.find((member) => (member.data as LineageEntity).urn === urn)?.position;
        expect(positionOf(A)?.x).toBeLessThan(positionOf(Q)?.x ?? 0);
        expect(positionOf(Q)?.x).toBeLessThan(positionOf(B)?.x ?? 0);
        // A query laid out as its own connected component would be stacked in a second row
        expect(box?.height).toBeLessThan(LINEAGE_NODE_HEIGHT + 2 * BOUNDING_BOX_PADDING + LINEAGE_NODE_HEIGHT);
    });

    it('excludes query nodes from the box member count', () => {
        const { group, graphStore } = setUp();
        const box = layoutBoundingBoxInterior(group, graphStore, false);

        expect(box?.memberNodes).toHaveLength(3);
        expect(box?.memberCount).toBe(2);
    });
});
