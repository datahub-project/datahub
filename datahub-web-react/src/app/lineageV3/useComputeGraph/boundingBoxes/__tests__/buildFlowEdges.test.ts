import { NodeContext, createEdgeId } from '@app/lineageV3/common';
import { GraphStore } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxes.types';
import { createMemberNodeId } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxes.utils';
import buildFlowEdges from '@app/lineageV3/useComputeGraph/boundingBoxes/buildFlowEdges';

import { EntityType, LineageDirection } from '@types';

const DP = 'urn:li:dataProduct:DP';
const A = 'urn:li:dataset:A';
const B = 'urn:li:dataset:B';
const Q = 'urn:li:query:Q';

/** A -> B, routed through query Q, with A and B members of DP. */
function graphStore(): GraphStore {
    const edges: NodeContext['edges'] = new Map([[createEdgeId(A, B), { isDisplayed: true, via: Q }]]);
    return {
        nodes: new Map(),
        edges,
        adjacencyList: { [LineageDirection.Upstream]: new Map(), [LineageDirection.Downstream]: new Map() },
        rootType: EntityType.DataProduct,
    };
}

describe('buildFlowEdges', () => {
    const membership = new Map([
        [A, [DP]],
        [B, [DP]],
    ]);

    it('routes an edge through a query rendered inside a bounding box', () => {
        const withQuery = new Map(membership).set(Q, [DP]);
        const edgeIds = buildFlowEdges(graphStore(), withQuery, new Set(), []).map((edge) => edge.id);

        expect(edgeIds).toEqual([
            createEdgeId(createMemberNodeId(DP, A), createMemberNodeId(DP, Q)),
            createEdgeId(createMemberNodeId(DP, Q), createMemberNodeId(DP, B)),
        ]);
    });

    it('routes an edge through a query rendered outside every bounding box', () => {
        const edgeIds = buildFlowEdges(graphStore(), membership, new Set([Q]), []).map((edge) => edge.id);

        expect(edgeIds).toEqual([
            createEdgeId(createMemberNodeId(DP, A), Q),
            createEdgeId(Q, createMemberNodeId(DP, B)),
        ]);
    });

    it('connects members directly when their query is not displayed', () => {
        const edgeIds = buildFlowEdges(graphStore(), membership, new Set(), []).map((edge) => edge.id);

        expect(edgeIds).toEqual([createEdgeId(createMemberNodeId(DP, A), createMemberNodeId(DP, B))]);
    });
});
