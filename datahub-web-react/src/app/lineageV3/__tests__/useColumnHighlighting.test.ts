import { FineGrainedLineage, NodeContext, createColumnRef } from '@app/lineageV3/common';
import { computeSingleColumnHighlights } from '@app/lineageV3/useColumnHighlighting';
import { createMemberNodeId } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.utils';

import { EntityType, LineageDirection } from '@types';

const UPSTREAM = 'urn:li:dataset:upstream';
const DOWNSTREAM = 'urn:li:dataset:downstream';
const DP_A = 'urn:li:dataProduct:A';
const DP_B = 'urn:li:dataProduct:B';
const FIELD = 'id';

const upstreamRef = createColumnRef(UPSTREAM, FIELD);
const downstreamRef = createColumnRef(DOWNSTREAM, FIELD);

function node(urn: string, direction?: LineageDirection) {
    // Counts are marked fetched so no tentative edges to lineage filter nodes are emitted
    const lineageAssets = new Map([
        [FIELD, { name: FIELD, numUpstream: 0, numDownstream: 0, lineageCountsFetched: true }],
    ]);
    return { id: urn, urn, type: EntityType.Dataset, direction, entity: { lineageAssets } } as any;
}

function fineGrainedLineage(): FineGrainedLineage {
    return {
        downstream: new Map([[upstreamRef, new Map([[downstreamRef, null]])]]),
        upstream: new Map([[downstreamRef, new Map([[upstreamRef, null]])]]),
    };
}

function run(nodeIdsByUrn: Map<string, string[]>) {
    const nodes: NodeContext['nodes'] = new Map([
        [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream)],
        [DOWNSTREAM, node(DOWNSTREAM, LineageDirection.Upstream)],
    ]);
    return computeSingleColumnHighlights(
        upstreamRef,
        {
            fineGrainedLineage: fineGrainedLineage(),
            nodes,
            adjacencyList: { [LineageDirection.Upstream]: new Map(), [LineageDirection.Downstream]: new Map() },
            displayedNodeIds: new Set([UPSTREAM, DOWNSTREAM]),
            nodeIdsByUrn,
            validQueryIds: new Set<string>(),
            rootUrn: DP_A,
            rootType: EntityType.DataProduct,
        },
        'red',
    );
}

describe('column edges attach to rendered node ids', () => {
    it('uses urns directly when node ids are urns', () => {
        const { columnEdges } = run(new Map());

        const edges = Array.from(columnEdges.values());
        expect(edges).toHaveLength(1);
        expect(edges[0].source).toEqual(UPSTREAM);
        expect(edges[0].target).toEqual(DOWNSTREAM);
        expect(edges[0].sourceHandle).toEqual(upstreamRef);
        expect(edges[0].targetHandle).toEqual(downstreamRef);
    });

    it('attaches to data-product-qualified member node ids', () => {
        const upstreamId = createMemberNodeId(DP_A, UPSTREAM);
        const downstreamId = createMemberNodeId(DP_A, DOWNSTREAM);
        const { columnEdges } = run(
            new Map([
                [UPSTREAM, [upstreamId]],
                [DOWNSTREAM, [downstreamId]],
            ]),
        );

        const edges = Array.from(columnEdges.values());
        expect(edges).toHaveLength(1);
        expect(edges[0].source).toEqual(upstreamId);
        expect(edges[0].target).toEqual(downstreamId);
        // Handles stay urn-based, as Column.tsx builds them from the entity urn
        expect(edges[0].sourceHandle).toEqual(upstreamRef);
        expect(edges[0].targetHandle).toEqual(downstreamRef);
    });

    it('emits one edge per rendered copy of an entity in multiple data products', () => {
        const downstreamA = createMemberNodeId(DP_A, DOWNSTREAM);
        const downstreamB = createMemberNodeId(DP_B, DOWNSTREAM);
        const upstreamId = createMemberNodeId(DP_A, UPSTREAM);
        const { columnEdges } = run(
            new Map([
                [UPSTREAM, [upstreamId]],
                [DOWNSTREAM, [downstreamA, downstreamB]],
            ]),
        );

        const targets = Array.from(columnEdges.values()).map((edge) => edge.target);
        expect(targets.sort()).toEqual([downstreamA, downstreamB].sort());
    });
});
