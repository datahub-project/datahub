import { TENTATIVE_EDGE_NAME } from '@app/lineageV3/LineageEdge/TentativeEdge';
import {
    FineGrainedLineage,
    NodeContext,
    createColumnRef,
    createHiddenLineageRef,
    createLineageFilterNodeId,
} from '@app/lineageV3/common';
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

interface NodeOverrides {
    numUpstream?: number;
    numDownstream?: number;
    lineageCountsFetched?: boolean;
    numUpstreamChildren?: number;
    numDownstreamChildren?: number;
}

function node(urn: string, direction?: LineageDirection, overrides: NodeOverrides = {}) {
    const {
        numUpstream = 0,
        numDownstream = 0,
        // Counts default to fetched so no tentative hidden lineage edges are emitted
        lineageCountsFetched = true,
        numUpstreamChildren = 1,
        numDownstreamChildren = 1,
    } = overrides;
    const lineageAssets = new Map([[FIELD, { name: FIELD, numUpstream, numDownstream, lineageCountsFetched }]]);
    return {
        id: urn,
        urn,
        type: EntityType.Dataset,
        direction,
        entity: { lineageAssets, numUpstreamChildren, numDownstreamChildren },
    } as any;
}

function fineGrainedLineage(): FineGrainedLineage {
    return {
        downstream: new Map([[upstreamRef, new Map([[downstreamRef, null]])]]),
        upstream: new Map([[downstreamRef, new Map([[upstreamRef, null]])]]),
    };
}

function run(nodeIdsByUrn: Map<string, string[]>, nodes?: NodeContext['nodes'], showFilterNodes = false) {
    return computeSingleColumnHighlights(
        upstreamRef,
        {
            fineGrainedLineage: fineGrainedLineage(),
            nodes:
                nodes ??
                new Map([
                    [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream)],
                    [DOWNSTREAM, node(DOWNSTREAM, LineageDirection.Upstream)],
                ]),
            adjacencyList: { [LineageDirection.Upstream]: new Map(), [LineageDirection.Downstream]: new Map() },
            displayedNodeIds: new Set([UPSTREAM, DOWNSTREAM]),
            nodeIdsByUrn,
            validQueryIds: new Set<string>(),
            rootUrn: DP_A,
            rootType: EntityType.DataProduct,
            showFilterNodes,
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

describe('column edges to hidden nodes', () => {
    const hiddenUpstreamRef = createHiddenLineageRef(UPSTREAM, LineageDirection.Upstream);
    const hiddenDownstreamRef = createHiddenLineageRef(UPSTREAM, LineageDirection.Downstream);

    /** Edges other than the upstream column -> downstream column edge every case emits. */
    function hiddenEdges(overrides: NodeOverrides, showFilterNodes = false) {
        const nodes: NodeContext['nodes'] = new Map([
            [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream, overrides)],
            [DOWNSTREAM, node(DOWNSTREAM, LineageDirection.Upstream)],
        ]);
        const { columnEdges } = run(new Map(), nodes, showFilterNodes);
        return Array.from(columnEdges.values()).filter((edge) => edge.targetHandle !== downstreamRef);
    }

    it('emits a tentative edge in each direction while counts are unfetched', () => {
        const edges = hiddenEdges({ lineageCountsFetched: false });

        expect(edges).toHaveLength(2);
        expect(edges.every((edge) => edge.type === TENTATIVE_EDGE_NAME)).toBe(true);
        // Self edges: the control is part of the node's own DOM
        expect(edges.every((edge) => edge.source === UPSTREAM && edge.target === UPSTREAM)).toBe(true);
        expect(edges.map((edge) => [edge.sourceHandle, edge.targetHandle])).toEqual(
            expect.arrayContaining([
                [upstreamRef, hiddenDownstreamRef],
                [hiddenUpstreamRef, upstreamRef],
            ]),
        );
    });

    it('emits a solid edge once counts show more lineage than is on the graph', () => {
        const edges = hiddenEdges({ numDownstream: 2 });

        expect(edges).toHaveLength(1);
        expect(edges[0].type).toEqual('default');
        expect(edges[0].sourceHandle).toEqual(upstreamRef);
        expect(edges[0].targetHandle).toEqual(hiddenDownstreamRef);
    });

    it('emits no edge once counts show all lineage is on the graph', () => {
        expect(hiddenEdges({ numDownstream: 1 })).toHaveLength(0);
    });

    it('emits no edge in a direction the entity has no children in', () => {
        const edges = hiddenEdges({ lineageCountsFetched: false, numUpstreamChildren: 0 });

        expect(edges).toHaveLength(1);
        expect(edges[0].targetHandle).toEqual(hiddenDownstreamRef);
    });

    it('targets the lineage filter node when filter nodes are shown', () => {
        const edges = hiddenEdges({ numDownstream: 2 }, true);

        expect(edges).toHaveLength(1);
        expect(edges[0].target).toEqual(createLineageFilterNodeId(UPSTREAM, LineageDirection.Downstream));
        expect(edges[0].targetHandle).toBeUndefined();
    });
});
