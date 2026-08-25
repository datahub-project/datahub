import { TENTATIVE_EDGE_NAME } from '@app/lineageV3/LineageEdge/TentativeEdge';
import {
    ColumnRef,
    FineGrainedLineage,
    NodeContext,
    createColumnRef,
    createLineageFilterNodeId,
    setDefault,
} from '@app/lineageV3/common';
import { computeSingleColumnHighlights } from '@app/lineageV3/useColumnHighlighting';
import { createMemberNodeId } from '@app/lineageV3/useComputeGraph/boundingBoxes/boundingBoxes.utils';

import { EntityType, LineageDirection } from '@types';

const UPSTREAM = 'urn:li:dataset:upstream';
const DOWNSTREAM = 'urn:li:dataset:downstream';
const DOWNSTREAM_OF_DOWNSTREAM = 'urn:li:dataset:downstreamOfDownstream';
const DP_A = 'urn:li:dataProduct:A';
const DP_B = 'urn:li:dataProduct:B';
const FIELD = 'id';

const upstreamRef = createColumnRef(UPSTREAM, FIELD);
const downstreamRef = createColumnRef(DOWNSTREAM, FIELD);

interface AssetOverrides {
    numUpstream?: number;
    numDownstream?: number;
    lineageCountsFetched?: boolean;
}

function node(urn: string, direction?: LineageDirection, asset?: AssetOverrides) {
    // Counts default to fetched, so a node without overrides emits no tentative edges
    const lineageAssets = asset && new Map([[FIELD, { name: FIELD, lineageCountsFetched: true, ...asset }]]);
    return { id: urn, urn, type: EntityType.Dataset, direction, entity: { lineageAssets } } as any;
}

/** Builds fine grained lineage from `[upstreamColumn, downstreamColumn]` pairs. */
function lineageFromEdges(edges: [ColumnRef, ColumnRef][]): FineGrainedLineage {
    const lineage: FineGrainedLineage = { downstream: new Map(), upstream: new Map() };
    edges.forEach(([from, to]) => {
        setDefault(lineage.downstream, from, new Map()).set(to, null);
        setDefault(lineage.upstream, to, new Map()).set(from, null);
    });
    return lineage;
}

function fineGrainedLineage(): FineGrainedLineage {
    return lineageFromEdges([[upstreamRef, downstreamRef]]);
}

interface Overrides {
    fineGrainedLineage?: FineGrainedLineage;
    nodes?: NodeContext['nodes'];
    displayedNodeIds?: Set<string>;
    showFilterNodes?: boolean;
}

function run(nodeIdsByUrn: Map<string, string[]>, overrides: Overrides = {}) {
    return computeSingleColumnHighlights(
        upstreamRef,
        {
            fineGrainedLineage: overrides.fineGrainedLineage ?? fineGrainedLineage(),
            nodes:
                overrides.nodes ??
                new Map([
                    [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream)],
                    [DOWNSTREAM, node(DOWNSTREAM, LineageDirection.Upstream)],
                ]),
            adjacencyList: { [LineageDirection.Upstream]: new Map(), [LineageDirection.Downstream]: new Map() },
            displayedNodeIds: overrides.displayedNodeIds ?? new Set([UPSTREAM, DOWNSTREAM]),
            nodeIdsByUrn,
            validQueryIds: new Set<string>(),
            rootUrn: DP_A,
            rootType: EntityType.DataProduct,
            showFilterNodes: overrides.showFilterNodes ?? false,
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

// Numerator of the column lineage controls, compared against the counts fetched for each column
describe('related columns shown on the graph', () => {
    const QUERY = 'urn:li:query:transform';
    const MISSING = 'urn:li:dataset:missing';
    const OTHER_FIELD = 'name';
    const queryRef = createColumnRef(QUERY, FIELD);
    const missingRef = createColumnRef(MISSING, FIELD);

    it('counts both directions for the column under the cursor', () => {
        const { shownRelatedColumns } = run(new Map());

        expect(shownRelatedColumns.get(upstreamRef)).toEqual({
            [LineageDirection.Downstream]: 1,
            [LineageDirection.Upstream]: 0,
        });
    });

    it('counts related columns only on the side the traversal reached them from', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, downstreamRef],
                [downstreamRef, createColumnRef(DOWNSTREAM_OF_DOWNSTREAM, FIELD)],
            ]),
            nodes: new Map([
                [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream)],
                [DOWNSTREAM, node(DOWNSTREAM, LineageDirection.Upstream)],
                [DOWNSTREAM_OF_DOWNSTREAM, node(DOWNSTREAM_OF_DOWNSTREAM, LineageDirection.Upstream)],
            ]),
            displayedNodeIds: new Set([UPSTREAM, DOWNSTREAM, DOWNSTREAM_OF_DOWNSTREAM]),
        });

        // Nothing is known about what is upstream of the downstream column, as we never looked
        expect(shownRelatedColumns.get(downstreamRef)).toEqual({ [LineageDirection.Downstream]: 1 });
    });

    it('leaves out columns on nodes that are not displayed', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, missingRef],
                [missingRef, downstreamRef],
            ]),
        });

        expect(shownRelatedColumns.has(missingRef)).toBe(false);
        expect(shownRelatedColumns.has(downstreamRef)).toBe(true);
    });

    it('counts each related column once, no matter how many paths reach it', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, downstreamRef],
                [upstreamRef, createColumnRef(DOWNSTREAM, OTHER_FIELD)],
                [upstreamRef, queryRef],
                [queryRef, downstreamRef],
            ]),
            displayedNodeIds: new Set([UPSTREAM, QUERY, DOWNSTREAM]),
        });

        expect(shownRelatedColumns.get(upstreamRef)?.[LineageDirection.Downstream]).toEqual(2);
    });

    it('counts through transformations, which are not rendered as columns of their own', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, queryRef],
                [queryRef, downstreamRef],
            ]),
            displayedNodeIds: new Set([UPSTREAM, QUERY, DOWNSTREAM]),
        });

        expect(shownRelatedColumns.get(upstreamRef)?.[LineageDirection.Downstream]).toEqual(1);
    });

    it('counts through nodes missing from the graph', () => {
        const { shownRelatedColumns } = run(new Map(), {
            fineGrainedLineage: lineageFromEdges([
                [upstreamRef, missingRef],
                [missingRef, downstreamRef],
            ]),
        });

        expect(shownRelatedColumns.get(upstreamRef)?.[LineageDirection.Downstream]).toEqual(1);
    });
});

// How hidden column lineage is shown when lineage filter nodes are rendered, in place of the
// column lineage controls
describe('column edges to lineage filter nodes', () => {
    const upstreamFilterNode = createLineageFilterNodeId(UPSTREAM, LineageDirection.Upstream);
    const downstreamFilterNode = createLineageFilterNodeId(UPSTREAM, LineageDirection.Downstream);

    /** Edges other than the upstream column -> downstream column edge every case emits. */
    function filterNodeEdges(asset: AssetOverrides, showFilterNodes = true) {
        const { columnEdges } = run(new Map(), {
            nodes: new Map([
                [UPSTREAM, node(UPSTREAM, LineageDirection.Upstream, asset)],
                // Counts fetched, so only the column under test emits filter node edges
                [DOWNSTREAM, node(DOWNSTREAM, LineageDirection.Upstream, {})],
            ]),
            showFilterNodes,
        });
        return Array.from(columnEdges.values()).filter((edge) => edge.targetHandle !== downstreamRef);
    }

    it('emits a tentative edge in each direction while counts are unfetched', () => {
        const edges = filterNodeEdges({ lineageCountsFetched: false });

        expect(edges).toHaveLength(2);
        expect(edges.every((edge) => edge.type === TENTATIVE_EDGE_NAME)).toBe(true);
        // Edges point away from the column, so the filter node is the target only downstream
        expect(edges.map((edge) => [edge.source, edge.target])).toEqual(
            expect.arrayContaining([
                [UPSTREAM, downstreamFilterNode],
                [upstreamFilterNode, UPSTREAM],
            ]),
        );
    });

    it('emits a solid edge once counts show more lineage than is on the graph', () => {
        const edges = filterNodeEdges({ numDownstream: 2 });

        expect(edges).toHaveLength(1);
        expect(edges[0].type).toEqual('default');
        expect(edges[0].sourceHandle).toEqual(upstreamRef);
        expect(edges[0].target).toEqual(downstreamFilterNode);
    });

    it('emits no edge once counts show all lineage is on the graph', () => {
        expect(filterNodeEdges({ numDownstream: 1 })).toHaveLength(0);
    });

    it('emits no edge when filter nodes are not rendered, as the controls show the counts', () => {
        expect(filterNodeEdges({ lineageCountsFetched: false }, false)).toHaveLength(0);
    });
});
