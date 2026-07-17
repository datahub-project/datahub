import { NodeContext, buildHighlightAdjacencyList, createEdgeId } from '@app/lineageV3/common';
import { computeHighlights } from '@app/lineageV3/useNodeHighlighting';
import buildFlowEdges from '@app/lineageV3/useComputeGraph/dataProduct/buildFlowEdges';
import { createMemberNodeId } from '@app/lineageV3/useComputeGraph/dataProduct/dataProduct.utils';
import filterToRevealedEdges from '@app/lineageV3/useComputeGraph/dataProduct/revealedEdges';

import { EntityType, LineageDirection } from '@types';

const A = 'urn:li:dataset:A'; // outside, free
const Q = 'urn:li:query:Q'; // query node, free
const B = 'urn:li:dataset:B'; // inside data product DP
const DP = 'urn:li:dataProduct:DP';

function node(urn: string, type: EntityType) {
    return {
        id: urn,
        urn,
        type,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: { [LineageDirection.Upstream]: 0, [LineageDirection.Downstream]: 0 },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    } as any;
}

function store(nodes: Map<string, any>, edges: NodeContext['edges']) {
    return {
        nodes,
        edges,
        adjacencyList: { [LineageDirection.Upstream]: new Map(), [LineageDirection.Downstream]: new Map() },
        rootType: EntityType.DataProduct,
    } as any;
}

/** Is a rendered flow edge highlighted, per the LineageTableEdge rule (id OR originalId match)? */
function isEdgeHighlighted(edge: any, highlightedEdges: Set<string>): boolean {
    return highlightedEdges.has(edge.id) || highlightedEdges.has(edge.data?.originalId);
}

/** Runs the real DP pipeline for one hovered urn, returning the rendered edges + highlight set. */
function run(nodes: Map<string, any>, edges: NodeContext['edges'], hovered: string, opts: {
    membership: Map<string, string[]>;
    freeIds: Set<string>;
    displayedIds: Set<string>;
}) {
    const revealed = filterToRevealedEdges(store(nodes, edges));
    const flowEdges = buildFlowEdges(revealed, opts.membership, opts.freeIds, []);
    const { highlightedEdges } = computeHighlights(hovered, buildHighlightAdjacencyList(revealed.edges, opts.displayedIds));
    return { flowEdges, highlightedEdges };
}

describe('data product lineage highlighting through query nodes', () => {
    const membership = new Map([[B, [DP]]]);
    const freeIds = new Set([A, Q]);
    const displayedIds = new Set([A, Q, B]);
    const memberEdgeId = createEdgeId(Q, createMemberNodeId(DP, B));

    it('single A->B edge with via=Q: crossing edge into box highlights', () => {
        const nodes = new Map([[A, node(A, EntityType.Dataset)], [Q, node(Q, EntityType.Query)], [B, node(B, EntityType.Dataset)]]);
        const edges: NodeContext['edges'] = new Map([[createEdgeId(A, B), { isDisplayed: true, via: Q }]]);
        const { flowEdges, highlightedEdges } = run(nodes, edges, A, { membership, freeIds, displayedIds });
        const crossing = flowEdges.find((e) => e.id === memberEdgeId);
        expect(crossing).toBeDefined();
        expect(isEdgeHighlighted(crossing, highlightedEdges)).toBe(true);
    });

    it('reverse direction: B(box) -> Q -> A(free), hover A upstream', () => {
        const nodes = new Map([[A, node(A, EntityType.Dataset)], [Q, node(Q, EntityType.Query)], [B, node(B, EntityType.Dataset)]]);
        const edges: NodeContext['edges'] = new Map([[createEdgeId(B, A), { isDisplayed: true, via: Q }]]);
        const { flowEdges, highlightedEdges } = run(nodes, edges, A, { membership, freeIds, displayedIds });
        const crossing = flowEdges.find((e) => e.id === createEdgeId(createMemberNodeId(DP, B), Q));
        expect(crossing).toBeDefined();
        expect(isEdgeHighlighted(crossing, highlightedEdges)).toBe(true);
    });

    // Regression: a query fanning several upstreams into one downstream box member. Hovering ANY
    // upstream must highlight the shared query->member segment (previously only the upstream that
    // built the deduped segment first matched).
    it('fan-in query: hovering any upstream highlights the shared query->member segment', () => {
        const REG = 'urn:li:dataset:REG';
        const nodes = new Map([
            [A, node(A, EntityType.Dataset)],
            [REG, node(REG, EntityType.Dataset)],
            [Q, node(Q, EntityType.Query)],
            [B, node(B, EntityType.Dataset)],
        ]);
        // REG's edge is inserted first, so today's dedup keys the shared segment to REG.
        const edges: NodeContext['edges'] = new Map([
            [createEdgeId(REG, B), { isDisplayed: true, via: Q }],
            [createEdgeId(A, B), { isDisplayed: true, via: Q }],
        ]);
        const opts = { membership, freeIds: new Set([A, REG, Q]), displayedIds: new Set([A, REG, Q, B]) };
        const shared = createEdgeId(Q, createMemberNodeId(DP, B));

        // Both the "unlucky" upstream (A) and the first-built one (REG) must light up the shared segment.
        [A, REG].forEach((hovered) => {
            const { flowEdges, highlightedEdges } = run(nodes, edges, hovered, opts);
            const sharedSeg = flowEdges.find((e) => e.id === shared);
            const upstreamSeg = flowEdges.find((e) => e.id === createEdgeId(hovered, Q));
            expect(sharedSeg).toBeDefined();
            expect(upstreamSeg).toBeDefined();
            expect(isEdgeHighlighted(upstreamSeg, highlightedEdges)).toBe(true);
            expect(isEdgeHighlighted(sharedSeg, highlightedEdges)).toBe(true);
        });
    });

    it('query bridging two boxes highlights the far crossing edge', () => {
        const C = 'urn:li:dataset:C';
        const DP2 = 'urn:li:dataProduct:DP2';
        const nodes = new Map([
            [A, node(A, EntityType.Dataset)],
            [B, node(B, EntityType.Dataset)],
            [Q, node(Q, EntityType.Query)],
            [C, node(C, EntityType.Dataset)],
        ]);
        const edges: NodeContext['edges'] = new Map([
            [createEdgeId(A, B), { isDisplayed: true }],
            [createEdgeId(B, C), { isDisplayed: true, via: Q }],
        ]);
        const opts = { membership: new Map([[B, [DP]], [C, [DP2]]]), freeIds: new Set([A, Q]), displayedIds: new Set([A, B, Q, C]) };
        const { flowEdges, highlightedEdges } = run(nodes, edges, A, opts);
        const farCrossing = flowEdges.find((e) => e.id === createEdgeId(Q, createMemberNodeId(DP2, C)));
        expect(farCrossing).toBeDefined();
        expect(isEdgeHighlighted(farCrossing, highlightedEdges)).toBe(true);
    });

    it('interior member reached through an external query highlights', () => {
        const C = 'urn:li:dataset:C';
        const nodes = new Map([
            [A, node(A, EntityType.Dataset)],
            [B, node(B, EntityType.Dataset)],
            [Q, node(Q, EntityType.Query)],
            [C, node(C, EntityType.Dataset)],
        ]);
        const edges: NodeContext['edges'] = new Map([
            [createEdgeId(A, B), { isDisplayed: true }],
            [createEdgeId(B, C), { isDisplayed: true, via: Q }],
        ]);
        const opts = { membership: new Map([[B, [DP]], [C, [DP]]]), freeIds: new Set([A, Q]), displayedIds: new Set([A, B, Q, C]) };
        const { flowEdges, highlightedEdges } = run(nodes, edges, A, opts);
        const intoC = flowEdges.find((e) => e.id === createEdgeId(Q, createMemberNodeId(DP, C)));
        expect(intoC).toBeDefined();
        expect(isEdgeHighlighted(intoC, highlightedEdges)).toBe(true);
    });
});
