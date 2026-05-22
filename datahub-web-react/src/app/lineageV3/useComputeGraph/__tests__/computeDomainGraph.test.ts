import { describe, expect, it } from 'vitest';

import { AggregatedDomainEdge, FetchStatus, LineageEntity, domainEdgeKey } from '@app/lineageV3/common';
import computeDomainGraph, { computeNeighbourPlacements } from '@app/lineageV3/useComputeGraph/computeDomainGraph';

import { EntityType, LineageDirection } from '@types';

// String literals duplicated from LineageBoundingBoxNode.tsx / LineageEntityNode.tsx — those
// modules pull in styled-components, which doesn't initialise cleanly in this Vitest config,
// and re-importing the constants would drag the whole tree in.
const LINEAGE_BOUNDING_BOX_NODE_NAME = 'lineage-bounding-box';
const LINEAGE_ENTITY_NODE_NAME = 'lineage-entity';

const ROOT = 'urn:li:domain:root';
const A = 'urn:li:domain:A';
const B = 'urn:li:domain:B';
const C = 'urn:li:domain:C';

function edge(
    sourceUrn: string,
    neighbourUrn: string,
    direction: LineageDirection,
    memberMatchCount = 1,
): AggregatedDomainEdge {
    return {
        sourceUrn,
        neighbourUrn,
        neighbourType: EntityType.Domain,
        direction,
        memberMatchCount,
        neighbourEntityCount: 0,
        degreeMin: 1,
        degreeMax: 1,
    };
}

function buildEdgeMap(...edges: AggregatedDomainEdge[]): Map<string, AggregatedDomainEdge> {
    const map = new Map<string, AggregatedDomainEdge>();
    edges.forEach((e) => map.set(domainEdgeKey(e.sourceUrn, e.neighbourUrn, e.direction), e));
    return map;
}

describe('computeNeighbourPlacements', () => {
    it('places direct upstream + downstream neighbours at depth=1 on the matching side', () => {
        const edges = buildEdgeMap(
            edge(ROOT, A, LineageDirection.Upstream),
            edge(ROOT, B, LineageDirection.Downstream),
        );
        const placements = computeNeighbourPlacements(edges, ROOT);
        expect(placements.size).toBe(2);
        expect(placements.get(A)).toEqual({ side: LineageDirection.Upstream, depth: 1 });
        expect(placements.get(B)).toEqual({ side: LineageDirection.Downstream, depth: 1 });
    });

    it('carries the side through a multi-hop chain regardless of intermediate edge direction', () => {
        // Source — upstream → A — downstream → B. B should still anchor on the upstream side of
        // source because the user's drill-down rooted at the upstream column shouldn't ricochet
        // back across the source bbox when the next hop reverses direction.
        const edges = buildEdgeMap(edge(ROOT, A, LineageDirection.Upstream), edge(A, B, LineageDirection.Downstream));
        const placements = computeNeighbourPlacements(edges, ROOT);
        expect(placements.get(A)).toEqual({ side: LineageDirection.Upstream, depth: 1 });
        expect(placements.get(B)).toEqual({ side: LineageDirection.Upstream, depth: 2 });
    });

    it('assigns BFS depths reflecting hop count from source', () => {
        // ROOT → A → B → C (all downstream chain).
        const edges = buildEdgeMap(
            edge(ROOT, A, LineageDirection.Downstream),
            edge(A, B, LineageDirection.Downstream),
            edge(B, C, LineageDirection.Downstream),
        );
        const placements = computeNeighbourPlacements(edges, ROOT);
        expect(placements.get(A)?.depth).toBe(1);
        expect(placements.get(B)?.depth).toBe(2);
        expect(placements.get(C)?.depth).toBe(3);
        // All on the downstream side since chain originates from a downstream edge.
        expect(placements.get(A)?.side).toBe(LineageDirection.Downstream);
        expect(placements.get(B)?.side).toBe(LineageDirection.Downstream);
        expect(placements.get(C)?.side).toBe(LineageDirection.Downstream);
    });

    it('skips edges that loop back to the source Domain', () => {
        // Cycle: ROOT → A → ROOT. The back-edge to root must not produce a placement (root is
        // the bbox, not a neighbour) and must not break the BFS.
        const edges = buildEdgeMap(
            edge(ROOT, A, LineageDirection.Downstream),
            edge(A, ROOT, LineageDirection.Downstream),
        );
        const placements = computeNeighbourPlacements(edges, ROOT);
        expect(placements.has(ROOT)).toBe(false);
        expect(placements.get(A)).toEqual({ side: LineageDirection.Downstream, depth: 1 });
    });

    it('placement is assigned on first BFS visit (no oscillation when reachable both ways)', () => {
        // ROOT —upstream→ A and ROOT —downstream→ A. Whichever edge BFS processes first wins;
        // the regression we guard against is the placement flipping back and forth and producing
        // an unstable layout. Just assert that the placement is one of the two valid options
        // and consistent across calls with the same input.
        const edges = buildEdgeMap(
            edge(ROOT, A, LineageDirection.Upstream),
            edge(ROOT, A, LineageDirection.Downstream),
        );
        const placements1 = computeNeighbourPlacements(edges, ROOT);
        const placements2 = computeNeighbourPlacements(edges, ROOT);
        const side1 = placements1.get(A)?.side;
        const side2 = placements2.get(A)?.side;
        expect(side1).toBeDefined();
        expect(side1).toBe(side2);
        expect(placements1.get(A)?.depth).toBe(1);
    });
});

function makeNode(urn: string, type: EntityType, overrides: Partial<LineageEntity> = {}): LineageEntity {
    return {
        id: urn,
        urn,
        type,
        isExpanded: {
            [LineageDirection.Upstream]: false,
            [LineageDirection.Downstream]: false,
        },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNNEEDED,
            [LineageDirection.Downstream]: FetchStatus.UNNEEDED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
        ...overrides,
    };
}

// computeDomainGraph reads only nodes / aggregatedDomainEdges / aggregatedInnerEdges from its
// context; the remaining graph store fields are accepted in the type signature but not consumed,
// so empty stubs are sufficient for layout-level assertions.
function makeContext(nodes: Map<string, LineageEntity>) {
    return {
        nodes,
        edges: new Map(),
        adjacencyList: {
            [LineageDirection.Upstream]: new Map(),
            [LineageDirection.Downstream]: new Map(),
        },
        rootType: EntityType.Domain,
        showDataProcessInstances: false,
        showGhostEntities: false,
        showTransformations: false,
        hideTransformations: false,
        aggregatedDomainEdges: undefined,
        aggregatedInnerEdges: undefined,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any;
}

describe('computeDomainGraph layout (nested DP members)', () => {
    it('nests DP bboxes inside the Domain bbox and asset rows inside their DP bbox', () => {
        // ROOT Domain contains DP1 (with two datasets) and DP2 (with one dataset). Both DPs
        // become inner bboxes; assets sit inside their DP bbox via parentId.
        const nodes = new Map<string, LineageEntity>();
        nodes.set(ROOT, makeNode(ROOT, EntityType.Domain));
        const dp1 = 'urn:li:dataProduct:dp1';
        const dp2 = 'urn:li:dataProduct:dp2';
        const ds1a = 'urn:li:dataset:ds1a';
        const ds1b = 'urn:li:dataset:ds1b';
        const ds2 = 'urn:li:dataset:ds2';
        nodes.set(dp1, makeNode(dp1, EntityType.DataProduct, { parentDomain: ROOT }));
        nodes.set(dp2, makeNode(dp2, EntityType.DataProduct, { parentDomain: ROOT }));
        nodes.set(ds1a, makeNode(ds1a, EntityType.Dataset, { parentDataProduct: dp1 }));
        nodes.set(ds1b, makeNode(ds1b, EntityType.Dataset, { parentDataProduct: dp1 }));
        nodes.set(ds2, makeNode(ds2, EntityType.Dataset, { parentDataProduct: dp2 }));

        const { flowNodes } = computeDomainGraph(ROOT, EntityType.Domain, makeContext(nodes));

        const byId = new Map(flowNodes.map((n) => [n.id, n]));
        // Outer Domain bbox is the parent for the DP bboxes.
        expect(byId.get(ROOT)?.type).toBe(LINEAGE_BOUNDING_BOX_NODE_NAME);
        expect(byId.get(dp1)?.type).toBe(LINEAGE_BOUNDING_BOX_NODE_NAME);
        expect(byId.get(dp1)?.parentId).toBe(ROOT);
        expect(byId.get(dp2)?.type).toBe(LINEAGE_BOUNDING_BOX_NODE_NAME);
        expect(byId.get(dp2)?.parentId).toBe(ROOT);

        // Assets are nested inside their DP bbox, not the Domain bbox.
        expect(byId.get(ds1a)?.type).toBe(LINEAGE_ENTITY_NODE_NAME);
        expect(byId.get(ds1a)?.parentId).toBe(dp1);
        expect(byId.get(ds1b)?.parentId).toBe(dp1);
        expect(byId.get(ds2)?.parentId).toBe(dp2);

        // ReactFlow requires parents to precede their children in the node list.
        const idxRoot = flowNodes.findIndex((n) => n.id === ROOT);
        const idxDp1 = flowNodes.findIndex((n) => n.id === dp1);
        const idxDs1a = flowNodes.findIndex((n) => n.id === ds1a);
        expect(idxRoot).toBeLessThan(idxDp1);
        expect(idxDp1).toBeLessThan(idxDs1a);
    });

    it('nests DP-member assets inside the DP bbox even when also tagged to the Domain', () => {
        // DP membership wins over direct Domain tagging — an asset that belongs to a member DP
        // renders inside that DP's bbox rather than parallel to it at the Domain level.
        // Assets that aren't in any DP fall back to the Domain bbox.
        const nodes = new Map<string, LineageEntity>();
        nodes.set(ROOT, makeNode(ROOT, EntityType.Domain));
        const dp = 'urn:li:dataProduct:dp';
        const dsDirect = 'urn:li:dataset:direct';
        const dsBoth = 'urn:li:dataset:both';
        nodes.set(dp, makeNode(dp, EntityType.DataProduct, { parentDomain: ROOT }));
        nodes.set(dsDirect, makeNode(dsDirect, EntityType.Dataset, { parentDomain: ROOT }));
        nodes.set(dsBoth, makeNode(dsBoth, EntityType.Dataset, { parentDomain: ROOT, parentDataProduct: dp }));

        const { flowNodes } = computeDomainGraph(ROOT, EntityType.Domain, makeContext(nodes));

        const byId = new Map(flowNodes.map((n) => [n.id, n]));
        expect(byId.get(dsDirect)?.parentId).toBe(ROOT);
        expect(byId.get(dsBoth)?.parentId).toBe(dp);
        const dsBothInstances = flowNodes.filter((n) => n.id === dsBoth);
        expect(dsBothInstances).toHaveLength(1);
    });

    it('grows the Domain bbox vertically with the number of nested DP rows', () => {
        // One-DP Domain vs three-DP Domain — the outer Domain bbox should be visibly taller in
        // the three-DP case so all members fit without overlap.
        function buildNodes(dpCount: number): Map<string, LineageEntity> {
            const nodes = new Map<string, LineageEntity>();
            nodes.set(ROOT, makeNode(ROOT, EntityType.Domain));
            for (let i = 0; i < dpCount; i += 1) {
                const urn = `urn:li:dataProduct:dp${i}`;
                nodes.set(urn, makeNode(urn, EntityType.DataProduct, { parentDomain: ROOT }));
            }
            return nodes;
        }
        const oneDpHeight = computeDomainGraph(ROOT, EntityType.Domain, makeContext(buildNodes(1))).flowNodes.find(
            (n) => n.id === ROOT,
        )?.height;
        const threeDpHeight = computeDomainGraph(ROOT, EntityType.Domain, makeContext(buildNodes(3))).flowNodes.find(
            (n) => n.id === ROOT,
        )?.height;
        expect(oneDpHeight).toBeDefined();
        expect(threeDpHeight).toBeDefined();
        expect(threeDpHeight as number).toBeGreaterThan(oneDpHeight as number);
    });
});
