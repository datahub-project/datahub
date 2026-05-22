import { describe, expect, it } from 'vitest';

import { AggregatedDomainEdge, domainEdgeKey } from '@app/lineageV3/common';
import { computeNeighbourPlacements } from '@app/lineageV3/useComputeGraph/computeDomainGraph';

import { EntityType, LineageDirection } from '@types';

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
