import {
    FetchStatus,
    LINEAGE_FILTER_TYPE,
    LineageEntity,
    LineageFilter,
    NodeContext,
    addToAdjacencyList,
} from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { getNodesWithLineage, hideOrphanedNodes } from '@app/lineageV3/useComputeGraph/lineageGraph.utils';

import { EntityType, LineageDirection } from '@types';

/**
 * Test helper: Creates a minimal adjacency list for testing
 */
function createAdjacencyList(
    upstreamEdges: Map<string, Set<string>>,
    downstreamEdges: Map<string, Set<string>>,
): NodeContext['adjacencyList'] {
    return {
        [LineageDirection.Upstream]: upstreamEdges,
        [LineageDirection.Downstream]: downstreamEdges,
    };
}

describe('lineageGraph.utils', () => {
    describe('getNodesWithLineage (orphaned node filtering)', () => {
        const A = 'urn:li:dataset:A';
        const B = 'urn:li:dataset:B';
        const C = 'urn:li:dataset:C';
        const D = 'urn:li:dataset:D';
        const E = 'urn:li:dataset:E';

        it('includes all nodes in a linear chain', () => {
            // Chain: A -> B -> D
            const adjacencyList = createAdjacencyList(
                new Map([
                    [B, new Set([A])], // B upstream from A
                    [D, new Set([B])], // D upstream from B
                ]),
                new Map([
                    [A, new Set([B])], // A downstream to B
                    [B, new Set([D])], // B downstream to D
                ]),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // All chain members appear in edges
            expect(result).toEqual(new Set([A, B, D]));
            // C is truly orphaned
            expect(result).not.toContain(C);
        });

        it('includes nodes from multiple upstream sources (fan-in)', () => {
            // Edges: A -> B, C -> B (two sources to one target)
            const adjacencyList = createAdjacencyList(
                new Map([
                    [B, new Set([A, C])], // B upstream from both A and C
                ]),
                new Map([
                    [A, new Set([B])], // A downstream to B
                    [C, new Set([B])], // C downstream to B
                ]),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // All three nodes in edges (A, B, C)
            expect(result).toEqual(new Set([A, B, C]));
            // D is orphaned
            expect(result).not.toContain(D);
        });

        it('includes nodes from multiple downstream targets (fan-out)', () => {
            // Edges: A -> B, A -> C (one source to two targets)
            const adjacencyList = createAdjacencyList(
                new Map([
                    [B, new Set([A])], // B upstream from A
                    [C, new Set([A])], // C upstream from A
                ]),
                new Map([
                    [A, new Set([B, C])], // A downstream to both B and C
                ]),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // All three nodes in edges (A, B, C)
            expect(result).toEqual(new Set([A, B, C]));
            // D is orphaned
            expect(result).not.toContain(D);
        });

        it('handles complex diamond pattern (A -> B, A -> C, B -> D, C -> D)', () => {
            // All four nodes connected in diamond shape
            const adjacencyList = createAdjacencyList(
                new Map([
                    [B, new Set([A])],
                    [C, new Set([A])],
                    [D, new Set([B, C])],
                ]),
                new Map([
                    [A, new Set([B, C])],
                    [B, new Set([D])],
                    [C, new Set([D])],
                ]),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // All nodes in the diamond are connected
            expect(result).toEqual(new Set([A, B, C, D]));
        });

        it('filters a data product with both connected and orphaned members', () => {
            // Real-world scenario: Product has A->B (connected) and C, D (orphaned)
            const adjacencyList = createAdjacencyList(new Map([[B, new Set([A])]]), new Map([[A, new Set([B])]]));

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            expect(result).toEqual(new Set([A, B]));
            expect(result).not.toContain(C);
            expect(result).not.toContain(D);
        });

        it('handles multiple separate lineage chains', () => {
            // Two independent chains: A -> B and C -> D
            const adjacencyList = createAdjacencyList(
                new Map([
                    [B, new Set([A])],
                    [D, new Set([C])],
                ]),
                new Map([
                    [A, new Set([B])],
                    [C, new Set([D])],
                ]),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // All four nodes are in lineage chains
            expect(result).toEqual(new Set([A, B, C, D]));
        });

        it('handles complex real-world scenario with multiple levels and orphans', () => {
            // Scenario: E->D->B->A and C orphaned
            // Shows multi-level chain with orphaned member
            const adjacencyList = createAdjacencyList(
                new Map([
                    [D, new Set([E])],
                    [B, new Set([D])],
                    [A, new Set([B])],
                ]),
                new Map([
                    [E, new Set([D])],
                    [D, new Set([B])],
                    [B, new Set([A])],
                ]),
            );

            const result = getNodesWithLineage([A, B, D, E, C], adjacencyList);

            // Chain members: E, D, B, A
            expect(result).toEqual(new Set([E, D, B, A]));
            // C is orphaned
            expect(result).not.toContain(C);
        });

        it('handles empty node list', () => {
            const adjacencyList = createAdjacencyList(new Map(), new Map());

            const result = getNodesWithLineage([], adjacencyList);

            expect(result).toEqual(new Set());
        });

        it('handles undefined node IDs in input', () => {
            // A -> B edge, with undefined in input list
            const adjacencyList = createAdjacencyList(new Map([[B, new Set([A])]]), new Map([[A, new Set([B])]]));

            const result = getNodesWithLineage([A, undefined, B], adjacencyList);

            // Only A and B are valid; undefined is skipped
            expect(result).toEqual(new Set([A, B]));
        });

        it('handles self-loop edge (A -> A)', () => {
            // A has an edge to itself, B has no edges
            const adjacencyList = createAdjacencyList(new Map([[A, new Set([A])]]), new Map([[A, new Set([A])]]));

            const result = getNodesWithLineage([A, B], adjacencyList);

            expect(result).toEqual(new Set([A]));
            expect(result).not.toContain(B);
        });

        it('returns deterministic results regardless of input order', () => {
            // Tests that A -> B produces same result in any input order
            const adjacencyList = createAdjacencyList(new Map([[B, new Set([A])]]), new Map([[A, new Set([B])]]));

            const result1 = getNodesWithLineage([A, B, C], adjacencyList);
            const result2 = getNodesWithLineage([C, B, A], adjacencyList);
            const result3 = getNodesWithLineage([B, A, C], adjacencyList);

            expect(result1).toEqual(result2);
            expect(result2).toEqual(result3);
            expect(result1).toEqual(new Set([A, B]));
        });

        it('only checks nodes specified in input list', () => {
            // Tests that function scopes to input: A->B exists, but we only check A and B
            const adjacencyList = createAdjacencyList(new Map([[B, new Set([A])]]), new Map([[A, new Set([B])]]));

            // Only check A and B, not C and D
            const result = getNodesWithLineage([A, B], adjacencyList);

            expect(result).toEqual(new Set([A, B]));
            // C and D never in input, so can't be in result
            expect(result).not.toContain(C);
            expect(result).not.toContain(D);
        });
    });

    describe('hideOrphanedNodes', () => {
        const DP = 'urn:li:dataProduct:DP';
        const A = 'urn:li:dataset:A';
        const B = 'urn:li:dataset:B';
        const C = 'urn:li:dataset:C';
        const FILTER = 'lineage-filter:A';

        function entityNode(urn: string, fetched: boolean): LineageEntity {
            return {
                id: urn,
                urn,
                type: EntityType.Dataset,
                entity: fetched ? ({ urn, type: EntityType.Dataset, exists: true } as FetchedEntityV2) : undefined,
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

        const filterNode = {
            id: FILTER,
            type: LINEAGE_FILTER_TYPE,
            direction: LineageDirection.Downstream,
            parent: A,
        } as LineageFilter;

        function setUp(fetched: Record<string, boolean>, edges: [string, string][]) {
            const nodes: NodeContext['nodes'] = new Map(
                Object.entries(fetched).map(([urn, isFetched]) => [urn, entityNode(urn, isFetched)]),
            );
            const adjacencyList = createAdjacencyList(new Map(), new Map());
            edges.forEach(([upstream, downstream]) =>
                addToAdjacencyList(adjacencyList, LineageDirection.Downstream, upstream, downstream),
            );
            return { nodes, adjacencyList };
        }

        const ids = (shown: { id: string }[]) => shown.map((node) => node.id);
        const pick = (nodes: NodeContext['nodes'], urns: string[]): LineageEntity[] =>
            urns.map((urn) => nodes.get(urn)).filter((node): node is LineageEntity => !!node);

        it('hides fetched members with no lineage and keeps connected ones', () => {
            const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: true, [B]: true, [C]: true }, [[A, B]]);
            const shown = pick(nodes, [DP, A, B, C]);

            const result = hideOrphanedNodes(shown, DP, new Set([A, B, C]), nodes, adjacencyList);

            expect(ids(result)).toEqual([DP, A, B]);
        });

        it('keeps unfetched members, since hiding them would stop their lineage from being fetched', () => {
            const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: true, [B]: true, [C]: false }, [[A, B]]);
            const shown = pick(nodes, [DP, A, B, C]);

            const result = hideOrphanedNodes(shown, DP, new Set([A, B, C]), nodes, adjacencyList);

            expect(ids(result)).toEqual([DP, A, B, C]);
        });

        it('hides nothing while no home member is known to have lineage, e.g. mid-way through loading a page', () => {
            const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: false, [C]: true }, []);
            const shown = pick(nodes, [DP, A, C]);

            const result = hideOrphanedNodes(shown, DP, new Set([A, C]), nodes, adjacencyList);

            expect(ids(result)).toEqual([DP, A, C]);
        });

        it('hides fetched orphans once a home member has lineage, even if that member is not fetched yet', () => {
            const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: false, [B]: true, [C]: true }, [[B, A]]);
            const shown = pick(nodes, [DP, A, C]);

            const result = hideOrphanedNodes(shown, DP, new Set([A, C]), nodes, adjacencyList);

            expect(ids(result)).toEqual([DP, A]);
        });

        it('shows every node when no home member has lineage, so the home box does not disappear', () => {
            const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: true, [C]: true }, []);
            const shown = pick(nodes, [DP, A, C]);

            const result = hideOrphanedNodes(shown, DP, new Set([A, C]), nodes, adjacencyList);

            expect(ids(result)).toEqual([DP, A, C]);
        });

        it('always keeps the root and filter nodes', () => {
            const { nodes, adjacencyList } = setUp({ [DP]: true, [A]: true, [B]: true }, [[A, B]]);
            const shown = [...pick(nodes, [DP, A, B]), filterNode];

            const result = hideOrphanedNodes(shown, DP, new Set([A, B]), nodes, adjacencyList);

            expect(ids(result)).toEqual([DP, A, B, FILTER]);
        });
    });
});
