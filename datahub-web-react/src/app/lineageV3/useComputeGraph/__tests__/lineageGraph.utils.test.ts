import { NodeContext } from '@app/lineageV3/common';
import { getNodesWithLineage } from '@app/lineageV3/useComputeGraph/lineageGraph.utils';

import { LineageDirection } from '@types';

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
});
