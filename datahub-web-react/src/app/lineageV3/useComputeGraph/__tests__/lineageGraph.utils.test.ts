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
        const C = 'urn:li:dataset:C'; // Orphaned node
        const D = 'urn:li:dataset:D';

        it('identifies nodes with downstream edges', () => {
            // Chain: A -> B -> D
            // A has downstream neighbors {B}
            // B has downstream neighbors {D}
            const adjacencyList = createAdjacencyList(
                new Map(), // no upstream edges
                new Map([
                    [A, new Set([B])],
                    [B, new Set([D])],
                ]),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // A and B have downstream edges. D and C have none.
            expect(result).toEqual(new Set([A, B]));
            expect(result).not.toContain(C);
            expect(result).not.toContain(D);
        });

        it('identifies nodes with upstream edges', () => {
            // Chain: A -> B -> D
            // B has upstream neighbors {A}
            // D has upstream neighbors {B}
            const adjacencyList = createAdjacencyList(
                new Map([
                    [B, new Set([A])],
                    [D, new Set([B])],
                ]),
                new Map(), // no downstream edges
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // B and D have upstream edges. A and C have none.
            expect(result).toEqual(new Set([B, D]));
            expect(result).not.toContain(A);
            expect(result).not.toContain(C);
        });

        it('identifies nodes with both upstream and downstream edges', () => {
            // Chain: A -> B -> D
            // A has downstream {B}
            // B has upstream {A} and downstream {D}
            // D has upstream {B}
            const adjacencyList = createAdjacencyList(
                new Map([
                    [B, new Set([A])],
                    [D, new Set([B])],
                ]),
                new Map([
                    [A, new Set([B])],
                    [B, new Set([D])],
                ]),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // A, B, D all have edges. C is orphaned.
            expect(result).toEqual(new Set([A, B, D]));
            expect(result).not.toContain(C);
        });

        it('excludes nodes with no edges (orphaned nodes)', () => {
            const adjacencyList = createAdjacencyList(new Map([[B, new Set([A])]]), new Map([[B, new Set([D])]]));

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // C has no upstream or downstream edges, so it should be excluded
            expect(result).not.toContain(C);
        });

        it('handles empty node list', () => {
            const adjacencyList = createAdjacencyList(new Map(), new Map());

            const result = getNodesWithLineage([], adjacencyList);

            expect(result).toEqual(new Set());
        });

        it('handles nodes with undefined IDs', () => {
            const adjacencyList = createAdjacencyList(
                new Map(),
                new Map([[A, new Set([B])]]), // A has downstream B
            );

            const result = getNodesWithLineage([A, undefined, B, C], adjacencyList);

            // A has downstream edges, B and C don't appear in maps, undefined is skipped
            expect(result).toEqual(new Set([A]));
        });

        it('correctly filters a mixed scenario: some members have lineage, some are orphaned', () => {
            // Scenario: Data product with 4 members
            // - A -> B (A has downstream to B, B has upstream from A)
            // - C has no lineage (orphaned pipeline)
            // - D has no lineage (orphaned container)
            const adjacencyList = createAdjacencyList(
                new Map([[B, new Set([A])]]), // B <- A (B has upstream A)
                new Map([[A, new Set([B])]]), // A -> B (A has downstream B)
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // A and B are connected. C and D are orphaned.
            expect(result).toEqual(new Set([A, B]));
            expect(result).not.toContain(C);
            expect(result).not.toContain(D);
        });

        it('preserves nodes in a linear chain', () => {
            // Chain: A -> B -> D
            const adjacencyList = createAdjacencyList(
                new Map([
                    [B, new Set([A])],
                    [D, new Set([B])],
                ]),
                new Map([
                    [A, new Set([B])],
                    [B, new Set([D])],
                ]),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // All nodes in the chain should be kept
            expect(result).toEqual(new Set([A, B, D]));
            // C (not in chain) should be filtered out
            expect(result).not.toContain(C);
        });

        it('handles nodes with multiple upstream connections', () => {
            // Multiple inputs: A -> B, C -> B
            // B has upstream from both A and C
            const adjacencyList = createAdjacencyList(
                new Map([
                    [B, new Set([A, C])], // B has upstream from A and C
                ]),
                new Map(),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // B has upstream edges. A and C don't have any edges.
            expect(result).toEqual(new Set([B]));
            expect(result).not.toContain(A);
            expect(result).not.toContain(C);
            expect(result).not.toContain(D);
        });

        it('handles nodes with multiple downstream connections', () => {
            // Multiple outputs: A -> B, A -> C
            // A has downstream to both B and C
            const adjacencyList = createAdjacencyList(
                new Map(),
                new Map([
                    [A, new Set([B, C])], // A has downstream to B and C
                ]),
            );

            const result = getNodesWithLineage([A, B, C, D], adjacencyList);

            // A has downstream edges. B and C don't have any edges.
            expect(result).toEqual(new Set([A]));
            expect(result).not.toContain(B);
            expect(result).not.toContain(C);
            expect(result).not.toContain(D);
        });

        it('handles complex branching scenarios (diamond pattern)', () => {
            // Diamond pattern: A -> B, A -> C, B -> D, C -> D
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

        it('handles nodes with self-loops', () => {
            // Self-loop: A -> A (edge case)
            const adjacencyList = createAdjacencyList(
                new Map([[A, new Set([A])]]), // A has upstream from itself
                new Map([[A, new Set([A])]]), // A has downstream to itself
            );

            const result = getNodesWithLineage([A, B], adjacencyList);

            // A has edges (to itself), B doesn't
            expect(result).toEqual(new Set([A]));
            expect(result).not.toContain(B);
        });

        it('filters only specified nodes from input list', () => {
            // Only check lineage for A and B, don't check C and D
            const adjacencyList = createAdjacencyList(
                new Map(),
                new Map([[A, new Set([B])]]), // A -> B
            );

            const result = getNodesWithLineage([A, B], adjacencyList); // Only check A and B

            expect(result).toEqual(new Set([A]));
            expect(result).not.toContain(C); // C wasn't even checked
            expect(result).not.toContain(D); // D wasn't even checked
        });

        it('preserves order-independent results', () => {
            const adjacencyList = createAdjacencyList(new Map([[B, new Set([A])]]), new Map([[A, new Set([B])]]));

            const result1 = getNodesWithLineage([A, B, C], adjacencyList);
            const result2 = getNodesWithLineage([C, B, A], adjacencyList);
            const result3 = getNodesWithLineage([B, A, C], adjacencyList);

            expect(result1).toEqual(result2);
            expect(result2).toEqual(result3);
        });
    });
});
