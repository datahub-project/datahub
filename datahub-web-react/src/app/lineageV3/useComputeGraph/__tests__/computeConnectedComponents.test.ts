import { FetchStatus, LineageEntity } from '@app/lineageV3/common';
import computeConnectedComponents from '@app/lineageV3/useComputeGraph/computeConnectedComponents';

import { EntityType, LineageDirection } from '@types';

describe('computeConnectedComponents', () => {
    const createMockNode = (urn: string, type: EntityType): LineageEntity => ({
        id: urn,
        urn,
        type,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: true },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.UNFETCHED,
            [LineageDirection.Downstream]: FetchStatus.UNFETCHED,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    });

    it('should handle empty graph', () => {
        const nodes = new Map<string, LineageEntity>();
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };
        const edges = new Map();

        const result = computeConnectedComponents({ nodes, adjacencyList, edges });

        expect(result.displayedNodesByRoots.length).toBe(0);
        expect(result.parents.size).toBe(0);
    });

    it('should handle single node graph', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const nodes = new Map([['root', rootNode]]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };
        const edges = new Map();

        const result = computeConnectedComponents({ nodes, adjacencyList, edges });

        expect(result.displayedNodesByRoots.length).toBe(1);
        expect(result.displayedNodesByRoots[0]).toEqual([[rootNode], [rootNode]]);
        expect(result.parents.size).toBe(0);
    });

    it('should handle simple linear graph', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const childNode = createMockNode('child', EntityType.Dataset);
        const nodes = new Map([
            ['root', rootNode],
            ['child', childNode],
        ]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>([['child', new Set(['root'])]]),
            [LineageDirection.Downstream]: new Map<string, Set<string>>([['root', new Set(['child'])]]),
        };
        const edges = new Map();

        const result = computeConnectedComponents({ nodes, adjacencyList, edges });

        expect(result.displayedNodesByRoots.length).toBe(1);
        expect(result.displayedNodesByRoots[0]).toEqual([[rootNode], [rootNode, childNode]]);
        expect(result.parents.get('child')).toEqual(new Set(['root']));
    });

    it('should handle disconnected components', () => {
        const root1 = createMockNode('root1', EntityType.Dataset);
        const child1 = createMockNode('child1', EntityType.Dataset);
        const root2 = createMockNode('root2', EntityType.Dataset);
        const child2 = createMockNode('child2', EntityType.Dataset);

        const nodes = new Map([
            ['root1', root1],
            ['child1', child1],
            ['root2', root2],
            ['child2', child2],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>([
                ['child1', new Set(['root1'])],
                ['child2', new Set(['root2'])],
            ]),
            [LineageDirection.Downstream]: new Map<string, Set<string>>([
                ['root1', new Set(['child1'])],
                ['root2', new Set(['child2'])],
            ]),
        };
        const edges = new Map();

        const result = computeConnectedComponents({ nodes, adjacencyList, edges });

        expect(result.displayedNodesByRoots.length).toBe(2);
        // Check first component
        const component1 = result.displayedNodesByRoots.find(([roots]) => roots.includes(root1));
        expect(component1?.[1]).toEqual([root1, child1]);
        // Check second component
        const component2 = result.displayedNodesByRoots.find(([roots]) => roots.includes(root2));
        expect(component2?.[1]).toEqual([root2, child2]);
    });

    it('should handle complex graph with multiple paths', () => {
        const root = createMockNode('root', EntityType.Dataset);
        const child1 = createMockNode('child1', EntityType.Dataset);
        const child2 = createMockNode('child2', EntityType.Dataset);
        const grandchild = createMockNode('grandchild', EntityType.Dataset);

        const nodes = new Map([
            ['root', root],
            ['child1', child1],
            ['child2', child2],
            ['grandchild', grandchild],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>([
                ['child1', new Set(['root'])],
                ['child2', new Set(['root'])],
                ['grandchild', new Set(['child1', 'child2'])],
            ]),
            [LineageDirection.Downstream]: new Map<string, Set<string>>([
                ['root', new Set(['child1', 'child2'])],
                ['child1', new Set(['grandchild'])],
                ['child2', new Set(['grandchild'])],
            ]),
        };
        const edges = new Map();

        const result = computeConnectedComponents({ nodes, adjacencyList, edges });

        expect(result.displayedNodesByRoots.length).toBe(1);
        const component = result.displayedNodesByRoots[0][1];
        expect(component).toContain(root);
        expect(component).toContain(child1);
        expect(component).toContain(child2);
        expect(component).toContain(grandchild);
        expect(result.parents.get('grandchild')).toEqual(new Set(['child1', 'child2']));
    });
});
