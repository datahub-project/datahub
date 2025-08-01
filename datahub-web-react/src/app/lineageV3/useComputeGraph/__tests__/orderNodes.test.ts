import { FetchStatus, LineageEntity } from '@app/lineageV3/common';
import orderNodes from '@app/lineageV3/useComputeGraph/orderNodes';

import { EntityType, LineageDirection } from '@types';

describe('orderNodes', () => {
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

    it('should return empty array for non-existent root node', () => {
        const nodes = new Map<string, LineageEntity>();
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };
        const result = orderNodes('nonexistent', LineageDirection.Downstream, {
            nodes,
            adjacencyList,
            rootType: EntityType.Dataset,
        });
        expect(result).toEqual([]);
    });

    it('should return only root node when it has no children', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const nodes = new Map([['root', rootNode]]);
        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>(),
        };
        const result = orderNodes('root', LineageDirection.Downstream, {
            nodes,
            adjacencyList,
            rootType: EntityType.Dataset,
        });
        expect(result).toEqual([]);
    });

    it('should order nodes in BFS order', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const child1 = createMockNode('child1', EntityType.Dataset);
        const child2 = createMockNode('child2', EntityType.Dataset);
        const grandchild1 = createMockNode('grandchild1', EntityType.Dataset);
        const grandchild2 = createMockNode('grandchild2', EntityType.Dataset);

        const nodes = new Map([
            ['root', rootNode],
            ['child1', child1],
            ['child2', child2],
            ['grandchild1', grandchild1],
            ['grandchild2', grandchild2],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>([
                ['root', new Set(['child1', 'child2'])],
                ['child1', new Set(['grandchild1'])],
                ['child2', new Set(['grandchild2'])],
            ]),
        };

        const result = orderNodes('root', LineageDirection.Downstream, {
            nodes,
            adjacencyList,
            rootType: EntityType.Dataset,
        });

        // Verify BFS order: children before grandchildren
        expect(result.indexOf(child1)).toBeLessThan(result.indexOf(grandchild1));
        expect(result.indexOf(child2)).toBeLessThan(result.indexOf(grandchild2));
    });

    it('should order transformations last within same level', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const normalNode = createMockNode('normal', EntityType.Dataset);
        const transformNode = createMockNode('transform', EntityType.DataJob);

        const nodes = new Map([
            ['root', rootNode],
            ['normal', normalNode],
            ['transform', transformNode],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>([['root', new Set(['normal', 'transform'])]]),
        };

        const result = orderNodes('root', LineageDirection.Downstream, {
            nodes,
            adjacencyList,
            rootType: EntityType.Dataset,
        });

        // Verify transformation is ordered last
        expect(result.indexOf(normalNode)).toBeLessThan(result.indexOf(transformNode));
    });

    it('should order alphabetically within same type', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const nodeA = createMockNode('nodeA', EntityType.Dataset);
        const nodeB = createMockNode('nodeB', EntityType.Dataset);
        const nodeC = createMockNode('nodeC', EntityType.Dataset);

        const nodes = new Map([
            ['root', rootNode],
            ['nodeA', nodeA],
            ['nodeB', nodeB],
            ['nodeC', nodeC],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>(),
            [LineageDirection.Downstream]: new Map<string, Set<string>>([
                ['root', new Set(['nodeC', 'nodeA', 'nodeB'])],
            ]),
        };

        const result = orderNodes('root', LineageDirection.Downstream, {
            nodes,
            adjacencyList,
            rootType: EntityType.Dataset,
        });

        // Verify alphabetical order
        expect(result.indexOf(nodeA)).toBeLessThan(result.indexOf(nodeB));
        expect(result.indexOf(nodeB)).toBeLessThan(result.indexOf(nodeC));
    });

    it('should handle both upstream and downstream directions', () => {
        const rootNode = createMockNode('root', EntityType.Dataset);
        const upstreamNode = createMockNode('upstream', EntityType.Dataset);
        const downstreamNode = createMockNode('downstream', EntityType.Dataset);

        const nodes = new Map([
            ['root', rootNode],
            ['upstream', upstreamNode],
            ['downstream', downstreamNode],
        ]);

        const adjacencyList = {
            [LineageDirection.Upstream]: new Map<string, Set<string>>([['root', new Set(['upstream'])]]),
            [LineageDirection.Downstream]: new Map<string, Set<string>>([['root', new Set(['downstream'])]]),
        };

        const upstreamResult = orderNodes('root', LineageDirection.Upstream, {
            nodes,
            adjacencyList,
            rootType: EntityType.Dataset,
        });

        const downstreamResult = orderNodes('root', LineageDirection.Downstream, {
            nodes,
            adjacencyList,
            rootType: EntityType.Dataset,
        });

        expect(upstreamResult).toEqual([upstreamNode]);
        expect(downstreamResult).toEqual([downstreamNode]);
    });
});
