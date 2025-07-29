import getConnectedComponents from '@app/lineageV3/traversals/getConnectedComponents';

describe('getConnectedComponents', () => {
    it('should return empty array for empty graph', () => {
        const nodes = new Set<string>();
        const neighbors = new Map<string, Set<string>>();
        const components = getConnectedComponents(nodes, neighbors);
        expect(components).toEqual([]);
    });

    it('should return single component for single node', () => {
        const nodes = new Set(['node1']);
        const neighbors = new Map<string, Set<string>>([['node1', new Set<string>()]]);
        const components = getConnectedComponents(nodes, neighbors);
        expect(components).toHaveLength(1);
        expect(components[0]).toEqual(new Set(['node1']));
    });

    it('should return single component for connected nodes', () => {
        const nodes = new Set(['node1', 'node2', 'node3']);
        const neighbors = new Map<string, Set<string>>([
            ['node1', new Set<string>(['node2'])],
            ['node2', new Set<string>(['node1', 'node3'])],
            ['node3', new Set<string>(['node2'])],
        ]);
        const components = getConnectedComponents(nodes, neighbors);
        expect(components).toHaveLength(1);
        expect(components[0]).toEqual(new Set(['node1', 'node2', 'node3']));
    });

    it('should return multiple components for disconnected nodes', () => {
        const nodes = new Set(['node1', 'node2', 'node3', 'node4']);
        const neighbors = new Map<string, Set<string>>([
            ['node1', new Set<string>(['node2'])],
            ['node2', new Set<string>(['node1'])],
            ['node3', new Set<string>(['node4'])],
            ['node4', new Set<string>(['node3'])],
        ]);
        const components = getConnectedComponents(nodes, neighbors);
        expect(components).toHaveLength(2);
        expect(components).toContainEqual(new Set(['node1', 'node2']));
        expect(components).toContainEqual(new Set(['node3', 'node4']));
    });

    it('should handle isolated nodes', () => {
        const nodes = new Set(['node1', 'node2', 'node3']);
        const neighbors = new Map<string, Set<string>>([
            ['node1', new Set<string>()],
            ['node2', new Set<string>()],
            ['node3', new Set<string>()],
        ]);
        const components = getConnectedComponents(nodes, neighbors);
        expect(components).toHaveLength(3);
        expect(components).toContainEqual(new Set(['node1']));
        expect(components).toContainEqual(new Set(['node2']));
        expect(components).toContainEqual(new Set(['node3']));
    });

    it('should handle complex graph with multiple components', () => {
        const nodes = new Set(['node1', 'node2', 'node3', 'node4', 'node5', 'node6']);
        const neighbors = new Map<string, Set<string>>([
            ['node1', new Set<string>(['node2', 'node3'])],
            ['node2', new Set<string>(['node1'])],
            ['node3', new Set<string>(['node1'])],
            ['node4', new Set<string>(['node5'])],
            ['node5', new Set<string>(['node4'])],
            ['node6', new Set<string>()],
        ]);
        const components = getConnectedComponents(nodes, neighbors);
        expect(components).toHaveLength(3);
        expect(components).toContainEqual(new Set(['node1', 'node2', 'node3']));
        expect(components).toContainEqual(new Set(['node4', 'node5']));
        expect(components).toContainEqual(new Set(['node6']));
    });
});
