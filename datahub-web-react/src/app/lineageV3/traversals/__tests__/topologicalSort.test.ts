import topologicalSort from '@app/lineageV3/traversals/topologicalSort';

describe('topologicalSort', () => {
    it('should handle empty graph', () => {
        const nodes = new Set<string>();
        const children = new Map<string, Set<string>>();
        const sorted = topologicalSort(nodes, children);
        expect(sorted).toEqual([]);
    });

    it('should handle single node', () => {
        const nodes = new Set(['node1']);
        const children = new Map<string, Set<string>>([['node1', new Set()]]);
        const sorted = topologicalSort(nodes, children);
        expect(sorted).toEqual(['node1']);
    });

    it('should handle linear chain', () => {
        const nodes = new Set(['node1', 'node2', 'node3']);
        const children = new Map<string, Set<string>>([
            ['node1', new Set(['node2'])],
            ['node2', new Set(['node3'])],
            ['node3', new Set()],
        ]);
        const sorted = topologicalSort(nodes, children);
        expect(sorted).toEqual(['node1', 'node2', 'node3']);
    });

    it('should handle diamond pattern', () => {
        const nodes = new Set(['node4', 'node3', 'node2', 'node1']);
        const children = new Map<string, Set<string>>([
            ['node1', new Set(['node2', 'node3'])],
            ['node2', new Set(['node4'])],
            ['node3', new Set(['node4'])],
            ['node4', new Set()],
        ]);
        const sorted = topologicalSort(nodes, children);
        expect(sorted[0]).toBe('node1');
        expect(sorted[3]).toBe('node4');
    });

    it('should detect cycles', () => {
        const debugSpy = vitest.spyOn(console, 'debug').mockImplementation(() => {});
        const nodes = new Set(['node1', 'node2', 'node3']);
        const children = new Map<string, Set<string>>([
            ['node1', new Set(['node2'])],
            ['node2', new Set(['node3'])],
            ['node3', new Set(['node1'])],
        ]);
        const sorted = topologicalSort(nodes, children);
        expect(debugSpy).toHaveBeenCalledWith('Cycle detected in topological sort for node: node1');
        // Even with cycles, we should get a valid ordering that respects as many dependencies as possible
        expect(sorted).toHaveLength(3);
        expect(new Set(sorted)).toEqual(nodes);
    });

    it('should handle disconnected components', () => {
        const nodes = new Set(['node1', 'node2', 'node3', 'node4']);
        const children = new Map<string, Set<string>>([
            ['node1', new Set(['node2'])],
            ['node2', new Set()],
            ['node3', new Set(['node4'])],
            ['node4', new Set()],
        ]);
        const sorted = topologicalSort(nodes, children);
        expect(sorted.indexOf('node1')).toBeLessThan(sorted.indexOf('node2'));
        expect(sorted.indexOf('node3')).toBeLessThan(sorted.indexOf('node4'));
        expect(new Set(sorted)).toEqual(nodes);
    });

    it('should handle nodes with no children', () => {
        const nodes = new Set(['node1', 'node2', 'node3']);
        const children = new Map<string, Set<string>>([
            ['node1', new Set()],
            ['node2', new Set()],
            ['node3', new Set()],
        ]);
        const sorted = topologicalSort(nodes, children);
        expect(sorted).toHaveLength(3);
        expect(new Set(sorted)).toEqual(nodes);
    });

    it('topologically sorts a complex directed acyclic graph', () => {
        const debugSpy = vitest.spyOn(console, 'debug').mockImplementation(() => {});

        const nodes = new Set([
            'dj1',
            'dj2',
            'dj3',
            'dj4',
            'dj5',
            'dj6',
            'dj7',
            'dj8',
            'dj9',
            'dj10',
            'dj11',
            'dj12',
            'dj13',
            'dj14',
            'dj15',
        ]);

        const rawChildren: [string, string[]][] = [
            ['dj10', ['dj1', 'dj6', 'dj7', 'dj8', 'dj15']],
            ['dj13', ['dj2']],
            ['dj1', ['dj3']],
            ['dj6', ['dj4']],
            ['dj12', ['dj5']],
            ['dj8', ['dj9']],
            ['dj5', ['dj10']],
            ['dj7', ['dj11']],
            ['dj3', ['dj13']],
            ['dj14', ['dj13']],
            ['dj4', ['dj13']],
            ['dj9', ['dj13']],
            ['dj11', ['dj13']],
            ['dj15', ['dj14']],
        ];
        const children = new Map(rawChildren.map(([urn, neighbors]) => [urn, new Set(neighbors)]));

        const sorted = topologicalSort(nodes, children);
        expect(debugSpy).not.toHaveBeenCalledOnce();

        expect(sorted[0]).toBe('dj12');
        expect(sorted[1]).toBe('dj5');
        expect(sorted[2]).toBe('dj10');
        expect(sorted[nodes.size - 2]).toBe('dj13');
        expect(sorted[nodes.size - 1]).toBe('dj2');
        expect(sorted.indexOf('dj1')).toBeLessThan(sorted.indexOf('dj3'));
        expect(sorted.indexOf('dj15')).toBeLessThan(sorted.indexOf('dj14'));
        expect(sorted.indexOf('dj8')).toBeLessThan(sorted.indexOf('dj9'));
        expect(sorted.indexOf('dj7')).toBeLessThan(sorted.indexOf('dj11'));
    });
});
