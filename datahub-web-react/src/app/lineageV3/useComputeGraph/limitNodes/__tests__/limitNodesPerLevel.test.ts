import { describe, expect, it } from 'vitest';

import { LINEAGE_FILTER_NODE_NAME } from '@app/lineageV3/LineageFilterNode/LineageFilterNodeBasic';
import { LineageNode } from '@app/lineageV3/common';
import { limitNodesPerLevel } from '@app/lineageV3/useComputeGraph/limitNodes/limitNodesPerLevel';

import { EntityType, LineageDirection } from '@types';

const makeNode = (id: string, type: string = EntityType.Dataset): LineageNode =>
    ({
        id,
        type,
    }) as LineageNode;

describe('limitNodesPerLevel', () => {
    it('should return only root level when no nodes', () => {
        const adjacency: Record<LineageDirection, Map<string, Set<string>>> = {
            [LineageDirection.Downstream]: new Map(),
            [LineageDirection.Upstream]: new Map(),
        };
        const result = limitNodesPerLevel({
            nodes: [],
            rootUrn: 'root',
            rootType: EntityType.Dataset,
            adjacencyList: adjacency,
            maxPerLevel: 2,
        });
        expect(result.limitedNodes).toEqual([]);
        expect(result.levelsInfo).toEqual({});
        expect(result.levelsMap.size).toBe(1);
        expect(result.levelsMap.get('root')).toBe(0);
    });

    it('should return only root if it is the only node', () => {
        const root = makeNode('root');
        const adjacency: Record<LineageDirection, Map<string, Set<string>>> = {
            [LineageDirection.Downstream]: new Map(),
            [LineageDirection.Upstream]: new Map(),
        };
        const result = limitNodesPerLevel({
            nodes: [root],
            rootUrn: 'root',
            rootType: EntityType.Dataset,
            adjacencyList: adjacency,
            maxPerLevel: 2,
        });
        expect(result.limitedNodes.map((n) => n.id)).toEqual(['root']);
        expect(result.levelsMap.get('root')).toBe(0);
    });

    it('should include downstream entities up to maxPerLevel', () => {
        const root = makeNode('root');
        const a = makeNode('a');
        const b = makeNode('b');
        const c = makeNode('c');

        const adjacency: Record<LineageDirection, Map<string, Set<string>>> = {
            [LineageDirection.Downstream]: new Map([['root', new Set(['a', 'b', 'c'])]]),
            [LineageDirection.Upstream]: new Map(),
        };

        const result = limitNodesPerLevel({
            nodes: [root, a, b, c],
            rootUrn: 'root',
            rootType: EntityType.Dataset,
            adjacencyList: adjacency,
            maxPerLevel: 2,
        });

        expect(result.levelsMap.get('root')).toBe(0);
        const level1Info = result.levelsInfo[1];
        expect(level1Info.shownEntities).toBe(2);
        expect(level1Info.hiddenEntities).toBe(1);

        const limitedIds = result.limitedNodes.map((n) => n.id);
        expect(limitedIds).toContain('root');
        expect(limitedIds.length).toBe(3);
    });

    it('should add transform nodes even when maxPerLevel limits entities', () => {
        const root = makeNode('root');
        const t1 = makeNode('t1', EntityType.DataJob);
        const e1 = makeNode('e1');

        const adjacency: Record<LineageDirection, Map<string, Set<string>>> = {
            [LineageDirection.Downstream]: new Map([['root', new Set(['t1', 'e1'])]]),
            [LineageDirection.Upstream]: new Map(),
        };

        const result = limitNodesPerLevel({
            nodes: [root, t1, e1],
            rootUrn: 'root',
            rootType: EntityType.Dataset,
            adjacencyList: adjacency,
            maxPerLevel: 1,
        });

        const limitedIds = result.limitedNodes.map((n) => n.id);
        expect(limitedIds).toContain('root');
        expect(limitedIds).toContain('t1');
        expect(limitedIds.length).toBe(3);
    });

    it('should not return filter nodes', () => {
        const root = makeNode('root');
        const f1 = makeNode('f1', LINEAGE_FILTER_NODE_NAME);
        const f2 = makeNode('f2', LINEAGE_FILTER_NODE_NAME);

        const adjacency: Record<LineageDirection, Map<string, Set<string>>> = {
            [LineageDirection.Downstream]: new Map([['root', new Set(['f1'])]]),
            [LineageDirection.Upstream]: new Map([['root', new Set(['f2'])]]),
        };

        const result = limitNodesPerLevel({
            nodes: [root, f1, f2],
            rootUrn: 'root',
            rootType: EntityType.Dataset,
            adjacencyList: adjacency,
            maxPerLevel: 2,
        });

        const limitedIds = result.limitedNodes.map((n) => n.id);

        // Check that filter nodes are not included
        expect(limitedIds).not.toContain('f1');
        expect(limitedIds).not.toContain('f2');

        // Root should still be included
        expect(limitedIds).toContain('root');
    });

    it('should merge transform nodes into entity levels', () => {
        const root = makeNode('root');
        const t1 = makeNode('t1', EntityType.DataJob);
        const e1 = makeNode('e1');

        const adjacency: Record<LineageDirection, Map<string, Set<string>>> = {
            [LineageDirection.Downstream]: new Map([
                ['root', new Set(['t1'])],
                ['t1', new Set(['e1'])],
            ]),
            [LineageDirection.Upstream]: new Map([
                ['t1', new Set(['root'])],
                ['e1', new Set(['t1'])],
            ]),
        };

        const result = limitNodesPerLevel({
            nodes: [root, t1, e1],
            rootUrn: 'root',
            rootType: EntityType.Dataset,
            adjacencyList: adjacency,
            maxPerLevel: 5,
        });
        expect(result.levelsMap.get('e1')).toBe(1);
        const ids = result.limitedNodes.map((n) => n.id);
        expect(ids).toContain('t1');
        expect(ids).toContain('e1');
    });

    it('should handle mix of entity, transform, and filter nodes', () => {
        const root = makeNode('root');
        const t1 = makeNode('t1', EntityType.DataJob);
        const a = makeNode('a');
        const f1 = makeNode('f1', LINEAGE_FILTER_NODE_NAME);

        const adjacency: Record<LineageDirection, Map<string, Set<string>>> = {
            [LineageDirection.Downstream]: new Map([['root', new Set(['t1', 'a', 'f1'])]]),
            [LineageDirection.Upstream]: new Map(),
        };

        const result = limitNodesPerLevel({
            nodes: [root, t1, a, f1],
            rootUrn: 'root',
            rootType: EntityType.Dataset,
            adjacencyList: adjacency,
            maxPerLevel: 2,
        });

        const ids = result.limitedNodes.map((n) => n.id);
        expect(ids).toContain('root');
        expect(ids).toContain('t1');
        expect(ids).toContain('a');
        expect(ids).not.toContain('f1');

        expect(result.levelsMap.get('a')).toBe(1);
        expect(result.levelsMap.get('root')).toBe(0);
    });

    it('should handle multi-level downstream hierarchy respecting maxPerLevel', () => {
        const root = makeNode('root');
        const a = makeNode('a');
        const b = makeNode('b');
        const c = makeNode('c');
        const d = makeNode('d');

        const adjacency: Record<LineageDirection, Map<string, Set<string>>> = {
            [LineageDirection.Downstream]: new Map([
                ['root', new Set(['a', 'b'])],
                ['a', new Set(['c', 'd'])],
            ]),
            [LineageDirection.Upstream]: new Map(),
        };

        const result = limitNodesPerLevel({
            nodes: [root, a, b, c, d],
            rootUrn: 'root',
            rootType: EntityType.Dataset,
            adjacencyList: adjacency,
            maxPerLevel: 1,
        });

        expect(result.levelsMap.get('root')).toBe(0);
        expect(result.levelsMap.get('a')).toBe(1);
        expect(result.levelsMap.get('b')).toBe(1);
        expect(result.levelsMap.get('c')).toBe(2);
        expect(result.levelsMap.get('d')).toBe(2);

        const limitedIds = result.limitedNodes.map((n) => n.id);
        expect(limitedIds).toContain('root');
        expect(limitedIds).toContain('a');
        expect(limitedIds).toContain('c');
    });

    it('should correctly assign levels for upstream adjacency nodes', () => {
        const root = makeNode('root');
        const u1 = makeNode('u1');
        const u2 = makeNode('u2');

        const adjacency: Record<LineageDirection, Map<string, Set<string>>> = {
            [LineageDirection.Downstream]: new Map(),
            [LineageDirection.Upstream]: new Map([['root', new Set(['u1', 'u2'])]]),
        };

        const result = limitNodesPerLevel({
            nodes: [root, u1, u2],
            rootUrn: 'root',
            rootType: EntityType.Dataset,
            adjacencyList: adjacency,
            maxPerLevel: 2,
        });

        expect(result.levelsMap.get('u1')).toBe(-1);
        expect(result.levelsMap.get('u2')).toBe(-1);
    });
});
