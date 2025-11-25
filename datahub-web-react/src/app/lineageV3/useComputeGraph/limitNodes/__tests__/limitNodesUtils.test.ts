import { describe, expect, it } from 'vitest';

import { LINEAGE_FILTER_NODE_NAME } from '@app/lineageV3/LineageFilterNode/LineageFilterNodeBasic';
import { LineageNode } from '@app/lineageV3/common';
import {
    assignLevelsToFilterNodes,
    buildAdjacency,
    collapseTransformPath,
    computeEntityLevels,
    computeLimitedLevels,
    groupNodesByLevel,
    isEntityNode,
    mergeTransformNodesIntoLevels,
} from '@app/lineageV3/useComputeGraph/limitNodes/limitNodesUtils';

import { EntityType, LineageDirection } from '@types';

const rootUrn = 'urn:li:dataset:root';
const rootType = EntityType.Dataset;

const makeNode = (id: string, type = 'DATASET', parent?: string, direction?: LineageDirection): LineageNode =>
    ({
        id,
        type,
        parent,
        direction,
    }) as unknown as LineageNode;

describe('limitNodesUtils', () => {
    describe('isEntityNode', () => {
        it('should return true for root node', () => {
            const node = makeNode(rootUrn, rootType);
            expect(isEntityNode(node, rootType, rootUrn)).toBe(true);
        });

        it('should return false for transform nodes', () => {
            const node = makeNode('t1', EntityType.DataJob);
            expect(isEntityNode(node, rootType)).toBe(false);
        });

        it('should return false for filter nodes', () => {
            const node = makeNode('f1', LINEAGE_FILTER_NODE_NAME);
            expect(isEntityNode(node, rootType)).toBe(false);
        });

        it('should return true for normal entity node', () => {
            const node = makeNode('e1');
            expect(isEntityNode(node, rootType)).toBe(true);
        });
    });

    describe('collapseTransformPath', () => {
        it('should collapse a chain of transforms to reach entity', () => {
            const nodes = [makeNode('t1', EntityType.DataJob), makeNode('t2', EntityType.DataJob), makeNode('e1')];
            const adjacency = new Map([
                ['t1', new Set(['t2'])],
                ['t2', new Set(['e1'])],
            ]);
            const visibleNodeIds = new Set(['t1', 't2', 'e1']);
            const nodeById = new Map(nodes.map((n) => [n.id, n]));
            const collapsed = collapseTransformPath('t1', adjacency, visibleNodeIds, nodeById, rootType, rootUrn);
            expect(collapsed.has('e1')).toBe(true);
        });

        it('should stop at root node', () => {
            const adjacency = new Map([['t1', new Set([rootUrn])]]);
            const nodeById = new Map([[rootUrn, makeNode(rootUrn)]]);
            const visibleNodeIds = new Set([rootUrn, 't1']);
            const collapsed = collapseTransformPath('t1', adjacency, visibleNodeIds, nodeById, rootType, rootUrn);
            expect(collapsed.has(rootUrn)).toBe(true);
        });

        it('should ignore invisible nodes', () => {
            const nodes = [makeNode('t1', EntityType.DataJob), makeNode('e1')];
            const adjacency = new Map([['t1', new Set(['e1'])]]);
            const nodeById = new Map(nodes.map((n) => [n.id, n]));
            const visibleNodeIds = new Set(['t1']);
            const collapsed = collapseTransformPath('t1', adjacency, visibleNodeIds, nodeById, rootType, rootUrn);
            expect(collapsed.has('e1')).toBe(false);
        });
    });

    describe('buildAdjacency', () => {
        it('should build adjacency collapsing transforms', () => {
            const nodes = [makeNode(rootUrn), makeNode('t1', EntityType.DataJob, rootUrn), makeNode('e1')];
            const adjacency = new Map([
                [rootUrn, new Set(['t1'])],
                ['t1', new Set(['e1'])],
            ]);
            const visibleNodeIds = new Set(['t1', 'e1', rootUrn]);
            const nodeById = new Map(nodes.map((n) => [n.id, n]));
            const adj = buildAdjacency(adjacency, nodes, rootType, rootUrn, visibleNodeIds, nodeById);
            expect(adj.get(rootUrn)?.has('e1')).toBe(true);
        });

        it('should ignore non-entity non-filter nodes', () => {
            const nodes = [makeNode('t1', EntityType.DataJob)];
            const adjacency = new Map([['t1', new Set(['t2'])]]);
            const nodeById = new Map(nodes.map((n) => [n.id, n]));
            const visibleNodeIds = new Set(['t1']);
            const adj = buildAdjacency(adjacency, nodes, rootType, rootUrn, visibleNodeIds, nodeById);
            expect(adj.size).toBe(0);
        });
    });

    describe('computeEntityLevels', () => {
        it('should assign correct levels downstream', () => {
            const outgoing = new Map([
                ['root', new Set(['a', 'b'])],
                ['a', new Set(['c'])],
            ]);
            const incoming = new Map();
            const entityNodeIds = new Set(['root', 'a', 'b', 'c']);
            const levels = computeEntityLevels('root', outgoing, incoming, entityNodeIds);
            expect(levels.get('root')).toBe(0);
            expect(levels.get('a')).toBe(1);
            expect(levels.get('b')).toBe(1);
            expect(levels.get('c')).toBe(2);
        });

        it('should assign levels upstream', () => {
            const outgoing = new Map();
            const incoming = new Map([
                ['root', new Set(['b'])],
                ['b', new Set(['c'])],
            ]);
            const entityNodeIds = new Set(['root', 'b', 'c']);
            const levels = computeEntityLevels('root', outgoing, incoming, entityNodeIds);
            expect(levels.get('root')).toBe(0);
            expect(levels.get('b')).toBe(-1);
            expect(levels.get('c')).toBe(-2);
        });
    });

    describe('assignLevelsToFilterNodes', () => {
        it('should assign downstream filter levels', () => {
            const parent = makeNode('root');
            const filter = makeNode('f1', LINEAGE_FILTER_NODE_NAME, 'root', LineageDirection.Downstream);
            const levelsMap = new Map([['root', 0]]);
            assignLevelsToFilterNodes([parent, filter], levelsMap);
            expect(levelsMap.get('f1')).toBe(1);
        });

        it('should assign upstream filter levels', () => {
            const parent = makeNode('root');
            const filter = makeNode('f2', LINEAGE_FILTER_NODE_NAME, 'root', LineageDirection.Upstream);
            const levelsMap = new Map([['root', 0]]);
            assignLevelsToFilterNodes([parent, filter], levelsMap);
            expect(levelsMap.get('f2')).toBe(-1);
        });

        it('should skip nodes not filters', () => {
            const n = makeNode('x');
            const levelsMap = new Map([['x', 0]]);
            assignLevelsToFilterNodes([n], levelsMap);
            expect(levelsMap.get('x')).toBe(0);
        });
    });

    describe('groupNodesByLevel', () => {
        it('should group nodes correctly', () => {
            const nodes = [makeNode('a'), makeNode('b'), makeNode('c')];
            const levelsMap = new Map([
                ['a', 0],
                ['b', 1],
                ['c', 1],
            ]);
            const grouped = groupNodesByLevel(nodes, levelsMap);
            expect(grouped[0].map((n) => n.id)).toEqual(['a']);
            expect(grouped[1].map((n) => n.id)).toEqual(['b', 'c']);
        });

        it('should ignore nodes without level', () => {
            const nodes = [makeNode('a')];
            const levelsMap = new Map();
            const grouped = groupNodesByLevel(nodes, levelsMap);
            expect(grouped).toEqual({});
        });
    });

    describe('computeLimitedLevels', () => {
        it('should limit nodes per level and compute info', () => {
            const n1 = makeNode('a');
            const n2 = makeNode('b');
            const n3 = makeNode('c');
            const nodesByLevel = { 0: [n1, n2, n3] };
            const { allowedNodeIds, levelsInfo } = computeLimitedLevels(nodesByLevel, 2, rootType);
            expect(allowedNodeIds.size).toBe(2);
            expect(levelsInfo[0].shownEntities).toBe(2);
            expect(levelsInfo[0].hiddenEntities).toBe(1);
        });

        it('should count transform nodes correctly', () => {
            const t = makeNode('t', EntityType.DataJob);
            const e = makeNode('e1');
            const nodesByLevel = { 0: [e, t] };
            const { levelsInfo } = computeLimitedLevels(nodesByLevel, 1, rootType);
            expect(levelsInfo[0].shownEntities).toBe(2);
            expect(levelsInfo[0].hiddenEntities).toBe(0);
        });
    });

    describe('mergeTransformNodesIntoLevels', () => {
        it('should merge transform nodes into downstream entity level', () => {
            const t1 = makeNode('t1', EntityType.DataJob);
            const e1 = makeNode('e1');
            const allNodes = [t1, e1];
            const nodesByLevel = { 1: [e1] };
            const levelsMap = new Map([
                ['e1', 1],
                ['t1', 0],
            ]);
            const adjacency = new Map([['t1', new Set(['e1'])]]);
            const merged = mergeTransformNodesIntoLevels(
                nodesByLevel,
                allNodes,
                levelsMap,
                rootType,
                adjacency,
                rootUrn,
            );
            expect(merged[1].map((n) => n.id)).toContain('t1');
        });

        it('should ignore transform nodes without children', () => {
            const t1 = makeNode('t1', EntityType.DataJob);
            const nodesByLevel: Record<number, LineageNode[]> = {};
            const merged = mergeTransformNodesIntoLevels(nodesByLevel, [t1], new Map(), rootType, new Map(), rootUrn);
            expect(Object.keys(merged)).toHaveLength(0);
        });
    });
});
