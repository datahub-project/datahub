import { renderHook } from '@testing-library/react-hooks';
import { describe, expect, it, vi } from 'vitest';

import { LineageEntity, NodeContext } from '@app/lineageV3/common';
import useResetLineageGraph from '@app/lineageV3/initialize/useResetLineageGraph';

import { EntityType, LineageDirection } from '@types';

vi.mock('@app/lineage/utils/useGetLineageTimeParams', () => ({
    useGetLineageTimeParams: () => ({ startTimeMillis: undefined, endTimeMillis: undefined }),
}));

vi.mock('@app/lineageV3/common', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/lineageV3/common')>()),
    useIgnoreSchemaFieldStatus: () => false,
}));

const URN = 'urn:li:dataset:(urn:li:dataPlatform:mysql,db.t,PROD)';

function makeContext(): NodeContext {
    return {
        nodes: new Map(),
        edges: new Map(),
        adjacencyList: {
            [LineageDirection.Upstream]: new Map(),
            [LineageDirection.Downstream]: new Map(),
        },
        setNodeVersion: vi.fn(),
        setDisplayVersion: vi.fn(),
        showGhostEntities: false,
    } as unknown as NodeContext;
}

const makeInitialNode = () => ({ id: URN, urn: URN, type: EntityType.Dataset }) as LineageEntity;

describe('useResetLineageGraph', () => {
    it('clears stale graph state, seeds the home node, and resets versions on mount', () => {
        const context = makeContext();
        // Pre-populate to verify the reset actually clears everything
        context.nodes.set('stale-urn', makeInitialNode());
        context.edges.set('stale-edge', {} as never);
        context.adjacencyList[LineageDirection.Upstream].set('a', new Set(['b']));
        context.adjacencyList[LineageDirection.Downstream].set('b', new Set(['a']));

        renderHook(() => useResetLineageGraph(context, URN, EntityType.Dataset, makeInitialNode));

        expect(Array.from(context.nodes.keys())).toEqual([URN]);
        expect(context.edges.size).toBe(0);
        expect(context.adjacencyList[LineageDirection.Upstream].size).toBe(0);
        expect(context.adjacencyList[LineageDirection.Downstream].size).toBe(0);
        expect(context.setNodeVersion).toHaveBeenCalledWith(0);
        expect(context.setDisplayVersion).toHaveBeenCalledWith([0, []]);
    });
});
