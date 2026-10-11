import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { LineageEntity, NodeContext } from '@app/lineageV3/common';
import useResetLineageGraph from '@app/lineageV3/initialize/useResetLineageGraph';

import { EntityType, LineageDirection } from '@types';

const mocks = vi.hoisted(() => ({ ignoreSchemaFieldStatus: false }));

vi.mock('@app/lineage/utils/useGetLineageTimeParams', () => ({
    useGetLineageTimeParams: () => ({ startTimeMillis: undefined, endTimeMillis: undefined }),
}));

vi.mock('@app/lineageV3/common', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/lineageV3/common')>()),
    useIgnoreSchemaFieldStatus: () => mocks.ignoreSchemaFieldStatus,
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
    beforeEach(() => {
        mocks.ignoreSchemaFieldStatus = false;
    });

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

    it('clears fetched edges and node entities when showGhostEntities changes', () => {
        const context = makeContext();
        const { rerender } = renderHook(
            ({ ctx }) => useResetLineageGraph(ctx, URN, EntityType.Dataset, makeInitialNode),
            { initialProps: { ctx: context } },
        );

        // Simulate fetched state that toggling ghost entities should invalidate.
        const homeNode = context.nodes.get(URN) as LineageEntity;
        homeNode.entity = {} as never;
        context.edges.set('e', {} as never);
        context.adjacencyList[LineageDirection.Upstream].set('a', new Set(['b']));

        rerender({ ctx: { ...context, showGhostEntities: true } });

        expect(context.edges.size).toBe(0);
        expect(context.adjacencyList[LineageDirection.Upstream].size).toBe(0);
        expect(homeNode.entity).toBeUndefined();
    });

    it('leaves fetched state intact on showGhostEntities change for schema fields when ignoring status', () => {
        mocks.ignoreSchemaFieldStatus = true;
        const context = makeContext();
        const { rerender } = renderHook(
            ({ ctx }) => useResetLineageGraph(ctx, URN, EntityType.SchemaField, makeInitialNode),
            { initialProps: { ctx: context } },
        );

        const homeNode = context.nodes.get(URN) as LineageEntity;
        homeNode.entity = {} as never;
        context.edges.set('e', {} as never);

        rerender({ ctx: { ...context, showGhostEntities: true } });

        // The ghost-entity reset is guarded out for schema fields when ignoring status.
        expect(context.edges.size).toBe(1);
        expect(homeNode.entity).not.toBeUndefined();
    });
});
