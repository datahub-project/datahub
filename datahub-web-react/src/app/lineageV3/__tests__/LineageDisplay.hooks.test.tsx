import { act } from '@testing-library/react';
import React from 'react';
import { ReactFlowProvider } from 'reactflow';

import { useFitView, useIsExpanding, useLineageSettled } from '@app/lineageV3/LineageDisplay.hooks';
import { FetchStatus, LineageEntity, LineageNodesContext, NodeContext } from '@app/lineageV3/common';
import { entityNodeDefault } from '@app/lineageV3/queries/useSearchAcrossLineage';
import { renderHook } from '@utils/test-utils/renderHook';

import { EntityType, LineageDirection } from '@types';

const fitView = vi.hoisted(() => vi.fn());
vi.mock('reactflow', async (importOriginal) => {
    const actual = await importOriginal<typeof import('reactflow')>();
    return { ...actual, useReactFlow: () => ({ ...actual.useReactFlow(), fitView }) };
});

beforeEach(() => {
    vi.useFakeTimers();
    fitView.mockClear();
});

afterEach(() => {
    vi.useRealTimers();
});

describe('useFitView', () => {
    it('reports a fit as pending until its animation has finished', () => {
        const { result } = renderHook(() => useFitView(true), {
            wrapper: ({ children }) => <ReactFlowProvider>{children}</ReactFlowProvider>,
        });
        expect(result.current).toBe(true);

        act(() => {
            vi.advanceTimersByTime(1500);
        });
        expect(fitView).toHaveBeenCalledTimes(1);
        expect(result.current).toBe(true);

        act(() => {
            vi.advanceTimersByTime(500);
        });
        expect(result.current).toBe(false);
    });

    it('reports no fit pending before the graph has loaded', () => {
        const { result } = renderHook(() => useFitView(false), {
            wrapper: ({ children }) => <ReactFlowProvider>{children}</ReactFlowProvider>,
        });
        expect(result.current).toBe(false);
        expect(fitView).not.toHaveBeenCalled();
    });
});

describe('useLineageSettled', () => {
    function renderSettled(initialProps: { loading: boolean; fitting: boolean }) {
        return renderHook(({ loading, fitting }) => useLineageSettled(loading, fitting), {
            initialProps,
            wrapper: ({ children }) => <ReactFlowProvider>{children}</ReactFlowProvider>,
        });
    }

    it('settles shortly after loading and fitting have both finished', () => {
        const { result, rerender } = renderSettled({ loading: true, fitting: false });
        expect(result.current).toBe(false);

        rerender({ loading: false, fitting: true });
        act(() => {
            vi.advanceTimersByTime(1000);
        });
        expect(result.current).toBe(false);

        rerender({ loading: false, fitting: false });
        expect(result.current).toBe(false);
        act(() => {
            vi.advanceTimersByTime(300);
        });
        expect(result.current).toBe(true);
    });

    it('stays unsettled across a brief gap between two requests', () => {
        const { result, rerender } = renderSettled({ loading: true, fitting: false });
        rerender({ loading: false, fitting: false });
        act(() => {
            vi.advanceTimersByTime(100);
        });
        rerender({ loading: true, fitting: false });
        act(() => {
            vi.advanceTimersByTime(300);
        });
        expect(result.current).toBe(false);
    });

    it('is unsettled as soon as loading starts again', () => {
        const { result, rerender } = renderSettled({ loading: false, fitting: false });
        act(() => {
            vi.advanceTimersByTime(300);
        });
        expect(result.current).toBe(true);

        rerender({ loading: true, fitting: false });
        expect(result.current).toBe(false);
    });
});

describe('useIsExpanding', () => {
    it('stops reporting an expansion once its fetch completes without a dataVersion bump', () => {
        const urn = 'urn:li:dataset:(urn:li:dataPlatform:hive,a,PROD)';
        const node: LineageEntity = entityNodeDefault(
            urn,
            EntityType.Dataset,
            LineageDirection.Upstream,
            EntityType.Dataset,
        );
        node.fetchStatus = { ...node.fetchStatus, [LineageDirection.Upstream]: FetchStatus.LOADING };
        const nodes = new Map([[urn, node]]);
        let displayVersion: [number, string[]] = [0, []];
        const { result, rerender } = renderHook(() => useIsExpanding(), {
            wrapper: ({ children }) => (
                <LineageNodesContext.Provider value={{ nodes, displayVersion } as unknown as NodeContext}>
                    {children}
                </LineageNodesContext.Provider>
            ),
        });
        expect(result.current).toBe(true);

        node.fetchStatus = { ...node.fetchStatus, [LineageDirection.Upstream]: FetchStatus.COMPLETE };
        displayVersion = [1, [urn]];
        rerender();
        expect(result.current).toBe(false);
    });
});
