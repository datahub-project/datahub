import { MockedProvider } from '@apollo/client/testing';
import { act, renderHook } from '@testing-library/react-hooks';
import React, { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { FetchStatus, LineageEntity, NodeContext } from '@app/lineageV3/common';
import useSearchAcrossLineage from '@app/lineageV3/queries/useSearchAcrossLineage';

import { SearchAcrossLineageStructureDocument } from '@graphql/search.generated';
import { EntityType, LineageDirection } from '@types';

// The selected time range is read from the URL via useGetLineageTimeParams. Drive it directly so we
// can simulate the user changing the range between renders.
const timeParams = vi.hoisted(() => ({
    current: { startTimeMillis: undefined, endTimeMillis: undefined } as {
        startTimeMillis?: number;
        endTimeMillis?: number;
    },
}));

vi.mock('@app/lineage/utils/useGetLineageTimeParams', () => ({
    useGetLineageTimeParams: () => timeParams.current,
}));

// Not under test here; keep it a no-op so the fetch behavior is isolated.
vi.mock('@app/lineageV3/queries/pruneAllDuplicateEdges', () => ({ default: vi.fn() }));

// Alchemy notification renders into the DOM via antd; stub it so failures are observable without it.
const notificationError = vi.hoisted(() => vi.fn());
vi.mock('@components', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@components')>()),
    notification: { error: notificationError, success: vi.fn(), info: vi.fn(), open: vi.fn() },
}));

const URN = 'urn:li:dataset:(urn:li:dataPlatform:mysql,db.t,PROD)';

function makeContext(): NodeContext {
    return {
        rootUrn: URN,
        rootType: EntityType.Dataset,
        nodes: new Map(),
        edges: new Map(),
        adjacencyList: {
            [LineageDirection.Upstream]: new Map(),
            [LineageDirection.Downstream]: new Map(),
        },
        setNodeVersion: vi.fn(),
        setDisplayVersion: vi.fn(),
    } as unknown as NodeContext;
}

// Seed the home node the way the graph initializer does: both directions LOADING.
function seedLoadingRoot(context: NodeContext) {
    context.nodes.set(URN, {
        id: URN,
        urn: URN,
        type: EntityType.Dataset,
        direction: LineageDirection.Upstream,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: false },
        fetchStatus: {
            [LineageDirection.Upstream]: FetchStatus.LOADING,
            [LineageDirection.Downstream]: FetchStatus.LOADING,
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    } as unknown as LineageEntity);
}

const flush = (ms: number) =>
    act(async () => {
        await new Promise((resolve) => {
            setTimeout(resolve, ms);
        });
    });

describe('useSearchAcrossLineage', () => {
    beforeEach(() => {
        timeParams.current = { startTimeMillis: undefined, endTimeMillis: undefined };
        notificationError.mockClear();
    });

    it('refetches lineage when the selected time range changes', async () => {
        let requestCount = 0;
        const mocks = Array.from({ length: 6 }).map(() => ({
            request: { query: SearchAcrossLineageStructureDocument },
            variableMatcher: () => true,
            result: () => {
                requestCount += 1;
                return { data: { searchAcrossLineage: { start: 0, count: 0, total: 0, searchResults: [] } } };
            },
        }));

        const wrapper = ({ children }: { children?: ReactNode }) => (
            <MockedProvider mocks={mocks} addTypename={false}>
                {children}
            </MockedProvider>
        );

        const context = makeContext();
        const { rerender } = renderHook(
            () => useSearchAcrossLineage(URN, EntityType.Dataset, context, LineageDirection.Upstream),
            { wrapper },
        );

        await flush(100);
        expect(requestCount).toBe(1);

        // User picks a time range in the lineage graph -> params change.
        timeParams.current = { startTimeMillis: 1_000, endTimeMillis: 2_000 };
        rerender();
        await flush(200);

        expect(requestCount).toBe(2);
    });

    it('stops the loading state and surfaces an error when the lineage query fails', async () => {
        // Reproduces the "hangs indefinitely" report: when the (time-filtered) query fails, the node
        // must leave LOADING and the loading gate must be released instead of spinning forever.
        const mocks = [
            {
                request: { query: SearchAcrossLineageStructureDocument },
                variableMatcher: () => true,
                error: new Error('backend stalled'),
            },
        ];

        const wrapper = ({ children }: { children?: ReactNode }) => (
            <MockedProvider mocks={mocks} addTypename={false}>
                {children}
            </MockedProvider>
        );

        const context = makeContext();
        seedLoadingRoot(context);

        renderHook(() => useSearchAcrossLineage(URN, EntityType.Dataset, context, LineageDirection.Upstream), {
            wrapper,
        });

        await flush(200);

        const node = context.nodes.get(URN) as LineageEntity;
        // UNFETCHED, not COMPLETE: the node leaves LOADING (spinner stops, gate released via the
        // nodeVersion bump) but stays retryable — COMPLETE would hide the expand control.
        expect(node.fetchStatus[LineageDirection.Upstream]).toBe(FetchStatus.UNFETCHED);
        expect(context.setNodeVersion).toHaveBeenCalled();
        expect(notificationError).toHaveBeenCalled();
    });

    it('re-arms the stall timeout after a time-range change so a second stall is still caught', async () => {
        // Regression: a stalled request keeps `loading` true, so the retry (Apollo auto-refetches when
        // the time params change) never produces a loading edge. If the timeout only re-armed on that
        // edge, `settledRef` would stay latched from the first timeout and a second stall would hang.
        vi.useFakeTimers();
        try {
            // Never-resolving requests keep `loading` true through both attempts.
            const mocks = Array.from({ length: 2 }).map(() => ({
                request: { query: SearchAcrossLineageStructureDocument },
                variableMatcher: () => true,
                delay: Infinity,
            }));

            const wrapper = ({ children }: { children?: ReactNode }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            );

            const context = makeContext();
            seedLoadingRoot(context);

            const { rerender } = renderHook(
                () => useSearchAcrossLineage(URN, EntityType.Dataset, context, LineageDirection.Upstream),
                { wrapper },
            );

            await act(async () => {
                await vi.advanceTimersByTimeAsync(60_000);
            });
            expect(notificationError).toHaveBeenCalledTimes(1);

            // User retries by picking a new time range while the first request is still in flight.
            timeParams.current = { startTimeMillis: 1_000, endTimeMillis: 2_000 };
            rerender();
            await act(async () => {
                await vi.advanceTimersByTimeAsync(60_000);
            });
            // Without the re-arm this stays 1 (settledRef latched, no new timer).
            expect(notificationError).toHaveBeenCalledTimes(2);
        } finally {
            vi.useRealTimers();
        }
    });
});
