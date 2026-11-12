import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import React, { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { LineageEntity, LineageNodesContext, NodeContext } from '@app/lineageV3/common';
import useBulkEntityLineage from '@app/lineageV3/queries/useBulkEntityLineage';
import { renderHook } from '@utils/test-utils/renderHook';

import { GetBulkEntityLineageV2Document } from '@graphql/lineage.generated';
import { EntityType, LineageDirection } from '@types';

const timeParams = vi.hoisted(() => ({
    current: { startTimeMillis: undefined, endTimeMillis: undefined } as {
        startTimeMillis?: number;
        endTimeMillis?: number;
    },
}));

vi.mock('@app/lineageV3/utils/useGetLineageTimeParams', () => ({
    useGetLineageTimeParams: () => timeParams.current,
}));

vi.mock('@app/useAppConfig', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/useAppConfig')>()),
    useAppConfig: () => ({ config: { featureFlags: {} } }),
}));

vi.mock('@app/useEntityRegistry', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/useEntityRegistry')>()),
    useEntityRegistry: () => ({ getLineageVizConfigV2: () => null, getLineageAssets: () => [] }),
}));

vi.mock('@app/lineageV3/common', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@app/lineageV3/common')>()),
    useIgnoreSchemaFieldStatus: () => false,
}));

vi.mock('@app/lineageV3/queries/pruneAllDuplicateEdges', () => ({ default: vi.fn() }));

const notificationError = vi.hoisted(() => vi.fn());
vi.mock('@components', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@components')>()),
    notification: { error: notificationError, success: vi.fn(), info: vi.fn(), open: vi.fn() },
}));

const URN = 'urn:li:dataset:(urn:li:dataPlatform:mysql,db.t,PROD)';

function makeContext(): NodeContext {
    const nodes = new Map<string, LineageEntity>();
    // A shown node with no entity details yet — the batch will try (and here, fail) to load it.
    nodes.set(URN, {
        id: URN,
        urn: URN,
        type: EntityType.Dataset,
        direction: LineageDirection.Upstream,
        entity: undefined,
        isExpanded: { [LineageDirection.Upstream]: true, [LineageDirection.Downstream]: false },
        fetchStatus: {
            [LineageDirection.Upstream]: 'UNFETCHED',
            [LineageDirection.Downstream]: 'UNNEEDED',
        },
        filters: {
            [LineageDirection.Upstream]: { facetFilters: new Map() },
            [LineageDirection.Downstream]: { facetFilters: new Map() },
        },
    } as unknown as LineageEntity);

    return {
        rootUrn: URN,
        rootType: EntityType.Dataset,
        nodes,
        edges: new Map(),
        adjacencyList: {
            [LineageDirection.Upstream]: new Map(),
            [LineageDirection.Downstream]: new Map(),
        },
        hideTransformations: false,
        showGhostEntities: false,
        dataVersion: 0,
        setDataVersion: vi.fn(),
        setDisplayVersion: vi.fn(),
    } as unknown as NodeContext;
}

describe('useBulkEntityLineage', () => {
    beforeEach(() => {
        timeParams.current = { startTimeMillis: undefined, endTimeMillis: undefined };
        notificationError.mockClear();
    });

    it('surfaces an error and stops re-requesting when a batch fails', async () => {
        let requestCount = 0;
        const mocks = Array.from({ length: 4 }).map(() => ({
            request: { query: GetBulkEntityLineageV2Document },
            variableMatcher: () => {
                requestCount += 1;
                return true;
            },
            error: new Error('backend stalled'),
        }));

        const context = makeContext();
        const wrapper = ({ children }: { children?: ReactNode }) => (
            <MockedProvider mocks={mocks} addTypename={false}>
                <LineageNodesContext.Provider value={context}>{children}</LineageNodesContext.Provider>
            </MockedProvider>
        );

        renderHook(() => useBulkEntityLineage([URN]), { wrapper });

        await waitFor(() =>
            expect(notificationError).toHaveBeenCalledWith(expect.objectContaining({ key: 'lineage-load-error' })),
        );
        // The failed urn is not re-requested in a loop.
        expect(requestCount).toBe(1);
    });

    it('retries a failed urn after the time range changes', async () => {
        let requestCount = 0;
        const mocks = Array.from({ length: 4 }).map(() => ({
            request: { query: GetBulkEntityLineageV2Document },
            variableMatcher: () => {
                requestCount += 1;
                return true;
            },
            error: new Error('backend stalled'),
        }));

        const context = makeContext();
        const wrapper = ({ children }: { children?: ReactNode }) => (
            <MockedProvider mocks={mocks} addTypename={false}>
                <LineageNodesContext.Provider value={context}>{children}</LineageNodesContext.Provider>
            </MockedProvider>
        );

        const { rerender } = renderHook(() => useBulkEntityLineage([URN]), { wrapper });
        await waitFor(() => expect(notificationError).toHaveBeenCalled());
        expect(requestCount).toBe(1);

        timeParams.current = { startTimeMillis: 1_000, endTimeMillis: 2_000 };
        rerender();

        await waitFor(() => expect(requestCount).toBe(2));
    });
});
