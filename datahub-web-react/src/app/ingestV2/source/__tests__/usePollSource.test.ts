import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';
import { updateListIngestionSourcesCache } from '@app/ingestV2/source/cacheUtils';
import usePollSource from '@app/ingestV2/source/usePollSource';

import { useGetIngestionSourceLazyQuery } from '@graphql/ingestion.generated';

const REFRESH_INTERVAL_MS = 3000;
const MAX_EMPTY_RETRIES = 10;
const sourceUrn1 = 'urn:li:source1';
const sourceUrn2 = 'urn:li:source2';
const requestUrn = 'urn:li:request';

// Mocks
vi.mock('@app/ingestV2/executions/utils', () => ({
    isExecutionRequestActive: vi.fn(),
}));
vi.mock('@app/ingestV2/source/cacheUtils', () => ({
    updateListIngestionSourcesCache: vi.fn(),
}));
vi.mock('@graphql/ingestion.generated', () => ({
    useGetIngestionSourceLazyQuery: vi.fn(),
}));

const getMocks = () => {
    return {
        setFinalSources: vi.fn(),
        setSourcesToRefetch: vi.fn(),
        setExecutedUrns: vi.fn(),
        getIngestionSourceQuery: vi.fn(),
    };
};

// Setup the hook and capture onCompleted
const setupHook = ({
    urn = sourceUrn1,
    isExecutedNow = false,
    queryInputs = {},
    setFinalSources,
    setSourcesToRefetch,
    setExecutedUrns,
    getIngestionSourceQuery,
    client,
}) => {
    let onCompleted;
    (useGetIngestionSourceLazyQuery as any).mockImplementation((opts) => {
        onCompleted = opts.onCompleted;
        return [getIngestionSourceQuery, { client }];
    });
    renderHook(() =>
        usePollSource({
            urn,
            setFinalSources,
            setSourcesToRefetch,
            setExecutedUrns,
            queryInputs,
            isExecutedNow,
        }),
    );
    return { onCompleted };
};

const triggerOnCompleted = (onCompleted, data) => {
    act(() => {
        onCompleted(data);
    });
};

describe('usePollSource', () => {
    let setFinalSources;
    let setSourcesToRefetch;
    let setExecutedUrns;
    let getIngestionSourceQuery;
    let client;

    beforeEach(() => {
        vi.useFakeTimers();
        ({ setFinalSources, setSourcesToRefetch, setExecutedUrns, getIngestionSourceQuery } = getMocks());
        client = {};
        (useGetIngestionSourceLazyQuery as any).mockReturnValue([getIngestionSourceQuery, { client }]);
        vi.mocked(isExecutionRequestActive).mockReset();
        vi.mocked(updateListIngestionSourcesCache).mockReset();
    });

    it('should not poll if urn is missing', () => {
        renderHook(() =>
            usePollSource({
                urn: '',
                setFinalSources,
                setSourcesToRefetch,
                setExecutedUrns,
                queryInputs: {},
                isExecutedNow: false,
            }),
        );
        vi.advanceTimersByTime(REFRESH_INTERVAL_MS);
        expect(getIngestionSourceQuery).not.toHaveBeenCalled();
    });

    it('should call getIngestionSourceQuery after interval', () => {
        renderHook(() =>
            usePollSource({
                urn: sourceUrn1,
                setFinalSources,
                setSourcesToRefetch,
                setExecutedUrns,
                queryInputs: {},
                isExecutedNow: false,
            }),
        );
        vi.advanceTimersByTime(REFRESH_INTERVAL_MS);
        expect(getIngestionSourceQuery).toHaveBeenCalledWith({ variables: { urn: sourceUrn1 } });
    });

    it('should return early if ingestion source is missing in query result data', () => {
        const { onCompleted } = setupHook({
            setFinalSources,
            setSourcesToRefetch,
            setExecutedUrns,
            getIngestionSourceQuery,
            client,
        });
        triggerOnCompleted(onCompleted, { data: { ingestionSource: undefined } });
        expect(setFinalSources).not.toHaveBeenCalled();
        expect(updateListIngestionSourcesCache).not.toHaveBeenCalled();
    });

    it('should handle undefined execution requests in source data', () => {
        const { onCompleted } = setupHook({
            setFinalSources,
            setSourcesToRefetch,
            setExecutedUrns,
            getIngestionSourceQuery,
            client,
        });
        triggerOnCompleted(onCompleted, { ingestionSource: { urn: sourceUrn1 } });
        expect(setFinalSources).toHaveBeenCalled();
        const updater = setFinalSources.mock.calls[0][0];
        const prevSources = [{ urn: sourceUrn1, executions: [] }];
        const result = updater(prevSources);
        expect(result[0].executions).toBeUndefined();
    });

    it('should update the final sources and cache with the updated source', () => {
        const setFinalSourcesMock = vi.fn();
        const { onCompleted } = setupHook({
            setFinalSources: setFinalSourcesMock,
            setSourcesToRefetch,
            setExecutedUrns,
            getIngestionSourceQuery,
            client,
        });
        const updatedSource = {
            urn: sourceUrn1,
            executions: { executionRequests: [{ urn: requestUrn }] },
        };
        triggerOnCompleted(onCompleted, { ingestionSource: updatedSource });
        expect(setFinalSourcesMock).toHaveBeenCalled();
        expect(updateListIngestionSourcesCache).toHaveBeenCalledWith(client, updatedSource, {}, true);

        // First argument of first call of mock calls
        const cb = setFinalSourcesMock.mock.calls[0][0];
        const prev = [{ urn: sourceUrn1, executions: { executionRequests: [] } }, { urn: sourceUrn2 }];
        const result = cb(prev);
        expect(result[0]).toEqual(updatedSource);
        expect(result[1]).toEqual({ urn: sourceUrn2 });
    });

    it('should keep polling when an execution request is active', () => {
        const { onCompleted } = setupHook({
            setFinalSources,
            setSourcesToRefetch,
            setExecutedUrns,
            getIngestionSourceQuery,
            client,
        });
        const updatedSource = {
            urn: sourceUrn1,
            executions: { executionRequests: [{ urn: requestUrn }] },
        };
        (isExecutionRequestActive as any).mockReturnValue(true);
        triggerOnCompleted(onCompleted, { ingestionSource: updatedSource });
        expect(setFinalSources).toHaveBeenCalled();

        // Call the query again after interval
        getIngestionSourceQuery.mockClear();
        vi.advanceTimersByTime(REFRESH_INTERVAL_MS);
        expect(getIngestionSourceQuery).toHaveBeenCalledWith({ variables: { urn: sourceUrn1 } });
    });

    it('should remove urn from polling if not executed recently and no executions', () => {
        const { onCompleted } = setupHook({
            setFinalSources,
            setSourcesToRefetch,
            setExecutedUrns,
            getIngestionSourceQuery,
            client,
        });
        triggerOnCompleted(onCompleted, {
            ingestionSource: { urn: sourceUrn1, executions: { executionRequests: [] } },
        });
        expect(setSourcesToRefetch).toHaveBeenCalled();

        const callback = setSourcesToRefetch.mock.calls[0][0];
        const prevSet = new Set([sourceUrn1, sourceUrn2]);
        const resultSet = callback(prevSet);
        expect(resultSet.has(sourceUrn1)).toBe(false);
        expect(resultSet.has(sourceUrn2)).toBe(true);
    });

    it('should remove urn from polling when execution completes', () => {
        const setSourcesToRefetchMock = vi.fn();
        const setExecutedUrnsMock = vi.fn();
        const { onCompleted } = setupHook({
            setFinalSources,
            setSourcesToRefetch: setSourcesToRefetchMock,
            setExecutedUrns: setExecutedUrnsMock,
            getIngestionSourceQuery,
            client,
        });
        triggerOnCompleted(onCompleted, {
            ingestionSource: {
                urn: sourceUrn1,
                executions: { executionRequests: [{ urn: requestUrn }] },
            },
        });
        (isExecutionRequestActive as any).mockReturnValue(false);

        expect(setSourcesToRefetchMock).toHaveBeenCalled();
        const cb = setSourcesToRefetchMock.mock.calls[0][0];
        const result = cb(new Set([sourceUrn1, sourceUrn2]));
        expect(result.has(sourceUrn1)).toBe(false);
        expect(setExecutedUrnsMock).toHaveBeenCalled();
        const cb2 = setExecutedUrnsMock.mock.calls[0][0];
        const result2 = cb2(new Set([sourceUrn1, sourceUrn2]));
        expect(result2.has(sourceUrn1)).toBe(false);
    });

    it('should stop polling after maximum retries', () => {
        const { onCompleted } = setupHook({
            setFinalSources,
            setSourcesToRefetch,
            setExecutedUrns,
            getIngestionSourceQuery,
            client,
            isExecutedNow: true,
        });
        for (let i = 0; i < MAX_EMPTY_RETRIES; i++) {
            triggerOnCompleted(onCompleted, {
                ingestionSource: { urn: sourceUrn1, executions: { executionRequests: [] } },
            });
            vi.advanceTimersByTime(REFRESH_INTERVAL_MS);
        }
        expect(setSourcesToRefetch).toHaveBeenCalled();
        expect(setExecutedUrns).toHaveBeenCalled();

        const refetchCb = setSourcesToRefetch.mock.calls[0][0];
        const executedCb = setExecutedUrns.mock.calls[0][0];

        const prevSet = new Set([sourceUrn1, sourceUrn2]);
        const refetchResult = refetchCb(prevSet);
        const executedResult = executedCb(prevSet);

        expect(refetchResult.has(sourceUrn1)).toBe(false);
        expect(refetchResult.has(sourceUrn2)).toBe(true);

        expect(executedResult.has(sourceUrn1)).toBe(false);
        expect(executedResult.has(sourceUrn2)).toBe(true);
    });
});
