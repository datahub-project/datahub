import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useSearchResultLineageCounts } from '@app/searchV2/useSearchResultLineageCounts';
import { useHideLineageInSearchCards } from '@app/useAppConfig';

import { useGetSearchResultLineageCountsQuery } from '@graphql/lineage.generated';

vi.mock('@graphql/lineage.generated', () => ({
    useGetSearchResultLineageCountsQuery: vi.fn(),
}));

vi.mock('@app/useAppConfig', () => ({
    useHideLineageInSearchCards: vi.fn(),
}));

const useCountsQueryMock = useGetSearchResultLineageCountsQuery as unknown as ReturnType<typeof vi.fn>;
const useHideLineageMock = useHideLineageInSearchCards as unknown as ReturnType<typeof vi.fn>;

const URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)';
const OTHER_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.other,PROD)';

describe('useSearchResultLineageCounts', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        useHideLineageMock.mockReturnValue(false);
        useCountsQueryMock.mockReturnValue({ data: undefined, loading: true, error: undefined });
    });

    it('does not fetch until there are results to count', () => {
        const { result } = renderHook(() => useSearchResultLineageCounts([]));

        expect(useCountsQueryMock).toHaveBeenCalledWith(
            expect.objectContaining({ skip: true, variables: { urns: [] } }),
        );
        expect(result.current.loading).toBe(false);
    });

    it('skips the fetch when search cards hide lineage', () => {
        useHideLineageMock.mockReturnValue(true);
        const { result } = renderHook(() => useSearchResultLineageCounts([URN]));

        expect(useCountsQueryMock).toHaveBeenCalledWith(expect.objectContaining({ skip: true }));
        expect(result.current.loading).toBe(false);
    });

    it('reports loading until the deferred counts arrive', () => {
        const { result } = renderHook(() => useSearchResultLineageCounts([URN, URN]));

        expect(useCountsQueryMock).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: false,
                fetchPolicy: 'cache-first',
                variables: { urns: [URN] },
            }),
        );
        expect(result.current.loading).toBe(true);
        expect(result.current.countsByUrn.size).toBe(0);
    });

    it('keeps prior counts while refetching but does not zero-settle new URNs', () => {
        useCountsQueryMock.mockReturnValue({
            loading: true,
            error: undefined,
            data: {
                entities: [{ urn: URN, upstream: { filtered: 0, total: 2 }, downstream: { filtered: 1, total: 5 } }],
            },
        });

        const { result } = renderHook(() => useSearchResultLineageCounts([URN, OTHER_URN]));

        expect(result.current.loading).toBe(true);
        expect(result.current.countsByUrn.has(URN)).toBe(true);
        // Stale Apollo `data` must not settle pagination URNs as zero while loading.
        expect(result.current.countsByUrn.has(OTHER_URN)).toBe(false);
        expect(result.current.countsByUrn.size).toBe(1);
    });

    it('does not settle new page URNs from a prior batch while loading', () => {
        useCountsQueryMock.mockReturnValue({
            loading: true,
            error: undefined,
            data: {
                entities: [{ urn: URN, upstream: { filtered: 0, total: 2 }, downstream: { filtered: 1, total: 5 } }],
            },
        });

        // Page turn: visible URNs no longer include the cached entity.
        const { result } = renderHook(() => useSearchResultLineageCounts([OTHER_URN]));

        expect(result.current.loading).toBe(true);
        expect(result.current.countsByUrn.has(OTHER_URN)).toBe(false);
        // Stale cached entity may still be indexed, but must not zero-fill the new page.
        expect(result.current.countsByUrn.get(OTHER_URN)).toBeUndefined();
    });

    it('maps returned totals by urn and settles missing URNs', () => {
        useCountsQueryMock.mockReturnValue({
            loading: false,
            error: undefined,
            data: {
                entities: [
                    { urn: URN, upstream: { filtered: 0, total: 2 }, downstream: { filtered: 1, total: 5 } },
                    null,
                ],
            },
        });

        const { result } = renderHook(() => useSearchResultLineageCounts([URN, OTHER_URN]));

        expect(result.current.loading).toBe(false);
        expect(result.current.countsByUrn.get(URN)).toEqual({
            urn: URN,
            upstream: { filtered: 0, total: 2 },
            downstream: { filtered: 1, total: 5 },
        });
        expect(result.current.countsByUrn.get(OTHER_URN)).toEqual({
            urn: OTHER_URN,
            upstream: { total: 0, filtered: 0 },
            downstream: { total: 0, filtered: 0 },
        });
    });

    it('leaves counts empty on query failure so the sidebar can fall back', () => {
        useCountsQueryMock.mockReturnValue({
            loading: false,
            error: new Error('lineage counts failed'),
            data: undefined,
        });

        const { result } = renderHook(() => useSearchResultLineageCounts([URN]));

        expect(result.current.loading).toBe(false);
        expect(result.current.countsByUrn.size).toBe(0);
        expect(result.current.error).toBeTruthy();
    });

    it('ignores stale data when the deferred query fails', () => {
        useCountsQueryMock.mockReturnValue({
            loading: false,
            error: new Error('lineage counts failed'),
            data: {
                entities: [{ urn: URN, upstream: { filtered: 0, total: 2 }, downstream: { filtered: 1, total: 5 } }],
            },
        });

        const { result } = renderHook(() => useSearchResultLineageCounts([URN, OTHER_URN]));

        expect(result.current.countsByUrn.size).toBe(0);
        expect(result.current.error).toBeTruthy();
    });
});
