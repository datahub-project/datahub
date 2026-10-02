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

describe('useSearchResultLineageCounts', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        useHideLineageMock.mockReturnValue(false);
        useCountsQueryMock.mockReturnValue({ data: undefined, loading: true });
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

    it('maps returned totals by urn', () => {
        useCountsQueryMock.mockReturnValue({
            loading: false,
            data: {
                entities: [
                    { urn: URN, upstream: { filtered: 0, total: 2 }, downstream: { filtered: 1, total: 5 } },
                    null,
                ],
            },
        });

        const { result } = renderHook(() => useSearchResultLineageCounts([URN]));

        expect(result.current.loading).toBe(false);
        expect(result.current.countsByUrn.get(URN)).toEqual({
            urn: URN,
            upstream: { filtered: 0, total: 2 },
            downstream: { filtered: 1, total: 5 },
        });
    });
});
