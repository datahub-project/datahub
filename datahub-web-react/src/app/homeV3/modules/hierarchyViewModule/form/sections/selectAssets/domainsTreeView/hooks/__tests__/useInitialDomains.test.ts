import { renderHook } from '@testing-library/react-hooks';

import { isDomain } from '@app/entityV2/domain/utils';
import useInitialDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useInitialDomains';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));

vi.mock('@app/entityV2/domain/utils', () => ({
    isDomain: vi.fn(),
}));

describe('useInitialDomains', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    test('skips query when domainUrns is empty', () => {
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: undefined,
            loading: false,
        } as any);
        const { result } = renderHook(() => useInitialDomains([]));
        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(expect.objectContaining({ skip: true }));
        expect(result.current.domains).toEqual([]);
    });

    test('calls query with correct variables when domainUrns is provided', () => {
        const domainUrns = ['domain1', 'domain2'];
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: undefined,
            loading: false,
        } as any);

        renderHook(() => useInitialDomains(domainUrns));

        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith({
            variables: {
                input: {
                    query: '*',
                    types: [EntityType.Domain],
                    filters: [
                        {
                            field: 'urn',
                            condition: FilterOperator.Equal,
                            values: domainUrns,
                        },
                    ],
                    count: domainUrns.length,
                },
            },
            skip: false,
            fetchPolicy: 'cache-and-network',
        });
    });

    test('filters and maps domains from search results', () => {
        const domainUrns = ['domain1', 'domain2'];
        const mockData = {
            searchAcrossEntities: {
                searchResults: [
                    { entity: { urn: 'domain1' } },
                    { entity: { urn: 'domain2' } },
                    { entity: { urn: 'invalid' } },
                ],
            },
        };

        // Mock isDomain to return true only for domain1 and domain2
        vi.mocked(isDomain).mockImplementation((entity) => domainUrns.includes(entity?.urn ?? ''));

        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: mockData,
            loading: false,
        } as any);

        const { result } = renderHook(() => useInitialDomains(domainUrns));
        expect(result.current.domains).toEqual([{ urn: 'domain1' }, { urn: 'domain2' }]);
    });

    test('returns undefined domains when data is undefined', () => {
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: undefined,
            loading: false,
        } as any);

        const { result } = renderHook(() => useInitialDomains(['domain1']));
        expect(result.current.domains).toBeUndefined();
    });

    test('returns empty array if search results are missing', () => {
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: {},
            loading: false,
        } as any);

        const { result } = renderHook(() => useInitialDomains(['domain1']));
        expect(result.current.domains).toEqual([]);
    });

    test('memoizes domains when domainUrns and data do not change', () => {
        const domainUrns = ['domain1'];
        const mockData = {
            searchAcrossEntities: {
                searchResults: [{ entity: { urn: 'domain1' } }],
            },
        };

        vi.mocked(isDomain).mockReturnValue(true);
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: mockData,
            loading: false,
        } as any);

        const { result, rerender } = renderHook(() => useInitialDomains(domainUrns));
        const firstDomains = result.current.domains;
        rerender();
        expect(result.current.domains).toBe(firstDomains); // Same reference due to useMemo
    });

    test('updates domains when domainUrns change', () => {
        const initialUrns = ['domain1'];
        const updatedUrns = ['domain2'];

        const mockData1 = {
            searchAcrossEntities: {
                searchResults: [{ entity: { urn: 'domain1' } }],
            },
        };

        const mockData2 = {
            searchAcrossEntities: {
                searchResults: [{ entity: { urn: 'domain2' } }],
            },
        };

        vi.mocked(useGetSearchResultsForMultipleQuery)
            .mockReturnValueOnce({ data: mockData1, loading: false } as any)
            .mockReturnValueOnce({ data: mockData2, loading: false } as any);

        vi.mocked(isDomain).mockReturnValue(true);

        const { result, rerender } = renderHook((props) => useInitialDomains(props), { initialProps: initialUrns });

        expect(result.current.domains).toEqual([{ urn: 'domain1' }]);
        rerender(updatedUrns);
        expect(result.current.domains).toEqual([{ urn: 'domain2' }]);
    });

    test('returns loading state', () => {
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: undefined,
            loading: true,
        } as any);

        const { result } = renderHook(() => useInitialDomains(['domain1']));
        expect(result.current.loading).toBe(true);
    });
});
