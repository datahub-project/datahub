import { renderHook } from '@testing-library/react-hooks';

import useInitialDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useInitialDomains';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));

describe('useInitialDomains', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    test('should skip query when domainUrns is empty', () => {
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({ data: undefined, loading: false } as any);
        const { result } = renderHook(() => useInitialDomains([]));
        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
            }),
        );
        expect(result.current.domains).toEqual([]);
    });

    test('should call query with correct variables when domainUrns is provided', () => {
        const domainUrns = ['domain1', 'domain2'];
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({ data: undefined, loading: false } as any);

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

    test('should return filtered domains from data', () => {
        const domainUrns = ['domain1', 'domain2'];
        const mockData = {
            searchAcrossEntities: {
                searchResults: [
                    { entity: { urn: 'domain1', type: EntityType.Domain } },
                    { entity: { urn: 'domain2', type: EntityType.Domain } },
                    { entity: { urn: 'invalid' } },
                ],
            },
        };

        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: mockData,
            loading: false,
        } as any);

        const { result } = renderHook(() => useInitialDomains(domainUrns));
        expect(result.current.domains).toEqual([
            { urn: 'domain1', type: EntityType.Domain },
            { urn: 'domain2', type: EntityType.Domain },
        ]);
    });

    test('should return undefined domains when data is undefined', () => {
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: undefined,
            loading: false,
        } as any);

        const { result } = renderHook(() => useInitialDomains(['domain1']));
        expect(result.current.domains).toBeUndefined();
    });

    test('should return empty array if search results are missing', () => {
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: {},
            loading: false,
        } as any);

        const { result } = renderHook(() => useInitialDomains(['domain1']));
        expect(result.current.domains).toEqual([]);
    });

    test('should memoize domains when domainUrns and data do not change', () => {
        const domainUrns = ['domain1'];
        const mockData = {
            searchAcrossEntities: {
                searchResults: [{ entity: { urn: 'domain1' } }],
            },
        };

        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({
            data: mockData,
            loading: false,
        } as any);

        const { result, rerender } = renderHook(() => useInitialDomains(domainUrns));
        const firstDomains = result.current.domains;
        rerender();
        expect(result.current.domains).toBe(firstDomains);
    });
});
