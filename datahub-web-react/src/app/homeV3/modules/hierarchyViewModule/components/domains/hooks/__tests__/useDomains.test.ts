import { renderHook } from '@testing-library/react-hooks';

import useDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomains';
import { ENTITY_NAME_FIELD } from '@app/searchV2/context/constants';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator, SortOrder } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));

describe('useDomains', () => {
    const mockDomains = [
        { urn: 'urn:li:domain:domain1', type: EntityType.Domain, properties: { name: 'Domain A' } },
        { urn: 'urn:li:domain:domain2', type: EntityType.Domain, properties: { name: 'Domain B' } },
    ];
    const mockRefetch = vi.fn();

    beforeEach(() => {
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: mockDomains.map((domain) => ({ entity: domain })),
                    total: mockDomains.length,
                },
            },
            error: undefined,
            refetch: mockRefetch,
        });
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    const setup = (parentDomainUrn: string | undefined, start: number, count: number, skip: boolean) =>
        renderHook(() => useDomains(parentDomainUrn, start, count, skip));

    it('should return domains, total, loading, and refetch', () => {
        const { result } = setup(undefined, 0, 10, false);

        expect(result.current.loading).toBe(false);
        expect(result.current.domains?.length).toBe(2);
        expect(result.current.total).toBe(2);
        expect(result.current.refetch).toBe(mockRefetch);
        expect(result.current.domains?.[0]?.urn).toBe('urn:li:domain:domain1');
    });

    it('should call useGetSearchResultsForMultipleQuery with correct variables for root domains', () => {
        setup(undefined, 0, 10, false);

        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: {
                    input: {
                        start: 0,
                        count: 10,
                        query: '*',
                        types: [EntityType.Domain],
                        orFilters: [
                            { and: [{ field: 'parentDomain', condition: FilterOperator.Exists, negated: true }] },
                        ],
                        sortInput: {
                            sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
                        },
                        searchFlags: {
                            skipCache: true,
                        },
                    },
                },
                fetchPolicy: 'cache-and-network',
                nextFetchPolicy: 'cache-first',
                skip: false,
            }),
        );
    });

    it('should call useGetSearchResultsForMultipleQuery with correct variables for child domains', () => {
        const parentUrn = 'urn:li:domain:parent';
        setup(parentUrn, 0, 10, false);

        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: {
                    input: {
                        start: 0,
                        count: 10,
                        query: '*',
                        types: [EntityType.Domain],
                        orFilters: [{ and: [{ field: 'parentDomain', values: [parentUrn] }] }],
                        sortInput: {
                            sortCriteria: [{ field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending }],
                        },
                        searchFlags: {
                            skipCache: true,
                        },
                    },
                },
                fetchPolicy: 'cache-and-network',
                nextFetchPolicy: 'cache-first',
                skip: false,
            }),
        );
    });

    it('should return empty array for domains if skip is true', () => {
        const { result } = setup(undefined, 0, 10, true);
        expect(result.current.domains?.length).toBe(0);
        expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
            }),
        );
    });

    it('should handle loading state correctly', () => {
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
            loading: true,
            data: undefined,
            error: undefined,
            refetch: vi.fn(),
        });

        const { result } = setup(undefined, 0, 10, false);
        expect(result.current.loading).toBe(true);
    });

    it('should handle no data gracefully', () => {
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: [],
                    total: undefined, // Explicitly set total to undefined
                },
            },
            error: undefined,
            refetch: vi.fn(),
        });

        const { result } = setup(undefined, 0, 10, false);
        expect(result.current.domains?.length).toBe(0);
        expect(result.current.total).toBeUndefined();
    });

    it('should filter out non-domain entities', () => {
        const mixedResults = [
            { entity: { urn: 'urn:li:domain:domain1', type: EntityType.Domain } },
            { entity: { urn: 'urn:li:dataset:dataset1', type: EntityType.Dataset } },
        ];
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: mixedResults.map((r) => ({ entity: r.entity })),
                    total: mixedResults.length,
                },
            },
            error: undefined,
            refetch: vi.fn(),
        });

        const { result } = setup(undefined, 0, 10, false);
        expect(result.current.domains?.length).toBe(1);
        expect(result.current.domains?.[0]?.urn).toBe('urn:li:domain:domain1');
    });
});
