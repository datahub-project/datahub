import { act, renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetChildDataProducts } from '@app/entityV2/summary/modules/dataProducts/useGetChildDataProducts';
import { DOMAINS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));
vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistryV2: vi.fn(),
}));

describe('useGetChildDataProducts', () => {
    const urn = 'urn:li:domain:parentDomain';
    const mockRegistry = {
        getGenericEntityProperties: vi.fn().mockImplementation((type, entity) => ({ ...entity, type })),
    };
    const mockSearchResults = [
        { entity: { urn: 'urn:li:dataProduct:product1', type: EntityType.DataProduct } },
        { entity: { urn: 'urn:li:dataProduct:product2', type: EntityType.DataProduct } },
    ];
    const mockRefetch = vi.fn();

    beforeEach(() => {
        (useEntityData as unknown as any).mockReturnValue({ urn });
        (useEntityRegistryV2 as unknown as any).mockReturnValue(mockRegistry);
        (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValue({
            loading: false,
            data: {
                searchAcrossEntities: {
                    searchResults: mockSearchResults,
                    total: 2,
                },
            },
            error: undefined,
            refetch: mockRefetch,
        });
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    const setup = (initialCount?: number) => renderHook(() => useGetChildDataProducts(initialCount));

    describe('Basic functionality', () => {
        it('should return entities, originEntities, total and not loading', () => {
            const { result } = setup();

            expect(result.current.loading).toBe(false);
            expect(result.current.total).toBe(2);
            expect(result.current.originEntities).toHaveLength(2);
            expect(result.current.entities).toHaveLength(2);
            expect(result.current.originEntities[0].urn).toBe('urn:li:dataProduct:product1');
            expect(result.current.originEntities[1].urn).toBe('urn:li:dataProduct:product2');
            expect(result.current.error).toBeUndefined();
        });

        it('should call entity registry to get generic properties for each entity', () => {
            setup();

            expect(mockRegistry.getGenericEntityProperties).toHaveBeenCalledTimes(2);
            expect(mockRegistry.getGenericEntityProperties).toHaveBeenCalledWith(
                EntityType.DataProduct,
                mockSearchResults[0].entity,
            );
            expect(mockRegistry.getGenericEntityProperties).toHaveBeenCalledWith(
                EntityType.DataProduct,
                mockSearchResults[1].entity,
            );
        });

        it('should use default initialCount of 50 when not provided', () => {
            setup();

            expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
                expect.objectContaining({
                    variables: {
                        input: {
                            query: '*',
                            start: 0,
                            count: 50,
                            types: [EntityType.DataProduct],
                            filters: [
                                {
                                    field: DOMAINS_FILTER_NAME,
                                    value: urn,
                                    values: [urn],
                                },
                            ],
                            searchFlags: { skipCache: true },
                        },
                    },
                    skip: false,
                }),
            );
        });

        it('should use custom initialCount when provided', () => {
            setup(25);

            expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
                expect.objectContaining({
                    variables: {
                        input: {
                            query: '*',
                            start: 0,
                            count: 25,
                            types: [EntityType.DataProduct],
                            filters: [
                                {
                                    field: DOMAINS_FILTER_NAME,
                                    value: urn,
                                    values: [urn],
                                },
                            ],
                            searchFlags: { skipCache: true },
                        },
                    },
                    skip: false,
                }),
            );
        });
    });

    describe('Loading states', () => {
        it('should return loading true if searchLoading is true', () => {
            (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
                loading: true,
                data: undefined,
                error: undefined,
                refetch: vi.fn(),
            });

            const { result } = setup();
            expect(result.current.loading).toBe(true);
        });

        it('should return loading true when no data yet', () => {
            (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
                loading: false,
                data: undefined,
                error: undefined,
                refetch: vi.fn(),
            });

            const { result } = setup();
            expect(result.current.loading).toBe(true);
        });

        it('should return loading false when data is available', () => {
            const { result } = setup();
            expect(result.current.loading).toBe(false);
        });
    });

    describe('Error handling', () => {
        it('should return error if there is an error', () => {
            const mockError = new Error('Search failed');
            (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
                loading: false,
                data: undefined,
                error: mockError,
                refetch: vi.fn(),
            });

            const { result } = setup();
            expect(result.current.error).toBe(mockError);
        });
    });

    describe('Skip query when no URN', () => {
        it('should skip query when urn is not available', () => {
            (useEntityData as unknown as any).mockReturnValueOnce({ urn: undefined });

            setup();

            expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
                expect.objectContaining({
                    skip: true,
                }),
            );
        });

        it('should skip query when urn is empty string', () => {
            (useEntityData as unknown as any).mockReturnValueOnce({ urn: '' });

            setup();

            expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
                expect.objectContaining({
                    skip: true,
                }),
            );
        });
    });

    describe('Empty results handling', () => {
        it('should handle empty search results gracefully', () => {
            (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
                loading: false,
                data: {
                    searchAcrossEntities: {
                        searchResults: [],
                        total: 0,
                    },
                },
                error: undefined,
                refetch: vi.fn(),
            });

            const { result } = setup();

            expect(result.current.loading).toBe(false);
            expect(result.current.total).toBe(0);
            expect(result.current.originEntities).toHaveLength(0);
            expect(result.current.entities).toHaveLength(0);
        });

        it('should handle missing searchAcrossEntities data', () => {
            (useGetSearchResultsForMultipleQuery as unknown as any).mockReturnValueOnce({
                loading: false,
                data: {},
                error: undefined,
                refetch: vi.fn(),
            });

            const { result } = setup();

            expect(result.current.loading).toBe(false);
            expect(result.current.total).toBe(0);
            expect(result.current.originEntities).toHaveLength(0);
            expect(result.current.entities).toHaveLength(0);
        });
    });

    describe('fetchEntities function', () => {
        it('should return origin entities when start is 0', async () => {
            const { result } = setup();

            let entities;
            await act(async () => {
                entities = await result.current.fetchEntities(0, 10);
            });

            expect(entities).toHaveLength(2);
            expect(entities[0].urn).toBe('urn:li:dataProduct:product1');
            expect(entities[1].urn).toBe('urn:li:dataProduct:product2');
            expect(mockRefetch).not.toHaveBeenCalled();
        });

        it('should fetch paginated entities for non-zero start', async () => {
            const mockPaginatedResults = [
                { entity: { urn: 'urn:li:dataProduct:product3', type: EntityType.DataProduct } },
            ];
            mockRefetch.mockResolvedValueOnce({
                data: {
                    searchAcrossEntities: {
                        searchResults: mockPaginatedResults,
                    },
                },
            });

            const { result } = setup();

            let entities;
            await act(async () => {
                entities = await result.current.fetchEntities(10, 5);
            });

            expect(entities).toHaveLength(1);
            expect(entities[0].urn).toBe('urn:li:dataProduct:product3');
            expect(mockRefetch).toHaveBeenCalledWith({
                input: {
                    query: '*',
                    start: 10,
                    count: 5,
                    types: [EntityType.DataProduct],
                    filters: [
                        {
                            field: DOMAINS_FILTER_NAME,
                            value: urn,
                            values: [urn],
                        },
                    ],
                    searchFlags: { skipCache: true },
                },
            });
        });

        it('should handle refetch failure gracefully', async () => {
            mockRefetch.mockResolvedValueOnce({
                data: undefined,
            });

            const { result } = setup();

            let entities;
            await act(async () => {
                entities = await result.current.fetchEntities(10, 5);
            });

            expect(entities).toHaveLength(0);
        });

        it('should handle missing searchResults in refetch response', async () => {
            mockRefetch.mockResolvedValueOnce({
                data: {
                    searchAcrossEntities: {
                        searchResults: undefined,
                    },
                },
            });

            const { result } = setup();

            let entities;
            await act(async () => {
                entities = await result.current.fetchEntities(10, 5);
            });

            expect(entities).toHaveLength(0);
        });
    });

    describe('Search input variables', () => {
        it('should generate correct search variables with domain filter', () => {
            setup();

            expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledWith(
                expect.objectContaining({
                    variables: {
                        input: {
                            query: '*',
                            start: 0,
                            count: 50,
                            types: [EntityType.DataProduct],
                            filters: [
                                {
                                    field: DOMAINS_FILTER_NAME,
                                    value: urn,
                                    values: [urn],
                                },
                            ],
                            searchFlags: { skipCache: true },
                        },
                    },
                    skip: false,
                }),
            );
        });

        it('should use wildcard query to find all data products', () => {
            setup();

            const callArgs = (useGetSearchResultsForMultipleQuery as any).mock.calls[0][0];
            expect(callArgs.variables.input.query).toBe('*');
        });

        it('should filter by DataProduct entity type only', () => {
            setup();

            const callArgs = (useGetSearchResultsForMultipleQuery as any).mock.calls[0][0];
            expect(callArgs.variables.input.types).toEqual([EntityType.DataProduct]);
        });

        it('should use DOMAINS_FILTER_NAME for filtering by parent domain', () => {
            setup();

            const callArgs = (useGetSearchResultsForMultipleQuery as any).mock.calls[0][0];
            const domainFilter = callArgs.variables.input.filters.find(
                (filter) => filter.field === DOMAINS_FILTER_NAME,
            );
            expect(domainFilter).toBeDefined();
            expect(domainFilter.value).toBe(urn);
            expect(domainFilter.values).toEqual([urn]);
        });

        it('should set skipCache to true in search flags', () => {
            setup();

            const callArgs = (useGetSearchResultsForMultipleQuery as any).mock.calls[0][0];
            expect(callArgs.variables.input.searchFlags.skipCache).toBe(true);
        });
    });

    describe('Memoization and dependencies', () => {
        it('should update search variables when URN changes', () => {
            const { rerender } = setup();

            // Change URN
            (useEntityData as unknown as any).mockReturnValue({ urn: 'urn:li:domain:newDomain' });
            rerender();

            // Should be called twice - once for initial render, once for rerender with new URN
            expect(useGetSearchResultsForMultipleQuery).toHaveBeenCalledTimes(2);

            const secondCallArgs = (useGetSearchResultsForMultipleQuery as any).mock.calls[1][0];
            expect(secondCallArgs.variables.input.filters[0].value).toBe('urn:li:domain:newDomain');
        });
    });
});
