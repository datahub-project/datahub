import { act, renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import generateUseDownloadListDataProductAssets from '@app/entityV2/dataProduct/generateUseDownloadListDataProductAssets';

import { useListDataProductAssetsLazyQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useListDataProductAssetsLazyQuery: vi.fn(),
}));

describe('generateUseDownloadListDataProductAssets', () => {
    const urn = 'urn:li:dataProduct:test-product';
    const mockFetchAssets = vi.fn();

    beforeEach(() => {
        (useListDataProductAssetsLazyQuery as unknown as any).mockReturnValue([
            mockFetchAssets,
            {
                data: undefined,
                loading: false,
                error: undefined,
            },
        ]);
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    const setup = () => {
        const useDownloadHook = generateUseDownloadListDataProductAssets({ urn });
        return renderHook(() =>
            useDownloadHook({
                variables: {
                    input: {
                        scrollId: null,
                        query: '*',
                    },
                },
            }),
        );
    };

    describe('initial state', () => {
        it('should return undefined searchResults when no data', () => {
            const { result } = setup();
            expect(result.current.searchResults).toBeUndefined();
            expect(result.current.loading).toBe(false);
            expect(result.current.error).toBeUndefined();
        });

        it('should return loading state from lazy query', () => {
            (useListDataProductAssetsLazyQuery as unknown as any).mockReturnValueOnce([
                mockFetchAssets,
                {
                    data: undefined,
                    loading: true,
                    error: undefined,
                },
            ]);

            const { result } = setup();
            expect(result.current.loading).toBe(true);
        });

        it('should return error state from lazy query', () => {
            const mockError = new Error('Test error');
            (useListDataProductAssetsLazyQuery as unknown as any).mockReturnValueOnce([
                mockFetchAssets,
                {
                    data: undefined,
                    loading: false,
                    error: mockError,
                },
            ]);

            const { result } = setup();
            expect(result.current.error).toBe(mockError);
        });
    });

    describe('searchResults from data', () => {
        it('should format searchResults correctly when data is present', () => {
            const mockData = {
                listDataProductAssets: {
                    total: 2,
                    searchResults: [
                        { entity: { urn: 'urn:li:dataset:1', type: EntityType.Dataset } },
                        { entity: { urn: 'urn:li:dataset:2', type: EntityType.Dataset } },
                    ],
                    facets: [{ field: 'platform', aggregations: [] }],
                },
            };

            (useListDataProductAssetsLazyQuery as unknown as any).mockReturnValueOnce([
                mockFetchAssets,
                {
                    data: mockData,
                    loading: false,
                    error: undefined,
                },
            ]);

            const { result } = setup();
            expect(result.current.searchResults).toBeDefined();
            expect(result.current.searchResults?.total).toBe(2);
            expect(result.current.searchResults?.count).toBe(2);
            expect(result.current.searchResults?.searchResults).toHaveLength(2);
            expect(result.current.searchResults?.facets).toHaveLength(1);
            expect(result.current.searchResults?.nextScrollId).toBeNull();
        });

        it('should handle empty search results', () => {
            const mockData = {
                listDataProductAssets: {
                    total: 0,
                    searchResults: [],
                    facets: [],
                },
            };

            (useListDataProductAssetsLazyQuery as unknown as any).mockReturnValueOnce([
                mockFetchAssets,
                {
                    data: mockData,
                    loading: false,
                    error: undefined,
                },
            ]);

            const { result } = setup();
            expect(result.current.searchResults?.total).toBe(0);
            expect(result.current.searchResults?.count).toBe(0);
            expect(result.current.searchResults?.searchResults).toHaveLength(0);
        });
    });

    describe('refetch', () => {
        it('should call fetchAssets with correct default parameters', async () => {
            mockFetchAssets.mockResolvedValueOnce({
                data: {
                    listDataProductAssets: {
                        total: 5,
                        searchResults: [{ entity: { urn: 'urn:li:dataset:1', type: EntityType.Dataset } }],
                        facets: [],
                    },
                },
            });

            const { result } = setup();

            await act(async () => {
                await result.current.refetch({
                    scrollId: null,
                    query: '*',
                    types: [EntityType.Dataset],
                });
            });

            expect(mockFetchAssets).toHaveBeenCalledWith({
                variables: {
                    urn,
                    input: {
                        types: [EntityType.Dataset],
                        query: '*',
                        start: 0,
                        count: 100,
                        orFilters: undefined,
                        viewUrn: undefined,
                        searchFlags: undefined,
                    },
                },
            });
        });

        it('should parse scrollId and use as start parameter', async () => {
            mockFetchAssets.mockResolvedValueOnce({
                data: {
                    listDataProductAssets: {
                        total: 200,
                        searchResults: [{ entity: { urn: 'urn:li:dataset:51', type: EntityType.Dataset } }],
                        facets: [],
                    },
                },
            });

            const { result } = setup();

            await act(async () => {
                await result.current.refetch({
                    scrollId: 'start:50',
                    query: 'test',
                    count: 25,
                });
            });

            expect(mockFetchAssets).toHaveBeenCalledWith({
                variables: {
                    urn,
                    input: {
                        types: [],
                        query: 'test',
                        start: 50,
                        count: 25,
                        orFilters: undefined,
                        viewUrn: undefined,
                        searchFlags: undefined,
                    },
                },
            });
        });

        it('should include all optional parameters when provided', async () => {
            const orFilters = [{ and: [{ field: 'platform', values: ['snowflake'] }] }];
            const searchFlags = { skipCache: true };

            mockFetchAssets.mockResolvedValueOnce({
                data: {
                    listDataProductAssets: {
                        total: 10,
                        searchResults: [],
                        facets: [],
                    },
                },
            });

            const { result } = setup();

            await act(async () => {
                await result.current.refetch({
                    scrollId: null,
                    query: 'filter test',
                    types: [EntityType.Dataset, EntityType.Dashboard],
                    orFilters,
                    viewUrn: 'urn:li:view:123',
                    searchFlags,
                });
            });

            expect(mockFetchAssets).toHaveBeenCalledWith({
                variables: {
                    urn,
                    input: {
                        types: [EntityType.Dataset, EntityType.Dashboard],
                        query: 'filter test',
                        start: 0,
                        count: 100,
                        orFilters,
                        viewUrn: 'urn:li:view:123',
                        searchFlags,
                    },
                },
            });
        });

        it('should return formatted results with nextScrollId when more results available', async () => {
            mockFetchAssets.mockResolvedValueOnce({
                data: {
                    listDataProductAssets: {
                        total: 150,
                        searchResults: new Array(50).fill(null).map((_, i) => ({
                            entity: { urn: `urn:li:dataset:${i}`, type: EntityType.Dataset },
                        })),
                        facets: [],
                    },
                },
            });

            const { result } = setup();

            let downloadResults;
            await act(async () => {
                downloadResults = await result.current.refetch({
                    scrollId: 'start:0',
                    query: '*',
                    count: 50,
                });
            });

            expect(downloadResults).toBeDefined();
            expect(downloadResults?.total).toBe(150);
            expect(downloadResults?.count).toBe(50);
            expect(downloadResults?.nextScrollId).toBe('start:50');
            expect(downloadResults?.searchResults).toHaveLength(50);
        });

        it('should return null nextScrollId when at end of results', async () => {
            mockFetchAssets.mockResolvedValueOnce({
                data: {
                    listDataProductAssets: {
                        total: 100,
                        searchResults: new Array(50).fill(null).map((_, i) => ({
                            entity: { urn: `urn:li:dataset:${i + 50}`, type: EntityType.Dataset },
                        })),
                        facets: [],
                    },
                },
            });

            const { result } = setup();

            let downloadResults;
            await act(async () => {
                downloadResults = await result.current.refetch({
                    scrollId: 'start:50',
                    query: '*',
                    count: 50,
                });
            });

            expect(downloadResults?.nextScrollId).toBeNull();
            expect(downloadResults?.count).toBe(50);
            expect(downloadResults?.total).toBe(100);
        });

        it('should return null when search data is not available', async () => {
            mockFetchAssets.mockResolvedValueOnce({
                data: null,
            });

            const { result } = setup();

            let downloadResults;
            await act(async () => {
                downloadResults = await result.current.refetch({
                    scrollId: null,
                    query: '*',
                });
            });

            expect(downloadResults).toBeNull();
        });

        it('should handle invalid scrollId format', async () => {
            mockFetchAssets.mockResolvedValueOnce({
                data: {
                    listDataProductAssets: {
                        total: 10,
                        searchResults: [],
                        facets: [],
                    },
                },
            });

            const { result } = setup();

            await act(async () => {
                await result.current.refetch({
                    scrollId: 'invalid-format',
                    query: '*',
                });
            });

            expect(mockFetchAssets).toHaveBeenCalledWith({
                variables: {
                    urn,
                    input: expect.objectContaining({
                        start: 0, // Should default to 0 for invalid format
                    }),
                },
            });
        });

        it('should handle undefined scrollId', async () => {
            mockFetchAssets.mockResolvedValueOnce({
                data: {
                    listDataProductAssets: {
                        total: 10,
                        searchResults: [],
                        facets: [],
                    },
                },
            });

            const { result } = setup();

            await act(async () => {
                await result.current.refetch({
                    scrollId: undefined,
                    query: '*',
                });
            });

            expect(mockFetchAssets).toHaveBeenCalledWith({
                variables: {
                    urn,
                    input: expect.objectContaining({
                        start: 0,
                    }),
                },
            });
        });
    });

    describe('scroll pagination logic', () => {
        it('should calculate correct nextScrollId for pagination', async () => {
            mockFetchAssets.mockResolvedValueOnce({
                data: {
                    listDataProductAssets: {
                        total: 300,
                        searchResults: new Array(100).fill(null).map((_, i) => ({
                            entity: { urn: `urn:li:dataset:${i + 100}`, type: EntityType.Dataset },
                        })),
                        facets: [],
                    },
                },
            });

            const { result } = setup();

            let downloadResults;
            await act(async () => {
                downloadResults = await result.current.refetch({
                    scrollId: 'start:100',
                    query: '*',
                    count: 100,
                });
            });

            expect(downloadResults?.nextScrollId).toBe('start:200');
        });

        it('should handle exact match at end of results', async () => {
            mockFetchAssets.mockResolvedValueOnce({
                data: {
                    listDataProductAssets: {
                        total: 200,
                        searchResults: new Array(100).fill(null).map((_, i) => ({
                            entity: { urn: `urn:li:dataset:${i + 100}`, type: EntityType.Dataset },
                        })),
                        facets: [],
                    },
                },
            });

            const { result } = setup();

            let downloadResults;
            await act(async () => {
                downloadResults = await result.current.refetch({
                    scrollId: 'start:100',
                    query: '*',
                    count: 100,
                });
            });

            expect(downloadResults?.nextScrollId).toBeNull();
        });
    });
});
