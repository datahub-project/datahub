import { NetworkStatus } from '@apollo/client';

import {
    BATCHED_SEARCH_COUNT,
    BatchedSearchParams,
    SearchFunction,
    performBatchedSearchWithUrnSorting,
} from '@app/observe/shared/bulkCreate/searchUtils';
import { LogicalOperatorType, LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';

import { Dataset, EntityType, SortOrder } from '@types';

describe('searchUtils', () => {
    describe('BATCHED_SEARCH_COUNT', () => {
        it('should be 500', () => {
            expect(BATCHED_SEARCH_COUNT).toBe(500);
        });
    });

    describe('performBatchedSearchWithUrnSorting', () => {
        const mockFilters: LogicalPredicate = {
            type: 'logical',
            operator: LogicalOperatorType.AND,
            operands: [
                {
                    type: 'property',
                    property: '_entityType',
                    operator: 'EQUAL',
                    values: ['dataset'],
                },
            ],
        };

        const mockDataset: Dataset = {
            urn: 'urn:li:dataset:(test,dataset1,PROD)',
            type: EntityType.Dataset,
            __typename: 'Dataset',
            name: 'Test Dataset',
            platform: {
                urn: 'urn:li:dataPlatform:test',
                name: 'Test Platform',
                type: EntityType.DataPlatform,
                __typename: 'DataPlatform',
            },
        } as Dataset;

        const createMockSearchFunction = (scenarios: {
            firstResult?: any;
            batchResults?: any[];
            shouldFailFirst?: boolean;
            shouldFailBatches?: number[];
        }): SearchFunction => {
            let callCount = 0;

            return async ({ input }) => {
                callCount++;

                if (callCount === 1) {
                    if (scenarios.shouldFailFirst) {
                        throw new Error('Network timeout');
                    }
                    return (
                        scenarios.firstResult || {
                            data: {
                                searchAcrossEntities: {
                                    __typename: 'SearchResults' as const,
                                    start: input.start || 0,
                                    count: 1,
                                    total: 10,
                                    searchResults: [
                                        {
                                            __typename: 'SearchResult' as const,
                                            entity: mockDataset,
                                            matchedFields: [],
                                            insights: [],
                                            extraProperties: [],
                                        },
                                    ],
                                    facets: [],
                                    suggestions: [],
                                },
                            },
                            loading: false,
                            networkStatus: NetworkStatus.ready,
                        }
                    );
                }

                // Handle batch requests
                const batchIndex = callCount - 2; // Adjust for 0-based indexing
                if (scenarios.shouldFailBatches?.includes(callCount)) {
                    throw new Error(`Batch ${callCount} timeout`);
                }

                return (
                    scenarios.batchResults?.[batchIndex] || {
                        data: {
                            searchAcrossEntities: {
                                __typename: 'SearchResults' as const,
                                start: input.start || 0,
                                count: 1,
                                total: 10,
                                searchResults: [
                                    {
                                        __typename: 'SearchResult' as const,
                                        entity: {
                                            ...mockDataset,
                                            urn: `urn:li:dataset:(test,dataset${callCount},PROD)`,
                                        },
                                        matchedFields: [],
                                        insights: [],
                                        extraProperties: [],
                                    },
                                ],
                                facets: [],
                                suggestions: [],
                            },
                        },
                        loading: false,
                        networkStatus: NetworkStatus.ready,
                    }
                );
            };
        };

        const defaultParams: BatchedSearchParams = {
            filters: mockFilters,
            query: '*',
            types: [EntityType.Dataset],
        };

        it('should successfully perform single batch search', async () => {
            const searchFunction = createMockSearchFunction({
                firstResult: {
                    data: {
                        searchAcrossEntities: {
                            __typename: 'SearchResults' as const,
                            start: 0,
                            count: 2,
                            total: 5,
                            searchResults: [
                                {
                                    __typename: 'SearchResult' as const,
                                    entity: mockDataset,
                                    matchedFields: [],
                                    insights: [],
                                    extraProperties: [],
                                },
                                {
                                    __typename: 'SearchResult' as const,
                                    entity: { ...mockDataset, urn: 'urn:li:dataset:(test,dataset2,PROD)' },
                                    matchedFields: [],
                                    insights: [],
                                    extraProperties: [],
                                },
                            ],
                            facets: [],
                            suggestions: [],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                },
            });

            const result = await performBatchedSearchWithUrnSorting(searchFunction, defaultParams);

            expect(result.datasets).toHaveLength(2);
            expect(result.total).toBe(5);
            expect(result.datasets[0].urn).toBe('urn:li:dataset:(test,dataset1,PROD)');
        });

        it('should successfully perform multiple batch search', async () => {
            const searchFunction = createMockSearchFunction({
                firstResult: {
                    data: {
                        searchAcrossEntities: {
                            __typename: 'SearchResults' as const,
                            start: 0,
                            count: 500,
                            total: 1500, // More than BATCHED_SEARCH_COUNT (500), requires 3 batches total
                            searchResults: Array.from({ length: 500 }, (_, i) => ({
                                __typename: 'SearchResult' as const,
                                entity: { ...mockDataset, urn: `urn:li:dataset:(test,dataset${i + 1},PROD)` },
                                matchedFields: [],
                                insights: [],
                                extraProperties: [],
                            })),
                            facets: [],
                            suggestions: [],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                },
                batchResults: [
                    {
                        data: {
                            searchAcrossEntities: {
                                __typename: 'SearchResults' as const,
                                start: 500,
                                count: 500,
                                total: 1500,
                                searchResults: Array.from({ length: 500 }, (_, i) => ({
                                    __typename: 'SearchResult' as const,
                                    entity: { ...mockDataset, urn: `urn:li:dataset:(test,dataset${i + 501},PROD)` },
                                    matchedFields: [],
                                    insights: [],
                                    extraProperties: [],
                                })),
                                facets: [],
                                suggestions: [],
                            },
                        },
                        loading: false,
                        networkStatus: NetworkStatus.ready,
                    },
                    {
                        data: {
                            searchAcrossEntities: {
                                __typename: 'SearchResults' as const,
                                start: 1000,
                                count: 500,
                                total: 1500,
                                searchResults: Array.from({ length: 500 }, (_, i) => ({
                                    __typename: 'SearchResult' as const,
                                    entity: { ...mockDataset, urn: `urn:li:dataset:(test,dataset${i + 1001},PROD)` },
                                    matchedFields: [],
                                    insights: [],
                                    extraProperties: [],
                                })),
                                facets: [],
                                suggestions: [],
                            },
                        },
                        loading: false,
                        networkStatus: NetworkStatus.ready,
                    },
                ],
            });

            const result = await performBatchedSearchWithUrnSorting(searchFunction, defaultParams);

            expect(result.datasets).toHaveLength(1500);
            expect(result.total).toBe(1500);
        });

        it('should cap total at MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT (5000)', async () => {
            const searchFunction = createMockSearchFunction({
                firstResult: {
                    data: {
                        searchAcrossEntities: {
                            __typename: 'SearchResults' as const,
                            start: 0,
                            count: 500,
                            total: 10000, // More than MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT (5000)
                            searchResults: Array.from({ length: 500 }, (_, i) => ({
                                __typename: 'SearchResult' as const,
                                entity: { ...mockDataset, urn: `urn:li:dataset:(test,dataset${i + 1},PROD)` },
                                matchedFields: [],
                                insights: [],
                                extraProperties: [],
                            })),
                            facets: [],
                            suggestions: [],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                },
                batchResults: Array.from({ length: 9 }, (_, batchIndex) => ({
                    data: {
                        searchAcrossEntities: {
                            __typename: 'SearchResults' as const,
                            start: (batchIndex + 1) * 500,
                            count: 500,
                            total: 10000,
                            searchResults: Array.from({ length: 500 }, (__, i) => ({
                                __typename: 'SearchResult' as const,
                                entity: {
                                    ...mockDataset,
                                    urn: `urn:li:dataset:(test,dataset${(batchIndex + 1) * 500 + i + 1},PROD)`,
                                },
                                matchedFields: [],
                                insights: [],
                                extraProperties: [],
                            })),
                            facets: [],
                            suggestions: [],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                })),
            });

            const result = await performBatchedSearchWithUrnSorting(searchFunction, defaultParams);

            // Should cap at 5000 datasets, not return all 10000
            expect(result.datasets).toHaveLength(5000);
            expect(result.total).toBe(5000); // Should be capped total, not original 10000
        });

        it('should include URN sorting in search input', async () => {
            const searchInputs: any[] = [];
            const searchFunction: SearchFunction = async ({ input }) => {
                searchInputs.push(input);
                return {
                    data: {
                        searchAcrossEntities: {
                            __typename: 'SearchResults' as const,
                            start: input.start || 0,
                            count: 1,
                            total: 5,
                            searchResults: [
                                {
                                    __typename: 'SearchResult' as const,
                                    entity: mockDataset,
                                    matchedFields: [],
                                    insights: [],
                                    extraProperties: [],
                                },
                            ],
                            facets: [],
                            suggestions: [],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                };
            };

            await performBatchedSearchWithUrnSorting(searchFunction, defaultParams);

            expect(searchInputs[0].sortInput).toEqual({
                sortCriteria: [
                    {
                        field: '_id',
                        sortOrder: SortOrder.Ascending,
                    },
                ],
            });
        });

        it('should filter out non-Dataset entities', async () => {
            const searchFunction = createMockSearchFunction({
                firstResult: {
                    data: {
                        searchAcrossEntities: {
                            total: 10,
                            searchResults: [
                                { entity: mockDataset },
                                { entity: { ...mockDataset, type: EntityType.Chart, __typename: 'Chart' } },
                                { entity: { ...mockDataset, __typename: 'Container' } },
                                { entity: { ...mockDataset, urn: 'urn:li:dataset:(test,dataset2,PROD)' } },
                            ],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                },
            });

            const result = await performBatchedSearchWithUrnSorting(searchFunction, defaultParams);

            expect(result.datasets).toHaveLength(2);
            expect(result.datasets.every((d) => d.type === EntityType.Dataset && d.__typename === 'Dataset')).toBe(
                true,
            );
        });

        it('should throw error when initial request fails', async () => {
            const searchFunction = createMockSearchFunction({
                shouldFailFirst: true,
            });

            await expect(performBatchedSearchWithUrnSorting(searchFunction, defaultParams)).rejects.toThrow(
                'Failed to execute initial search request. This may be due to network timeouts or server errors. Error: Network timeout',
            );
        });

        it('should throw error when initial request returns no data', async () => {
            const searchFunction = createMockSearchFunction({
                firstResult: {
                    data: {
                        searchAcrossEntities: null,
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                },
            });

            await expect(performBatchedSearchWithUrnSorting(searchFunction, defaultParams)).rejects.toThrow(
                'Initial search request succeeded but returned no data. This may indicate a server issue or that no datasets match the provided filters.',
            );
        });

        it('should throw error when batch requests fail', async () => {
            const searchFunction = createMockSearchFunction({
                firstResult: {
                    data: {
                        searchAcrossEntities: {
                            total: 2500, // Requires multiple batches
                            searchResults: [
                                {
                                    __typename: 'SearchResult' as const,
                                    entity: mockDataset,
                                    matchedFields: [],
                                    insights: [],
                                    extraProperties: [],
                                },
                            ],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                },
                shouldFailBatches: [2, 3], // Second and third calls (batch 1 and 2) will fail
            });

            await expect(performBatchedSearchWithUrnSorting(searchFunction, defaultParams)).rejects.toThrow(
                'Failed to retrieve datasets due to batch request failure. This may be due to network timeouts or server errors. Details: Batch 2: Batch 2 timeout',
            );
        });

        it('should throw error when batch request succeeds but returns no data', async () => {
            const searchFunction = createMockSearchFunction({
                firstResult: {
                    data: {
                        searchAcrossEntities: {
                            total: 1500, // Requires one additional batch
                            searchResults: [
                                {
                                    __typename: 'SearchResult' as const,
                                    entity: mockDataset,
                                    matchedFields: [],
                                    insights: [],
                                    extraProperties: [],
                                },
                            ],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                },
                batchResults: [
                    {
                        data: {
                            searchAcrossEntities: null, // No data in batch
                        },
                        loading: false,
                        networkStatus: NetworkStatus.ready,
                    },
                ],
            });

            await expect(performBatchedSearchWithUrnSorting(searchFunction, defaultParams)).rejects.toThrow(
                'Search request succeeded but returned no data. This may indicate a server issue.',
            );
        });

        it('should handle non-Error exceptions in initial request', async () => {
            const searchFunction: SearchFunction = async () => {
                throw new Error('String error');
            };

            await expect(performBatchedSearchWithUrnSorting(searchFunction, defaultParams)).rejects.toThrow(
                'Failed to execute initial search request. This may be due to network timeouts or server errors. Error: String error',
            );
        });

        it('should handle non-Error exceptions in batch requests', async () => {
            const searchFunction = createMockSearchFunction({
                firstResult: {
                    data: {
                        searchAcrossEntities: {
                            total: 1500,
                            searchResults: [
                                {
                                    __typename: 'SearchResult' as const,
                                    entity: mockDataset,
                                    matchedFields: [],
                                    insights: [],
                                    extraProperties: [],
                                },
                            ],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                },
            });

            // Override to throw non-Error on second call
            const originalFunction = searchFunction;
            const modifiedFunction: SearchFunction = async (params) => {
                const callCount = (modifiedFunction as any).callCount || 0;
                (modifiedFunction as any).callCount = callCount + 1;

                if (callCount === 1) {
                    throw new Error('Non-error batch failure');
                }
                return originalFunction(params);
            };

            await expect(performBatchedSearchWithUrnSorting(modifiedFunction, defaultParams)).rejects.toThrow(
                'Failed to retrieve datasets due to batch request failure. This may be due to network timeouts or server errors. Details: Batch 2: Non-error batch failure',
            );
        });

        it('should use default query "*" when not provided', async () => {
            const searchInputs: any[] = [];
            const searchFunction: SearchFunction = async ({ input }) => {
                searchInputs.push(input);
                return {
                    data: {
                        searchAcrossEntities: {
                            __typename: 'SearchResults' as const,
                            start: input.start || 0,
                            count: 1,
                            total: 5,
                            searchResults: [
                                {
                                    __typename: 'SearchResult' as const,
                                    entity: mockDataset,
                                    matchedFields: [],
                                    insights: [],
                                    extraProperties: [],
                                },
                            ],
                            facets: [],
                            suggestions: [],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                };
            };

            const paramsWithoutQuery = { ...defaultParams };
            delete (paramsWithoutQuery as any).query;

            await performBatchedSearchWithUrnSorting(searchFunction, paramsWithoutQuery);

            expect(searchInputs[0].query).toBe('*');
        });

        it('should properly calculate batch pagination', async () => {
            const searchInputs: any[] = [];
            const searchFunction: SearchFunction = async ({ input }) => {
                searchInputs.push(input);

                return {
                    data: {
                        searchAcrossEntities: {
                            __typename: 'SearchResults' as const,
                            start: input.start || 0,
                            count: 500,
                            total: 2500,
                            searchResults: Array.from({ length: 500 }, (_, i) => ({
                                __typename: 'SearchResult' as const,
                                entity: { ...mockDataset, urn: `dataset${input.start! + i}` },
                                matchedFields: [],
                                insights: [],
                                extraProperties: [],
                            })),
                            facets: [],
                            suggestions: [],
                        },
                    },
                    loading: false,
                    networkStatus: NetworkStatus.ready,
                };
            };

            await performBatchedSearchWithUrnSorting(searchFunction, defaultParams);

            // Should have 5 calls: initial + 4 batches (500 * 5 = 2500)
            expect(searchInputs).toHaveLength(5);
            expect(searchInputs[0].start).toBe(0);
            expect(searchInputs[1].start).toBe(500);
            expect(searchInputs[2].start).toBe(1000);
            expect(searchInputs[3].start).toBe(1500);
            expect(searchInputs[4].start).toBe(2000);
            expect(searchInputs.every((input) => input.count === BATCHED_SEARCH_COUNT)).toBe(true);
        });
    });
});
