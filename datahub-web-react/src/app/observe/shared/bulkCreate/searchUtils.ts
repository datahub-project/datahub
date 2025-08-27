import { ApolloQueryResult } from '@apollo/client';

import { MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT } from '@app/observe/shared/bulkCreate/constants';
import { LogicalPredicate } from '@app/tests/builder/steps/definition/builder/types';
import { convertLogicalPredicateToOrFilters } from '@app/tests/builder/steps/definition/builder/utils';

import { GetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Dataset, EntityType, SearchAcrossEntitiesInput, SortOrder } from '@types';

/**
 * Batch size for search requests when bulk creating dataset assertions
 */
export const BATCHED_SEARCH_COUNT = 500;

/**
 * Parameters for batched search with URN sorting
 */
export interface BatchedSearchParams {
    /** The filters to apply to the search */
    filters: LogicalPredicate;
    /** Optional query string, defaults to '*' */
    query?: string;
    /** Optional entity types to search, defaults to all types */
    types?: EntityType[];
}

/**
 * Result of a batched search operation
 */
export interface BatchedSearchResult {
    /** All datasets found across all batches */
    datasets: Dataset[];
    /** Total number of results available */
    total: number;
}

/**
 * Search function type that matches the GraphQL query refetch function signature
 */
export type SearchFunction = (params: {
    input: SearchAcrossEntitiesInput;
}) => Promise<ApolloQueryResult<GetSearchResultsForMultipleQuery>>;

/**
 * Utility function to perform batched search requests with URN sorting.
 *
 * This function automatically handles:
 * - Sorting results by URN in ascending order
 * - Batching requests sequentially to avoid overwhelming the server
 * - Combining results from multiple batches
 * - Filtering to only return Dataset entities
 * - Error handling with immediate failure on any batch error
 *
 * If any batch request fails (e.g., due to timeouts or server errors), the function
 * will immediately throw an error with detailed information about which batch failed.
 * This allows the caller to catch and display appropriate error messages to users.
 *
 * @param searchFunction - The GraphQL search function to use
 * @param params - The search parameters
 * @returns Promise resolving to combined search results
 * @throws Error if initial request fails or any batch requests fail
 */
export async function performBatchedSearchWithUrnSorting(
    searchFunction: SearchFunction,
    params: BatchedSearchParams,
): Promise<BatchedSearchResult> {
    const { filters, query = '*', types } = params;

    // Convert filters to orFilters format
    const orFilters = convertLogicalPredicateToOrFilters(filters);

    // Prepare the base search input with URN sorting
    const baseSearchInput: SearchAcrossEntitiesInput = {
        query,
        orFilters,
        types,
        count: BATCHED_SEARCH_COUNT,
        start: 0,
        sortInput: {
            sortCriteria: [
                {
                    field: '_id',
                    sortOrder: SortOrder.Ascending,
                },
            ],
        },
    };

    // First request to get total count
    let firstResult: ApolloQueryResult<GetSearchResultsForMultipleQuery>;
    try {
        firstResult = await searchFunction({
            input: baseSearchInput,
        });
    } catch (error) {
        throw new Error(
            `Failed to execute initial search request. This may be due to network timeouts or server errors. ` +
                `Error: ${error instanceof Error ? error.message : String(error)}`,
        );
    }

    if (!firstResult?.data?.searchAcrossEntities) {
        throw new Error(
            'Initial search request succeeded but returned no data. ' +
                'This may indicate a server issue or that no datasets match the provided filters.',
        );
    }

    const total = Math.min(firstResult.data.searchAcrossEntities.total, MAX_BULK_CREATE_DATASET_ASSERTIONS_COUNT);

    // Process first batch and initialize allDatasets with it
    const allDatasets: Dataset[] = firstResult.data.searchAcrossEntities.searchResults
        .filter((result) => result.entity.type === EntityType.Dataset && result.entity.__typename === 'Dataset')
        .map((result) => result.entity as Dataset);

    // Calculate remaining batches needed
    const remainingCount = total - BATCHED_SEARCH_COUNT;
    const additionalBatches = Math.ceil(remainingCount / BATCHED_SEARCH_COUNT);

    // Process additional batches sequentially if needed
    if (additionalBatches > 0) {
        const batchIndices = Array.from({ length: additionalBatches }, (_, i) => i + 1);

        // NOTE: we use reduce instead of a for loop for linting purposes
        await batchIndices.reduce(async (previousPromise, i) => {
            await previousPromise; // Wait for previous batch to complete

            const batchInput: SearchAcrossEntitiesInput = {
                ...baseSearchInput,
                start: i * BATCHED_SEARCH_COUNT,
            };

            try {
                const batchResult = await searchFunction({
                    input: batchInput,
                });

                if (!batchResult?.data?.searchAcrossEntities) {
                    throw new Error('Search request succeeded but returned no data. This may indicate a server issue.');
                }

                // Process this batch's results
                const batchDatasets = batchResult.data.searchAcrossEntities.searchResults
                    .filter(
                        (searchResult: any) =>
                            searchResult.entity.type === EntityType.Dataset &&
                            searchResult.entity.__typename === 'Dataset',
                    )
                    .map((searchResult: any) => searchResult.entity as Dataset);

                allDatasets.push(...batchDatasets);
            } catch (error) {
                const batchNumber = i + 1; // +1 because batch 1 was the first request, this is batch i+1
                throw new Error(
                    `Failed to retrieve datasets due to batch request failure. ` +
                        `This may be due to network timeouts or server errors. ` +
                        `Details: Batch ${batchNumber}: ${error instanceof Error ? error.message : String(error)}`,
                );
            }
        }, Promise.resolve());
    }

    return {
        datasets: allDatasets,
        total,
    };
}
