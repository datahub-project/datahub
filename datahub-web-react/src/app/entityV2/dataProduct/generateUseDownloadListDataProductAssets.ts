import {
    DownloadSearchResults,
    DownloadSearchResultsInput,
    DownloadSearchResultsParams,
} from '@app/search/utils/types';

import { useListDataProductAssetsLazyQuery } from '@graphql/search.generated';

/**
 * Parses a scrollId to extract the start offset.
 * ScrollId format: "start:{offset}"
 */
function parseScrollId(scrollId: string | null | undefined): number {
    if (!scrollId) return 0;
    const match = scrollId.match(/^start:(\d+)$/);
    return match ? parseInt(match[1], 10) : 0;
}

/**
 * Creates a scrollId for the next page based on current position.
 * Returns null if we've reached the end of results.
 */
function createNextScrollId(currentStart: number, count: number, total: number): string | null {
    const nextStart = currentStart + count;
    return nextStart < total ? `start:${nextStart}` : null;
}

/**
 * Generates a download hook for Data Product assets that uses listDataProductAssets
 * with simulated scroll pagination via offset encoding.
 */
export default function generateUseDownloadListDataProductAssets({ urn }: { urn: string }) {
    return (_params: DownloadSearchResultsParams) => {
        const [fetchAssets, { data, loading, error }] = useListDataProductAssetsLazyQuery();

        const refetch = async (
            input: DownloadSearchResultsInput,
        ): Promise<DownloadSearchResults | undefined | null> => {
            const start = parseScrollId(input.scrollId);
            const count = input.count || 100;

            const result = await fetchAssets({
                variables: {
                    urn,
                    input: {
                        types: input.types || [],
                        query: input.query || '*',
                        start,
                        count,
                        orFilters: input.orFilters,
                        viewUrn: input.viewUrn,
                        searchFlags: input.searchFlags,
                    },
                },
            });

            const searchData = result.data?.listDataProductAssets;
            if (!searchData) return null;

            const total = searchData.total || 0;
            const nextScrollId = createNextScrollId(start, searchData.searchResults?.length || 0, total);

            return {
                nextScrollId,
                count: searchData.searchResults?.length || 0,
                total,
                searchResults: searchData.searchResults || [],
                facets: searchData.facets || [],
            };
        };

        return {
            searchResults: data?.listDataProductAssets
                ? ({
                      nextScrollId: null,
                      count: data.listDataProductAssets.searchResults?.length || 0,
                      total: data.listDataProductAssets.total || 0,
                      searchResults: data.listDataProductAssets.searchResults || [],
                      facets: data.listDataProductAssets.facets || [],
                  } as DownloadSearchResults)
                : undefined,
            loading,
            error,
            refetch,
        };
    };
}
