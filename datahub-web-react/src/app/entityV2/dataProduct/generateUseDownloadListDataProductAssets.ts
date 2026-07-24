import {
    DownloadSearchResults,
    DownloadSearchResultsInput,
    DownloadSearchResultsParams,
} from '@app/search/utils/types';

import { useListDataProductAssetsQuery } from '@graphql/search.generated';

const DEFAULT_DOWNLOAD_PAGE_SIZE = 100;
type DataProductSearchResults = Omit<DownloadSearchResults, 'nextScrollId'>;

function getDownloadStart(scrollId: string | null | undefined): number {
    const parsedScrollId = Number.parseInt(scrollId || '0', 10);
    return Number.isNaN(parsedScrollId) ? 0 : parsedScrollId;
}

function toDownloadSearchResults(
    searchResults: DataProductSearchResults | null | undefined,
    start: number,
    requestedCount: number | null | undefined,
): DownloadSearchResults | undefined {
    if (!searchResults) {
        return undefined;
    }

    const count = searchResults.count || requestedCount || DEFAULT_DOWNLOAD_PAGE_SIZE;
    const nextStart = start + count;

    return {
        ...searchResults,
        nextScrollId: nextStart < searchResults.total ? `${nextStart}` : undefined,
    };
}

export default function generateUseDownloadListDataProductAssets({ urn }: { urn: string }) {
    return function useDownloadListDataProductAssets(params: DownloadSearchResultsParams) {
        const { scrollId, ...searchInput } = params.variables.input;
        const start = getDownloadStart(scrollId);
        const { data, loading, error, refetch } = useListDataProductAssetsQuery({
            ...params,
            variables: {
                urn,
                input: {
                    ...searchInput,
                    start,
                },
            },
        });

        return {
            searchResults: toDownloadSearchResults(data?.listDataProductAssets, start, searchInput.count),
            loading,
            error,
            refetch: (input: DownloadSearchResultsInput) => {
                const { scrollId: nextScrollId, ...nextSearchInput } = input;
                const nextStart = getDownloadStart(nextScrollId);

                return refetch({
                    urn,
                    input: {
                        ...nextSearchInput,
                        start: nextStart,
                    },
                }).then((res) =>
                    toDownloadSearchResults(res.data?.listDataProductAssets, nextStart, nextSearchInput.count),
                );
            },
        };
    };
}
