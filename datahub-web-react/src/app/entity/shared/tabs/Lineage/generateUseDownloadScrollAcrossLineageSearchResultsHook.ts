import { useGetDownloadScrollAcrossLineageResultsQuery } from '../../../../../graphql/scroll.generated';
import { LineageDirection } from '../../../../../types.generated';
import {
    DownloadSearchResults,
    DownloadSearchResultsInput,
    DownloadSearchResultsParams,
} from '../../../../search/utils/types';

/**
 * Generates a hook which can be used to download Scroll Across Lineage Results to CSV inside the
 * Download as CSV modal.
 */
export default function generateUseDownloadSearchAcrossLineageSearchResultsHook({
    urn,
    direction,
    startTimeMillis,
    endTimeMillis,
}: {
    urn: string;
    direction: LineageDirection;
    startTimeMillis?: number;
    endTimeMillis?: number;
    skipCache?: boolean;
    setSkipCache?: (skipCache: boolean) => void;
}) {
    return function useDownloadSearchAcrossLineageSearchResults(params: DownloadSearchResultsParams) {
        const { data, loading, error, refetch } = useGetDownloadScrollAcrossLineageResultsQuery({
            ...params,
            variables: {
                input: {
                    urn,
                    direction,
                    types: params.variables?.input?.types,
                    query: params.variables?.input?.query,
                    scrollId: params.variables?.input?.scrollId,
                    count: params.variables?.input?.count,
                    orFilters: params.variables?.input?.orFilters,
                    startTimeMillis: startTimeMillis || undefined,
                    endTimeMillis: endTimeMillis || undefined,
                },
            },
        });

        return {
            searchResults: (data?.scrollAcrossLineage && {
                ...data?.scrollAcrossLineage,
                nextScrollId: data?.scrollAcrossLineage?.nextScrollId,
                searchResults: data?.scrollAcrossLineage?.searchResults,
            }) as DownloadSearchResults,
            loading,
            error,
            refetch: (input: DownloadSearchResultsInput) => {
                return refetch({
                    input: {
                        urn,
                        direction,
                        types: input.types,
                        query: input.query,
                        scrollId: input.scrollId,
                        count: input.count,
                        orFilters: input.orFilters,
                        startTimeMillis: startTimeMillis || undefined,
                        endTimeMillis: endTimeMillis || undefined,
                    },
                }).then((res) => {
                    return (res.data?.scrollAcrossLineage && {
                        ...res.data?.scrollAcrossLineage,
                        nextScrollId: res.data?.scrollAcrossLineage?.nextScrollId,
                    }) as DownloadSearchResults;
                });
            },
        };
    };
}
