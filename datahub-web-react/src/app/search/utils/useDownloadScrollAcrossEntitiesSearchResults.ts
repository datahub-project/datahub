import {
    DownloadSearchResults,
    DownloadSearchResultsInput,
    DownloadSearchResultsParams,
} from '@app/search/utils/types';

import { useGetDownloadScrollResultsQuery } from '@graphql/scroll.generated';

/**
 * Hook for use in downloading a single page of search results via the Scroll API.
 *
 * @param params the param to be fed into the GraphQL query
 */
export function useDownloadScrollAcrossEntitiesSearchResults(params: DownloadSearchResultsParams) {
    const { data, loading, error, refetch } = useGetDownloadScrollResultsQuery({
        ...params,
        variables: { input: params.variables.input },
    });
    return {
        searchResults:
            ((data?.scrollAcrossEntities && {
                ...data?.scrollAcrossEntities,
                nextScrollId: data?.scrollAcrossEntities?.nextScrollId,
            }) as DownloadSearchResults) || undefined,
        loading,
        error,
        refetch: (input: DownloadSearchResultsInput) =>
            refetch({ input }).then(
                (res) =>
                    ((res.data?.scrollAcrossEntities && {
                        ...res.data?.scrollAcrossEntities,
                        nextScrollId: res.data?.scrollAcrossEntities?.nextScrollId,
                    }) as DownloadSearchResults) || undefined,
            ),
    };
}
