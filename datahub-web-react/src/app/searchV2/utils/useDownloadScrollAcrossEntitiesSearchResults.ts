import { useGetDownloadScrollResultsQuery } from '../../../graphql/scroll.generated';
import { DownloadSearchResults, DownloadSearchResultsInput, DownloadSearchResultsParams } from './types';

/**
 * Hook for use in downloading a single page of search results via the Scroll API.
 *
 * @param params the param to be fed into the GraphQL query
 */
export function useDownloadScrollAcrossEntitiesSearchResults(params: DownloadSearchResultsParams) {
    const { data, loading, error, refetch } = useGetDownloadScrollResultsQuery({
        ...params,
        variables: {
            input: {
                types: params.variables.input.types,
                orFilters: params.variables.input.orFilters,
                query: params.variables.input.query,
                scrollId: params.variables.input.scrollId,
                count: params.variables.input.count,
                viewUrn: params.variables.input.viewUrn,
                searchFlags: params.variables.input.searchFlags,
            },
        },
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
            refetch({
                input: {
                    types: input.types,
                    orFilters: input.orFilters,
                    query: input.query,
                    scrollId: input.scrollId,
                    count: input.count,
                    viewUrn: input.viewUrn,
                    searchFlags: input.searchFlags,
                },
            }).then(
                (res) =>
                    ((res.data?.scrollAcrossEntities && {
                        ...res.data?.scrollAcrossEntities,
                        nextScrollId: res.data?.scrollAcrossEntities?.nextScrollId,
                    }) as DownloadSearchResults) || undefined,
            ),
    };
}
