import { useSearchAcrossRelationshipsQuery } from '../../../../../graphql/search.generated';
import { LineageDirection } from '../../../../../types.generated';
import { GetSearchResultsParams } from '../../components/styled/search/types';

export default function generateUseSearchResultsViaRelationshipHook({
    urn,
    direction,
}: {
    urn: string;
    direction: LineageDirection;
}) {
    return function useGetSearchResultsViaSearchAcrossRelationships(params: GetSearchResultsParams) {
        const {
            variables: {
                input: { types, query, start, count, filters },
            },
        } = params;

        const { data, loading, error, refetch } = useSearchAcrossRelationshipsQuery({
            variables: {
                input: {
                    urn,
                    direction,
                    types,
                    query,
                    start,
                    count,
                    filters,
                },
            },
        });

        return {
            data: data?.searchAcrossRelationships,
            loading,
            error,
            refetch: (refetchParams: GetSearchResultsParams['variables']) => {
                const {
                    input: {
                        types: refetchTypes,
                        query: refetchQuery,
                        start: refetchStart,
                        count: refetchCount,
                        filters: refetchFilters,
                    },
                } = refetchParams;
                return refetch({
                    input: {
                        urn,
                        direction,
                        types: refetchTypes,
                        query: refetchQuery,
                        start: refetchStart,
                        count: refetchCount,
                        filters: refetchFilters,
                    },
                }).then((res) => res.data.searchAcrossRelationships);
            },
        };
    };
}
