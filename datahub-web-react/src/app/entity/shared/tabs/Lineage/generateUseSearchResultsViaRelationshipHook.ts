import { useEffect } from 'react';
import { useSearchAcrossLineageQuery } from '../../../../../graphql/search.generated';
import { LineageDirection } from '../../../../../types.generated';
import { GetSearchResultsParams } from '../../components/styled/search/types';

const filtersExist = (filters, orFilters) => {
    return filters?.length || orFilters?.length;
};

export default function generateUseSearchResultsViaRelationshipHook({
    urn,
    direction,
    startTimeMillis,
    endTimeMillis,
    skipCache,
    setSkipCache,
}: {
    urn: string;
    direction: LineageDirection;
    startTimeMillis?: number;
    endTimeMillis?: number;
    skipCache?: boolean;
    setSkipCache?: (skipCache: boolean) => void;
}) {
    return function useGetSearchResultsViaSearchAcrossLineage(params: GetSearchResultsParams) {
        const {
            variables: {
                input: { types, query, start, count, filters, orFilters },
            },
        } = params;
        const inputFields = {
            urn,
            direction,
            types,
            query,
            start,
            count,
            filters,
            orFilters,
            startTimeMillis: startTimeMillis || undefined,
            endTimeMillis: endTimeMillis || undefined,
        };

        const { data, loading, error, refetch } = useSearchAcrossLineageQuery({
            variables: {
                input: inputFields,
            },
            fetchPolicy: 'cache-first',
            skip: !filtersExist(filters, orFilters), // If you don't include any filters, we shound't return anything :). Might as well skip!
        });

        useEffect(() => {
            if (skipCache) {
                refetch({
                    input: { ...inputFields, searchFlags: { skipCache: true, fulltext: true } },
                });
                setSkipCache?.(false);
            }
        });

        return {
            data: data?.searchAcrossLineage,
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
                        orFilters: refetchOrFilters,
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
                        orFilters: refetchOrFilters,
                    },
                }).then((res) => res.data.searchAcrossLineage);
            },
        };
    };
}
