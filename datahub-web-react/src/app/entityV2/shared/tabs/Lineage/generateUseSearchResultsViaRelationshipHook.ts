import React, { useEffect } from 'react';
import { useSearchAcrossLineageCountQuery, useSearchAcrossLineageQuery } from '../../../../../graphql/search.generated';
import { LineageDirection } from '../../../../../types.generated';
import { GetSearchResultsParams } from '../../components/styled/search/types';

const filtersExist = (filters, orFilters) => {
    return filters?.length || orFilters?.length;
};

export function generateUseSearchResultsCountViaRelationshipHook({
    urn,
    direction,
    startTimeMillis,
    endTimeMillis,
    skipCache,
    setSkipCache,
    setIsLoading,
}: {
    urn: string;
    direction: LineageDirection;
    startTimeMillis?: number;
    endTimeMillis?: number;
    skipCache?: boolean;
    setSkipCache?: (skipCache: boolean) => void;
    setIsLoading?: React.Dispatch<React.SetStateAction<boolean>>;
}) {
    return function useGetSearchResultsCountViaSearchAcrossLineage({ variables: { input } }: GetSearchResultsParams) {
        const { filters, orFilters } = input;
        const inputFields = {
            ...input,
            urn,
            direction,
            startTimeMillis: startTimeMillis || undefined,
            endTimeMillis: endTimeMillis || undefined,
        };

        const { data, loading, error, refetch } = useSearchAcrossLineageCountQuery({
            variables: {
                input: inputFields,
            },
            fetchPolicy: 'cache-first',
            skip: !filtersExist(filters, orFilters), // If you don't include any filters, we shouldn't return anything :). Might as well skip!
        });

        useEffect(() => {
            if (skipCache) {
                setIsLoading?.(true);
                refetch({
                    input: { ...inputFields, searchFlags: { skipCache: true, fulltext: true } },
                }).finally(() => {
                    setIsLoading?.(false);
                });
                setSkipCache?.(false);
            }
        });

        return { total: data?.searchAcrossLineage?.total, loading, error };
    };
}

export default function generateUseSearchResultsViaRelationshipHook({
    urn,
    direction,
    startTimeMillis,
    endTimeMillis,
    skipCache,
    setSkipCache,
    setIsLoading,
}: {
    urn: string;
    direction: LineageDirection;
    startTimeMillis?: number;
    endTimeMillis?: number;
    skipCache?: boolean;
    setSkipCache?: (skipCache: boolean) => void;
    setIsLoading?: React.Dispatch<React.SetStateAction<boolean>>;
}) {
    return function useGetSearchResultsViaSearchAcrossLineage({ variables: { input } }: GetSearchResultsParams) {
        const { filters, orFilters } = input;
        const inputFields = {
            ...input,
            urn,
            direction,
            startTimeMillis: startTimeMillis || undefined,
            endTimeMillis: endTimeMillis || undefined,
        };

        const { data, loading, error, refetch } = useSearchAcrossLineageQuery({
            variables: { input: inputFields },
            fetchPolicy: 'cache-first',
            skip: !filtersExist(filters, orFilters), // If you don't include any filters, we shouldn't return anything :). Might as well skip!
        });

        useEffect(() => {
            if (skipCache) {
                setIsLoading?.(true);
                refetch({
                    input: { ...inputFields, searchFlags: { skipCache: true, fulltext: true } },
                }).finally(() => {
                    setIsLoading?.(false);
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
