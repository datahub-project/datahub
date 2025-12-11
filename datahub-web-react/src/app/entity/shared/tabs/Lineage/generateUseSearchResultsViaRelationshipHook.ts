/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect } from 'react';

import { GetSearchResultsParams } from '@app/entity/shared/components/styled/search/types';

import { useSearchAcrossLineageQuery } from '@graphql/search.generated';
import { LineageDirection } from '@types';

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
