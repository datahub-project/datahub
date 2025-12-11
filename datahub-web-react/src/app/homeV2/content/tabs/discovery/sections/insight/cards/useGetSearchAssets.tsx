/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { UnionType } from '@app/searchV2/utils/constants';
import { combineSiblingsInSearchResults } from '@src/app/search/utils/combineSiblingsInSearchResults';
import { useIsShowSeparateSiblingsEnabled } from '@src/app/useAppConfig';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, SortCriterion } from '@types';

const buildOrFilters = (filters: FilterSet) => {
    if (filters.unionType === UnionType.AND) {
        return [
            {
                and: filters.filters,
            },
        ];
    }
    return filters.filters.map((filter) => {
        return {
            and: [filter],
        };
    });
};

export const useGetSearchAssets = (
    types?: [EntityType],
    query?: string,
    filters?: FilterSet,
    sort?: SortCriterion,
    viewUrn?: string | null,
): any => {
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: types || [],
                query: query || '*',
                start: 0,
                count: 5,
                orFilters: (filters && buildOrFilters(filters)) || null,
                sortInput:
                    (sort && {
                        sortCriterion: sort,
                    }) ||
                    null,
                viewUrn,
                searchFlags: {
                    skipAggregates: true,
                },
            },
        },
        fetchPolicy: 'cache-first',
    });

    const showSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const searchResults = combineSiblingsInSearchResults(
        showSeparateSiblings,
        data?.searchAcrossEntities?.searchResults,
    );

    const assets = searchResults?.filter((result) => result.entity).map((result) => result.entity) || [];

    return { assets, loading };
};
