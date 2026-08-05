import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { UnionType } from '@app/searchV2/utils/constants';
import { combineSiblingsInSearchResults } from '@src/app/search/utils/combineSiblingsInSearchResults';
import { useIsShowSeparateSiblingsEnabled } from '@src/app/useAppConfig';

import { useGetSearchResultsForMultipleCardsQuery } from '@graphql/search.generated';
import { Entity, EntityType, SortCriterion } from '@types';

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
): { assets: Entity[]; loading: boolean } => {
    const { data, loading } = useGetSearchResultsForMultipleCardsQuery({
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
    // The card query omits matchedFields, and only the combined entities are used here.
    const searchResults = combineSiblingsInSearchResults(
        showSeparateSiblings,
        data?.searchAcrossEntities?.searchResults?.map((result) => ({ ...result, matchedFields: [] })),
    );

    const assets = searchResults?.filter((result) => result.entity).map((result) => result.entity) || [];

    return { assets, loading };
};
