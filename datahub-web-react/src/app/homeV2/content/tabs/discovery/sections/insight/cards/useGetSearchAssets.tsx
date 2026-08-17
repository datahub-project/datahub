import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { collapseSiblingEntities } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetSearchAssets.utils';
import { UnionType } from '@app/searchV2/utils/constants';
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
    const entities =
        data?.searchAcrossEntities?.searchResults?.map((result) => result.entity).filter((entity) => !!entity) || [];
    const assets = showSeparateSiblings ? entities : collapseSiblingEntities(entities);

    return { assets, loading };
};
