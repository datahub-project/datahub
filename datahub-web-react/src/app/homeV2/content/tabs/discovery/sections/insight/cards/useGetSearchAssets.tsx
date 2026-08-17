import { FilterSet } from '@app/entityV2/shared/components/styled/search/types';
import { collapseSiblingEntities } from '@app/homeV2/content/tabs/discovery/sections/insight/cards/useGetSearchAssets.utils';
import { UnionType } from '@app/searchV2/utils/constants';
import { useIsShowSeparateSiblingsEnabled } from '@src/app/useAppConfig';

import { useGetSearchResultsForMultipleCardsQuery } from '@graphql/search.generated';
import { Entity, EntityType, SortCriterion } from '@types';

/** Number of assets shown in a compact insight card row. */
export const INSIGHT_CARD_DISPLAY_COUNT = 5;

/**
 * Fetch extra results when collapsing sibling cohorts client-side so we still fill the card
 * after deduplicating pairs (e.g. dbt model + warehouse table).
 */
export const INSIGHT_CARD_FETCH_COUNT = INSIGHT_CARD_DISPLAY_COUNT * 2;

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
    const showSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const fetchCount = showSeparateSiblings ? INSIGHT_CARD_DISPLAY_COUNT : INSIGHT_CARD_FETCH_COUNT;

    const { data, loading } = useGetSearchResultsForMultipleCardsQuery({
        variables: {
            input: {
                types: types || [],
                query: query || '*',
                start: 0,
                count: fetchCount,
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

    const entities =
        data?.searchAcrossEntities?.searchResults?.map((result) => result.entity).filter((entity) => !!entity) || [];
    const collapsed = showSeparateSiblings ? entities : collapseSiblingEntities(entities);
    const assets = collapsed.slice(0, INSIGHT_CARD_DISPLAY_COUNT);

    return { assets, loading };
};
