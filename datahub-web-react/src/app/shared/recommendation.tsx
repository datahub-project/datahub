import { addUserFiltersToMultiEntitySearchInput } from '@app/shared/userSearchUtils';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

export const useGetRecommendations = (types: Array<EntityType>) => {
    const input = addUserFiltersToMultiEntitySearchInput(
        {
            types,
            query: '*',
            start: 0,
            count: 10,
        },
        types,
    );

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input,
        },
    });

    const recommendedData: Entity[] =
        data?.searchAcrossEntities?.searchResults?.map((searchResult) => searchResult.entity) || [];
    return { recommendedData, loading };
};
