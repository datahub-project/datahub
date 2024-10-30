import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { Entity, EntityType } from '../../types.generated';

export const useGetRecommendations = (types: Array<EntityType>) => {
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types,
                query: '*',
                start: 0,
                count: 10,
            },
        },
    });

    const recommendedData: Entity[] =
        data?.searchAcrossEntities?.searchResults?.map((searchResult) => searchResult.entity) || [];
    return { recommendedData, loading };
};
