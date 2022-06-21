import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';

export function useGetRecommendedTags() {
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.Tag],
                query: '*',
                start: 0,
                count: 5,
            },
        },
    });
    return data?.searchAcrossEntities?.searchResults;
}

export function useGetRecommendedOwners() {
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.CorpGroup, EntityType.CorpUser],
                query: '*',
                start: 0,
                count: 5,
            },
        },
    });
    return data?.searchAcrossEntities?.searchResults;
}

export function useGetRecommendedDomains() {
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.Domain],
                query: '*',
                start: 0,
                count: 5,
            },
        },
    });
    return data?.searchAcrossEntities?.searchResults;
}
