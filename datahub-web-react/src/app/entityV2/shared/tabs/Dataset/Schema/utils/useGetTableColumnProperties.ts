import {
    getEntityTypesPropertyFilter,
    getNotHiddenPropertyFilter,
    getShowInColumnsTablePropertyFilter,
    matchesAllowedPlatforms,
} from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, StructuredPropertyEntity } from '@src/types.generated';

export const useGetTableColumnProperties = (platformUrn?: string | null) => {
    const entityRegistry = useEntityRegistryV2();

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: 100,
        searchFlags: { skipCache: true },
        orFilters: [
            {
                and: [
                    getEntityTypesPropertyFilter(entityRegistry, true),
                    getNotHiddenPropertyFilter(),
                    getShowInColumnsTablePropertyFilter(),
                ],
            },
        ],
    };

    // Execute search
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
    });

    const results = data?.searchAcrossEntities?.searchResults;
    if (!results) return results;

    return results.filter((result) => matchesAllowedPlatforms(result.entity as StructuredPropertyEntity, platformUrn));
};
