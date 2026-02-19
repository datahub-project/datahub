import {
    getEntityTypesPropertyFilter,
    getNotHiddenPropertyFilter,
    getShowInColumnsTablePropertyFilter,
} from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

export const useGetTableColumnProperties = () => {
    const entityRegistry = useEntityRegistryV2();

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: 50,
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

    return data?.searchAcrossEntities?.searchResults;
};
