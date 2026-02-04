import {
    getEntityTypesPropertyFilter,
    getNotHiddenPropertyFilter,
    getShowInColumnsTablePropertyFilter,
} from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetStructuredPropertiesForColumnsQuery } from '@src/graphql/search.generated';
import { EntityType, StructuredPropertyEntity } from '@src/types.generated';

export const useGetTableColumnProperties = () => {
    const entityRegistry = useEntityRegistry();

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: 50,
        searchFlags: { skipCache: false },
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
    const { data } = useGetStructuredPropertiesForColumnsQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
    });

    const results = data?.searchAcrossEntities?.searchResults || [];
    return results
        .filter((result) => result.entity?.__typename === 'StructuredPropertyEntity')
        .map((result) => ({
            entity: result.entity as StructuredPropertyEntity,
        }));
};
