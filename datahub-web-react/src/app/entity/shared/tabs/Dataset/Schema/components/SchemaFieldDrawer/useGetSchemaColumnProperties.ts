import { getEntityTypesPropertyFilter, getNotHiddenPropertyFilter } from '@src/app/govern/structuredProperties/utils';
import { SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME } from '@src/app/search/utils/constants';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, SearchResult } from '@src/types.generated';

export default function useGetSchemaColumnProperties() {
    const entityRegistry = useEntityRegistry();

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: 50,
        searchFlags: { skipCache: true },
        orFilters: [
            {
                and: [
                    getEntityTypesPropertyFilter(entityRegistry, true, EntityType.SchemaField),
                    getNotHiddenPropertyFilter(),
                    {
                        field: SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME,
                        values: ['true'],
                    },
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

    return data?.searchAcrossEntities?.searchResults || ([] as SearchResult[]);
}
