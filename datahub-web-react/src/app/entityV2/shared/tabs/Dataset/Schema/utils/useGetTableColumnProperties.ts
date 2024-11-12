import { getEntityTypeUrn } from '@src/app/govern/structuredProperties/utils';
import { ENTITY_TYPES_FILTER_NAME } from '@src/app/searchV2/utils/constants';
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
                    {
                        field: ENTITY_TYPES_FILTER_NAME,
                        values: [getEntityTypeUrn(entityRegistry, EntityType.SchemaField)],
                    },
                    {
                        field: 'isHidden',
                        values: ['true'],
                        negated: true,
                    },
                    {
                        field: 'showInColumnsTable',
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

    return data?.searchAcrossEntities?.searchResults;
};
