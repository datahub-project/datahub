import { useMemo } from 'react';

import {
    getEntityTypesPropertyFilter,
    getNotHiddenPropertyFilter,
    getShowInColumnsTablePropertyFilter,
    matchesAllowedPlatforms,
} from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, StructuredPropertyEntity } from '@src/types.generated';

export const useGetTableColumnProperties = (platformUrn?: string | null) => {
    const entityRegistry = useEntityRegistry();

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

    // Memoized so the schema table's columns, and with them every rendered cell, stay the same across renders.
    return useMemo(
        () =>
            results?.filter((result) =>
                matchesAllowedPlatforms(result.entity as StructuredPropertyEntity, platformUrn),
            ),
        [results, platformUrn],
    );
};
