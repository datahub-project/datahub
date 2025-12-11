/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import {
    getEntityTypesPropertyFilter,
    getNotHiddenPropertyFilter,
    getShowInColumnsTablePropertyFilter,
} from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType } from '@src/types.generated';

export const useGetTableColumnProperties = () => {
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
