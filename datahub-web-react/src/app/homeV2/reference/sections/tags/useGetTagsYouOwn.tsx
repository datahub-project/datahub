/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { OWNERS_FILTER_NAME } from '@app/searchV2/utils/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { CorpUser, EntityType } from '@types';

const MAX_ASSETS_TO_FETCH = 50;

// TODO: Add Group Ownership here as well.
export const useGetTagsYouOwn = (user?: CorpUser | null, count = MAX_ASSETS_TO_FETCH) => {
    const { loading, data, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                start: 0,
                count,
                types: [EntityType.Tag],
                filters: [
                    {
                        field: OWNERS_FILTER_NAME,
                        value: user?.urn,
                        values: [user?.urn as string],
                    },
                ],
            },
        },
        skip: !user?.urn,
        fetchPolicy: 'cache-first',
    });

    const entityRegistry = useEntityRegistry();
    const entities =
        data?.searchAcrossEntities?.searchResults?.map((result) =>
            entityRegistry.getGenericEntityProperties(result.entity.type, result.entity),
        ) || [];

    return { entities, loading, error };
};
