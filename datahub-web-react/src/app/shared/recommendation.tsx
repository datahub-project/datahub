/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { addUserFiltersToMultiEntitySearchInput } from '@app/shared/userSearchUtils';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Entity, EntityType } from '@types';

export const useGetRecommendations = (types: Array<EntityType>) => {
    const input = addUserFiltersToMultiEntitySearchInput(
        {
            types,
            query: '*',
            start: 0,
            count: 10,
        },
        types,
    );

    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input,
        },
    });

    const recommendedData: Entity[] =
        data?.searchAcrossEntities?.searchResults?.map((searchResult) => searchResult.entity) || [];
    return { recommendedData, loading };
};
