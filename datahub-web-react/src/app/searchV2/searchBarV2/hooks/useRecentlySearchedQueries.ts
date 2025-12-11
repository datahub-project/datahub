/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { useUserContext } from '@src/app/context/useUserContext';
import { RECOMMENDATION_MODULE_ID_RECENT_SEARCHES } from '@src/app/entityV2/shared/constants';
import { useListRecommendationsQuery } from '@src/graphql/recommendations.generated';
import { ScenarioType } from '@src/types.generated';

export default function useRecentlySearchedQueries(skip?: boolean) {
    const { user, loaded } = useUserContext();

    const { data, loading } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: user?.urn as string,
                requestContext: {
                    scenario: ScenarioType.SearchBar,
                },
                limit: 1,
            },
        },
        skip: skip || !user?.urn,
    });

    const recentlySearchedQueries = useMemo(
        () =>
            data?.listRecommendations?.modules?.find(
                (module) => module.moduleId === RECOMMENDATION_MODULE_ID_RECENT_SEARCHES,
            )?.content ?? [],
        [data],
    );

    return { recentlySearchedQueries, loading: loading || loaded };
}
