import { useUserContext } from '@src/app/context/useUserContext';
import { RECOMMENDATION_MODULE_ID_RECENT_SEARCHES } from '@src/app/entityV2/shared/constants';
import { useListRecommendationsQuery } from '@src/graphql/recommendations.generated';
import { ScenarioType } from '@src/types.generated';
import { useMemo } from 'react';

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
