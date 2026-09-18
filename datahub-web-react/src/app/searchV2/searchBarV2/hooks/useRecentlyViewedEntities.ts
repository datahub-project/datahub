import { useUserContext } from '@app/context/useUserContext';
import { HOME_RECOMMENDATION_MODULE_LIMIT } from '@app/homeV2/homeRecommendationModules';
import { RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES } from '@src/app/entityV2/shared/constants';
import { Entity } from '@src/types.generated';

import { useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { RecommendationModuleId, ScenarioType } from '@types';

interface Response {
    entities: Entity[];
    loading: boolean;
    refetch: () => Promise<unknown>;
}

export default function useRecentlyViewedEntities(skip?: boolean): Response {
    const { user, localState } = useUserContext();
    const { selectedViewUrn } = localState;
    const userUrn = user?.urn;

    const { data, loading, refetch } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: userUrn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                    modules: [RecommendationModuleId.RecentlyViewedEntities],
                },
                limit: HOME_RECOMMENDATION_MODULE_LIMIT,
                viewUrn: selectedViewUrn,
            },
        },
        fetchPolicy: 'cache-first',
        skip: skip || !userUrn,
    });

    const viewedModule = data?.listRecommendations?.modules?.find(
        (module) => module.moduleId === RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES,
    );

    const entities =
        viewedModule?.content
            ?.map((content) => content.entity)
            .filter((entity): entity is Entity => entity?.type !== undefined) || [];

    return { entities, loading, refetch };
}
