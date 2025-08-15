import { useUserContext } from '@src/app/context/useUserContext';
import { RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES } from '@src/app/entityV2/shared/constants';
import { useListRecommendationsQuery } from '@src/graphql/recommendations.generated';
import { Entity, ScenarioType } from '@src/types.generated';

const LIMIT_OF_RECOMMENDATIONS = 5;

interface Response {
    entities: Entity[];
    loading: boolean;
}

export default function useRecentlyViewedEntities(): Response {
    const { user, loaded } = useUserContext();

    const { data, loading } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: user?.urn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: LIMIT_OF_RECOMMENDATIONS,
            },
        },
        fetchPolicy: 'cache-first',
        skip: !user?.urn,
    });

    const viewedModule = data?.listRecommendations?.modules?.find(
        (module) => module.moduleId === RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES,
    );

    const entities =
        viewedModule?.content
            .map((content) => content.entity)
            .filter((entity): entity is Entity => entity?.type !== undefined) || [];

    return { entities, loading: loading || loaded };
}
