import { useHomeRecommendations } from '@app/homeV2/useHomeRecommendations';
import { RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES } from '@src/app/entityV2/shared/constants';
import { Entity } from '@src/types.generated';

interface Response {
    entities: Entity[];
    loading: boolean;
}

export default function useRecentlyViewedEntities(): Response {
    const { modules, loading } = useHomeRecommendations();

    const viewedModule = modules?.find(
        (module) => module.moduleId === RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES,
    );

    const entities =
        viewedModule?.content
            .map((content) => content.entity)
            .filter((entity): entity is Entity => entity?.type !== undefined) || [];

    return { entities, loading };
}
