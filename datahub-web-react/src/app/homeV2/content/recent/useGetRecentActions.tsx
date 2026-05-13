import { useHomeRecommendations } from '@app/homeV2/useHomeRecommendations';
import { ASSET_ENTITY_TYPES } from '@app/searchV2/utils/constants';
import {
    RECOMMENDATION_MODULE_ID_RECENTLY_EDITED_ENTITIES,
    RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES,
} from '@src/app/entityV2/shared/constants';

import { Entity, EntityType } from '@types';

const SUPPORTED_ENTITY_TYPES = [
    ...ASSET_ENTITY_TYPES,
    EntityType.Domain,
    EntityType.GlossaryNode,
    EntityType.GlossaryTerm,
];

export const useGetRecentActions = () => {
    const { modules, loading, refetch } = useHomeRecommendations();

    const viewedModule = modules?.find(
        (module) => module.moduleId === RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES,
    );
    const viewed =
        viewedModule?.content
            ?.filter((content) => content.entity && SUPPORTED_ENTITY_TYPES.includes(content.entity.type))
            .map((content) => content.entity) || [];
    const editedModule = modules?.find(
        (module) => module.moduleId === RECOMMENDATION_MODULE_ID_RECENTLY_EDITED_ENTITIES,
    );
    const edited =
        editedModule?.content
            ?.filter((content) => content.entity && SUPPORTED_ENTITY_TYPES.includes(content.entity.type))
            .map((content) => content.entity) || [];

    return { viewed: viewed as Entity[], edited: edited as Entity[], loading, refetch };
};
