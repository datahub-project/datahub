import {
    RECOMMENDATION_MODULE_ID_RECENTLY_EDITED_ENTITIES,
    RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES,
} from '@src/app/entityV2/shared/constants';
import { useListRecommendationsQuery } from '../../../../graphql/recommendations.generated';
import { CorpUser, Entity, EntityType, ScenarioType } from '../../../../types.generated';
import { ASSET_ENTITY_TYPES } from '../../../searchV2/utils/constants';

const SUPPORTED_ENTITY_TYPES = [
    ...ASSET_ENTITY_TYPES,
    EntityType.Domain,
    EntityType.GlossaryNode,
    EntityType.GlossaryTerm,
];

export const useGetRecentActions = (user?: CorpUser | null) => {
    const { data, loading, error } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: user?.urn as string,
                requestContext: {
                    scenario: ScenarioType.Home,
                },
                limit: 10,
            },
        },
        fetchPolicy: 'cache-first',
        skip: !user?.urn,
    });

    const viewedModule = data?.listRecommendations?.modules?.find(
        (module) => module.moduleId === RECOMMENDATION_MODULE_ID_RECENTLY_VIEWED_ENTITIES,
    );
    const viewed =
        viewedModule?.content
            ?.filter((content) => content.entity && SUPPORTED_ENTITY_TYPES.includes(content.entity.type))
            .map((content) => content.entity) || [];
    const editedModule = data?.listRecommendations?.modules?.find(
        (module) => module.moduleId === RECOMMENDATION_MODULE_ID_RECENTLY_EDITED_ENTITIES,
    );
    const edited =
        editedModule?.content
            ?.filter((content) => content.entity && SUPPORTED_ENTITY_TYPES.includes(content.entity.type))
            .map((content) => content.entity) || [];

    return { viewed: viewed as Entity[], edited: edited as Entity[], loading, error };
};
