import useRecentlyViewedEntities from '@app/searchV2/searchBarV2/hooks/useRecentlyViewedEntities';
import { ASSET_ENTITY_TYPES } from '@app/searchV2/utils/constants';

import { Entity, EntityType } from '@types';

const SUPPORTED_ENTITY_TYPES = [
    ...ASSET_ENTITY_TYPES,
    EntityType.Domain,
    EntityType.GlossaryNode,
    EntityType.GlossaryTerm,
];

export const useGetRecentActions = () => {
    const { entities, loading, refetch } = useRecentlyViewedEntities();

    const viewed = entities.filter((entity) => SUPPORTED_ENTITY_TYPES.includes(entity.type));

    return { viewed: viewed as Entity[], edited: [] as Entity[], loading, refetch };
};
