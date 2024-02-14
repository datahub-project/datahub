import { EntityType } from '../../../types.generated';

export const navigateToEntityProfile = (history, entityRegistry, entity) => {
    history.push(entityRegistry.getEntityUrl(entity.type as EntityType, entity.urn as string));
};
