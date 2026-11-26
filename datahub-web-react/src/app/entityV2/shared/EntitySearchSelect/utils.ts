import EntityRegistry from '@app/entityV2/EntityRegistry';

import { Entity, EntityType } from '@types';

/**
 * Gets the display name for an entity, with special handling for unregistered entity types
 * like CustomOwnershipType that store their name in info.name instead of standard fields.
 *
 * @param entity The entity to get the display name for
 * @param entityRegistry The entity registry for looking up display names
 * @returns The display name for the entity
 */
export const getEntityDisplayName = (entity: Entity, entityRegistry: EntityRegistry): string => {
    // Special handling for unregistered entity types
    if (entity.type === EntityType.CustomOwnershipType && 'info' in entity) {
        const ownershipType = entity as any;
        return ownershipType.info?.name || entity.urn;
    }
    if (entity.type === EntityType.IngestionSource) {
        // IngestionSource stores name in the entity directly
        return (entity as any).name || entity.urn;
    }
    return entityRegistry.getDisplayName(entity.type, entity);
};

/**
 * Gets the display label for an entity type, with special handling for known unregistered types.
 * Falls back to translation keys if available, otherwise uses hardcoded defaults.
 *
 * @param entityType The entity type to get the label for
 * @param t The i18next translation function
 * @returns The display label for the entity type
 */
export const getEntityTypeLabel = (
    entityType: EntityType,
    t: (key: string, defaultValue: string) => string,
): string => {
    if (entityType === EntityType.CustomOwnershipType) {
        return t('customOwnershipType', 'Custom Ownership Type');
    }
    if (entityType === EntityType.IngestionSource) {
        return t('ingestionSource', 'Ingestion Source');
    }
    return entityType;
};
