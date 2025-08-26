import EntityRegistry from '@app/entity/EntityRegistry';

import { Entity, EntityType } from '@types';

/**
 * Extracts display name from entity with robust fallback logic.
 * Handles cases where entity.type is missing or entityRegistry.getDisplayName fails.
 * Follows DataHub's official entity display name patterns from User.tsx and Group.tsx.
 *
 * @param entity - The entity to extract display name from
 * @param entityRegistry - EntityRegistry instance for standard name extraction
 * @param entityType - Optional explicit entity type (used when entity.type is missing)
 * @returns Display name string, or empty string if entity is null/undefined
 */
export const getEntityDisplayName = (
    entity: Entity | any,
    entityRegistry: EntityRegistry,
    entityType?: EntityType,
): string => {
    if (!entity) return '';

    // Primary: Use entityRegistry if type is available
    const typeToUse = entity.type || entityType;
    if (typeToUse) {
        const registryName = entityRegistry.getDisplayName(typeToUse, entity);
        if (registryName) return registryName;
    }

    // Fallback: Manual extraction using DataHub's standard hierarchy
    return (
        entity.properties?.displayName ||
        entity.info?.displayName ||
        entity.name ||
        entity.properties?.fullName || // User-specific
        entity.username || // User-specific
        entity.urn ||
        ''
    );
};

/**
 * Variant that returns null instead of empty string for compatibility with existing code.
 */
export const getEntityDisplayNameOrNull = (
    entity: Entity | any,
    entityRegistry: EntityRegistry,
    entityType?: EntityType,
): string | null => {
    if (!entity) return null;

    const displayName = getEntityDisplayName(entity, entityRegistry, entityType);
    return displayName || entity.urn || null;
};
