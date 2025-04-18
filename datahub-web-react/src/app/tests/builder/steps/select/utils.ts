import EntityRegistry from '@app/entity/EntityRegistry';

import { EntityType } from '@types';

/**
 * Converts a list of EntityType into their corresponding Metadata Graph names.
 */
export const entityTypesToGraphNames = (entityTypes: EntityType[], registry: EntityRegistry): string[] => {
    return entityTypes.map((entityType) => registry.getGraphNameFromType(entityType));
};

/**
 * Converts a list of entity graph names into their corresponding Entity Types
 */
export const graphNamesToEntityTypes = (names: string[], registry: EntityRegistry): EntityType[] => {
    return names
        .map((name) => registry.getTypeFromGraphName(name))
        .filter((type) => type !== undefined) as EntityType[];
};
