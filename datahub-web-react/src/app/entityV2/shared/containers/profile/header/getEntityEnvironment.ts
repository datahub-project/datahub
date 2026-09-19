import { GenericEntityProperties } from '@app/entity/shared/types';

import { Container, Dataset, Entity, EntityType, FabricType } from '@types';

/**
 * Resolves an entity's environment (fabric) from data already present in the
 * search/entity response — datasets via `origin` (URN-derived), containers via
 * `properties.env`. Returns null for entity types that don't model environment
 * or when it's unset (best-effort on containers).
 *
 * Also accepts GenericEntityProperties: it is built by spreading the raw entity
 * (see getDataForEntityType), so `type`, `origin` and `properties.env` are present
 * on it at runtime even where its declared type omits them.
 */
export function getEntityEnvironment(entity?: Entity | GenericEntityProperties | null): FabricType | null {
    if (!entity) return null;
    switch (entity.type) {
        case EntityType.Dataset:
            return (entity as Dataset).origin ?? null;
        case EntityType.Container:
            return (entity as Container).properties?.env ?? null;
        default:
            return null;
    }
}
