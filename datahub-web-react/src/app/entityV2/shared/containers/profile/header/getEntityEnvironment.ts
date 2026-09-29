import { GenericEntityProperties } from '@app/entity/shared/types';

import { Container, Dataset, Entity, EntityType, FabricType, MlModel, MlModelGroup } from '@types';

/**
 * Resolves an entity's environment (fabric) from data already present in the
 * search/entity response — datasets, ML models and ML model groups via `origin`
 * (URN-derived), containers via `properties.origin`. Returns null for entity types
 * that don't model environment or when it's unset (best-effort on containers).
 *
 * Also accepts GenericEntityProperties: it is built by spreading the raw entity
 * (see getDataForEntityType), so `type`, `origin` and `properties.origin` are present
 * on it at runtime even where its declared type omits them.
 */
export function getEntityEnvironment(entity?: Entity | GenericEntityProperties | null): FabricType | null {
    if (!entity) return null;
    switch (entity.type) {
        case EntityType.Dataset:
        case EntityType.Mlmodel:
        case EntityType.MlmodelGroup:
            return (entity as Dataset | MlModel | MlModelGroup).origin ?? null;
        case EntityType.Container:
            return (entity as Container).properties?.origin ?? null;
        default:
            return null;
    }
}
