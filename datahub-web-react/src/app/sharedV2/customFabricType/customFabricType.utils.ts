import { CustomFabricType, Entity, EntityType } from '@types';

/**
 * The label of an environment value: a custom environment's display name, else its id, else the
 * raw value. Built-in environments (PROD, DEV, ...) have no entity and keep their value.
 */
export function getEnvironmentLabel(value: string, entity?: Partial<Entity> | null): string {
    if (entity?.type !== EntityType.CustomFabricType) {
        return value;
    }
    const customFabricType = entity as Partial<CustomFabricType>;
    return customFabricType.properties?.displayName || customFabricType.id || value;
}
