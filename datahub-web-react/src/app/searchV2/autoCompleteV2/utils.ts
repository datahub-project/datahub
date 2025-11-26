import EntityRegistry from '@src/app/entityV2/EntityRegistry';
import { capitalizeFirstLetterOnly } from '@src/app/shared/textUtil';
import { Entity } from '@src/types.generated';

export function getEntityDisplayType(entity: Entity, registry: EntityRegistry) {
    const properties = registry.getGenericEntityProperties(entity.type, entity);

    const subtype = registry.getFirstSubType(properties);
    const entityName = registry.getEntityName(entity.type);

    return subtype ? capitalizeFirstLetterOnly(subtype) : entityName;
}
