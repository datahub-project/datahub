import EntityRegistry from '@app/entity/EntityRegistry';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';

import { EntityType, StructuredPropertiesEntry } from '@types';

export function getDisplayedEntityType(
    entityData: GenericEntityProperties | null,
    entityRegistry: EntityRegistry,
    entityType: EntityType,
) {
    return (
        entityData?.entityTypeOverride ||
        capitalizeFirstLetterOnly(entityData?.subTypes?.typeNames?.[0]) ||
        entityRegistry.getEntityName(entityType) ||
        ''
    );
}

export function filterForAssetBadge(prop: StructuredPropertiesEntry) {
    return prop.structuredProperty.settings?.showAsAssetBadge && !prop.structuredProperty.settings?.isHidden;
}
