import EntityRegistry from '@app/entityV2/EntityRegistry';
import { GenericEntityProperties } from '@app/entityV2/shared/types';
import { capitalizeFirstLetterOnly } from '@app/sharedV2/textUtil';

import { EntityType } from '@types';

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
