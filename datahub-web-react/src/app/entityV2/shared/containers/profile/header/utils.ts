import { EntityType, StructuredPropertiesEntry } from '../../../../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../../../../shared/textUtil';
import { EntityRegistry } from '../../../../../../entityRegistryContext';
import { GenericEntityProperties } from '../../../../../entity/shared/types';

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

export function getEntityPlatforms(entityType: EntityType | null, entityData: GenericEntityProperties | null) {
    const platform = entityType === EntityType.SchemaField ? entityData?.parent?.platform : entityData?.platform;
    const platforms =
        entityType === EntityType.SchemaField ? entityData?.parent?.siblingPlatforms : entityData?.siblingPlatforms;

    return { platform, platforms };
}

export function filterForAssetBadge(prop: StructuredPropertiesEntry) {
    return prop.structuredProperty.settings?.showAsAssetBadge && !prop.structuredProperty.settings?.isHidden;
}
