import React from 'react';
import { useTranslation } from 'react-i18next';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../../Entity';
import { useEntityData } from '../../../../EntityContext';
import { capitalizeFirstLetterOnly } from '../../../../../../shared/textUtil';
import { getPlatformName } from '../../../../utils';
import PlatformContentView from './PlatformContentView';
import { GenericEntityProperties } from '../../../../types';
import EntityRegistry from '../../../../../EntityRegistry';
import { EntityType } from '../../../../../../../types.generated';
import useContentTruncation from '../../../../../../shared/useContentTruncation';
import { translateDisplayNames } from '../../../../../../../utils/translation/translation';

export function getDisplayedEntityType(
    entityData: GenericEntityProperties | null,
    entityRegistry: EntityRegistry,
    entityType: EntityType,
    t: any,
) {
    const typeName = entityData?.subTypes?.typeNames?.[0];

    return (
        entityData?.entityTypeOverride ||
        capitalizeFirstLetterOnly(translateDisplayNames(t, typeName)) ||
        entityRegistry.getEntityName(entityType) ||
        ''
    );
}

function PlatformContentContainer() {
    const { t } = useTranslation();
    const { entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const platformName = getPlatformName(entityData);
    const platformLogoUrl = entityData?.platform?.properties?.logoUrl;
    const entityLogoComponent = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const typeIcon = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType, t);
    const instanceId = entityData?.dataPlatformInstance?.instanceId;

    const { contentRef, isContentTruncated } = useContentTruncation(entityData);

    return (
        <PlatformContentView
            platformName={platformName}
            platformLogoUrl={platformLogoUrl}
            platformNames={entityData?.siblingPlatforms?.map(
                (platform) => platform.properties?.displayName || capitalizeFirstLetterOnly(platform.name),
            )}
            platformLogoUrls={entityData?.siblingPlatforms?.map((platform) => platform.properties?.logoUrl)}
            entityLogoComponent={entityLogoComponent}
            instanceId={instanceId}
            typeIcon={typeIcon}
            entityType={displayedEntityType}
            parentContainers={entityData?.parentContainers?.containers}
            parentContainersRef={contentRef}
            areContainersTruncated={isContentTruncated}
            parentEntities={entityData?.parentDomains?.domains}
        />
    );
}

export default PlatformContentContainer;
