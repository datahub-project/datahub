import React from 'react';

import { IconStyleType } from '@app/entity/Entity';
import EntityRegistry from '@app/entity/EntityRegistry';
import { useEntityData } from '@app/entity/shared/EntityContext';
import PlatformContentView from '@app/entity/shared/containers/profile/header/PlatformContent/PlatformContentView';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { getPlatformName } from '@app/entity/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import useContentTruncation from '@app/shared/useContentTruncation';
import { useEntityRegistry } from '@app/useEntityRegistry';

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

function PlatformContentContainer() {
    const { entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const platformName = getPlatformName(entityData);
    const platformLogoUrl = entityData?.platform?.properties?.logoUrl;
    const entityLogoComponent = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const typeIcon = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);
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
