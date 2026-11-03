import React from 'react';

import PlatformContentView from '@app/entity/shared/containers/profile/header/PlatformContent/PlatformContentView';
import { getDisplayedEntityType } from '@app/entity/shared/containers/profile/header/utils';
import { IconStyleType } from '@app/entityV2/Entity';
import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { getPlatformName } from '@app/entityV2/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/sharedV2/textUtil';
import useContentTruncation from '@app/sharedV2/useContentTruncation';
import { useEntityRegistry } from '@app/useEntityRegistry';

function PlatformContentContainer() {
    const { entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const platformName = getPlatformName(entityData?.platform);
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
