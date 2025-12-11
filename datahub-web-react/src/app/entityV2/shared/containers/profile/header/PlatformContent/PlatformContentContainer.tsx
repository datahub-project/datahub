/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { IconStyleType } from '@app/entityV2/Entity';
import ContainerIcon from '@app/entityV2/shared/containers/profile/header/PlatformContent/ContainerIcon';
import PlatformContentView from '@app/entityV2/shared/containers/profile/header/PlatformContent/PlatformContentView';
import { getDisplayedEntityType } from '@app/entityV2/shared/containers/profile/header/utils';
import { getPlatformNameFromEntityData } from '@app/entityV2/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import useContentTruncation from '@app/shared/useContentTruncation';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Container, EntityType } from '@types';

function PlatformContentContainer() {
    const { entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const platformName = getPlatformNameFromEntityData(entityData);
    const platformLogoUrl = entityData?.platform?.properties?.logoUrl;
    const entityLogoComponent = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const typeIcon =
        entityType === EntityType.Container ? (
            <ContainerIcon container={entityData as Container} />
        ) : (
            entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT)
        );
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
