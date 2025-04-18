import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entity/Entity';
import PlatformContentView from '@app/entity/shared/containers/profile/header/PlatformContent/PlatformContentView';
import { getDisplayedEntityType } from '@app/entity/shared/containers/profile/header/utils';
import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { getPlatformName } from '@app/entity/shared/utils';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import useContentTruncation from '@app/shared/useContentTruncation';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

const EntityName = styled.div`
    font-size: 16px;
    font-weight: 700;
    line-height: 24px;
    margin-bottom: 8px;
`;

export default function BulkVerifyEntityInfo() {
    const {
        entity: { entityData },
    } = useEntityFormContext();
    const { contentRef, isContentTruncated } = useContentTruncation(entityData);
    const entityRegistry = useEntityRegistry();

    // entity data may take a second to load before we're ready to display this component
    if (!entityData) return null;

    const entityType = entityData?.type as EntityType;

    const platformName = getPlatformName(entityData);
    const platformLogoUrl = entityData?.platform?.properties?.logoUrl;
    const entityLogoComponent = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const typeIcon = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);
    const instanceId = entityData?.dataPlatformInstance?.instanceId;

    return (
        <div>
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
            <EntityName>{entityRegistry.getDisplayName(entityType, entityData)}</EntityName>
        </div>
    );
}
