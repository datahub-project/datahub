import React from 'react';

import styled from 'styled-components';

import { useEntityFormContext } from '../EntityFormContext';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import useContentTruncation from '../../../../shared/useContentTruncation';

import PlatformContentView from '../../containers/profile/header/PlatformContent/PlatformContentView';
import { IconStyleType } from '../../../Entity';
import { getPlatformName } from '../../utils';
import { getDisplayedEntityType } from '../../containers/profile/header/utils';
import { capitalizeFirstLetterOnly } from '../../../../shared/textUtil';

import { EntityType } from '../../../../../types.generated';

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
