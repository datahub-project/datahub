import React, { useEffect, useRef, useState } from 'react';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../../Entity';
import { useEntityData } from '../../../../EntityContext';
import { capitalizeFirstLetterOnly } from '../../../../../../shared/textUtil';
import { getPlatformName } from '../../../../utils';
import PlatformContentView from './PlatformContentView';

export function useParentContainersTruncation(dataDependency: any) {
    const parentContainersRef = useRef<HTMLDivElement>(null);
    const [areContainersTruncated, setAreContainersTruncated] = useState(false);

    useEffect(() => {
        if (
            parentContainersRef &&
            parentContainersRef.current &&
            parentContainersRef.current.scrollWidth > parentContainersRef.current.clientWidth
        ) {
            setAreContainersTruncated(true);
        }
    }, [dataDependency]);

    return { parentContainersRef, areContainersTruncated };
}

function PlatformContentContainer() {
    const { entityType, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();

    const basePlatformName = getPlatformName(entityData);
    const platformName = capitalizeFirstLetterOnly(basePlatformName);

    const platformLogoUrl = entityData?.platform?.properties?.logoUrl;
    const entityLogoComponent = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const typeIcon = entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT);
    const entityTypeCased =
        (entityData?.subTypes?.typeNames?.length && capitalizeFirstLetterOnly(entityData?.subTypes.typeNames[0])) ||
        entityRegistry.getEntityName(entityType);
    const displayedEntityType = entityData?.entityTypeOverride || entityTypeCased;
    const instanceId = entityData?.dataPlatformInstance?.instanceId;

    const { parentContainersRef, areContainersTruncated } = useParentContainersTruncation(entityData);

    return (
        <PlatformContentView
            platformName={platformName}
            platformLogoUrl={platformLogoUrl}
            platformNames={entityData?.siblingPlatforms?.map(
                (platform) => platform.properties?.displayName || platform.name,
            )}
            platformLogoUrls={entityData?.siblingPlatforms?.map((platform) => platform.properties?.logoUrl)}
            entityLogoComponent={entityLogoComponent}
            instanceId={instanceId}
            typeIcon={typeIcon}
            entityType={displayedEntityType}
            parentContainers={entityData?.parentContainers?.containers}
            parentContainersRef={parentContainersRef}
            areContainersTruncated={areContainersTruncated}
        />
    );
}

export default PlatformContentContainer;
