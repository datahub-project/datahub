import { Image } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { LOGICAL_PLATFORM_URN } from '@app/shared/constants';
import LogicalPlatformDefaultIcon from '@app/sharedV2/logical/LogicalPlatformDefaultIcon';
import { IconStyleType } from '@src/app/entityV2/Entity';
import { getPlatformNameFromEntityData } from '@src/app/entityV2/shared/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity } from '@src/types.generated';

const ImageIcon = styled(Image)<{ $size: number }>`
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    object-fit: contain;
    background-color: transparent;
`;

const EntityIcon = styled.div`
    display: flex;
`;

interface Props {
    entity: Entity;
    size: number;
}

export function SingleEntityIcon({ entity, size }: Props) {
    const [isBrokenPlatformLogoUrl, setIsBrokenPlatformLogoUrl] = useState<boolean>(false);
    const entityRegistry = useEntityRegistryV2();

    const properties = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const platformLogoUrl = properties?.platform?.properties?.logoUrl;
    const platformName = getPlatformNameFromEntityData(properties);

    if (platformLogoUrl && !isBrokenPlatformLogoUrl) {
        return (
            <ImageIcon
                preview={false}
                src={platformLogoUrl}
                alt={platformName || ''}
                $size={size}
                onError={() => setIsBrokenPlatformLogoUrl(true)}
            />
        );
    }

    if (properties?.platform?.urn === LOGICAL_PLATFORM_URN) {
        return <LogicalPlatformDefaultIcon />;
    }

    return <EntityIcon>{entityRegistry.getIcon(entity.type, size, IconStyleType.ACCENT)}</EntityIcon>;
}
