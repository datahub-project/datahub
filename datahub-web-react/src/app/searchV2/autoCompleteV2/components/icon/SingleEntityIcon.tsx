import { IconStyleType } from '@src/app/entityV2/Entity';
import { getPlatformName } from '@src/app/entityV2/shared/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity } from '@src/types.generated';
import { Image } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

const ImageIcon = styled(Image)<{ $size: number }>`
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    object-fit: contain;
    background-color: transparent;
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
    const platformName = getPlatformName(properties);

    return (
        (platformLogoUrl && !isBrokenPlatformLogoUrl && (
            <ImageIcon
                preview={false}
                src={platformLogoUrl}
                alt={platformName || ''}
                $size={size}
                onError={() => setIsBrokenPlatformLogoUrl(true)}
            />
        )) ||
        entityRegistry.getIcon(entity.type, size, IconStyleType.ACCENT)
    );
}
