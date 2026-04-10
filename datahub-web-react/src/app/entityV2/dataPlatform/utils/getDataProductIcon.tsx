import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entityV2/Entity';
import { EntityRegistry } from '@src/entityRegistryContext';

import { DataPlatform, Entity, EntityType } from '@types';

export const PlatformIcon = styled.img<{ size?: number }>`
    max-height: ${(props) => (props.size ? props.size : 12)}px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

export function getDataProductIcon(entity: Entity, entityRegistry: EntityRegistry, size?: number) {
    let icon: React.ReactNode = null;
    const logoUrl = (entity as DataPlatform)?.properties?.logoUrl;
    icon = logoUrl ? (
        <PlatformIcon src={logoUrl} size={size} />
    ) : (
        entityRegistry.getIcon(EntityType.DataPlatform, size || 12, IconStyleType.ACCENT)
    );

    return icon;
}
