import React from 'react';
import styled from 'styled-components/macro';
import { Entity, EntityType } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';

const StyledIcon = styled.img`
    width: 28px;
    height: 28px;
`;

export function getEntityNameAndLogo(entity: Entity, entityType: EntityType, entityRegistry: EntityRegistry) {
    const genericProps = entityRegistry.getGenericEntityProperties(entityType, entity);
    const platform = genericProps?.platform;
    const logoUrl = platform?.properties?.logoUrl || '';
    const label = entityRegistry.getDisplayName(EntityType.DataPlatform, platform);
    const icon: JSX.Element | null = <StyledIcon alt="icon" src={logoUrl} />;

    return { label, icon };
}
