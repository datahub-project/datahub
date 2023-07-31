import React from 'react';
import styled from 'styled-components/macro';
import { CorpGroup, Entity, EntityType } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';

const StyledIcon = styled.img`
    width: 28px;
    height: 28px;
`;

export const getGroupName = (corpGroup: CorpGroup) => {
    return corpGroup.properties?.displayName || corpGroup.name || corpGroup.info?.displayName || undefined;
};

export function getEntityLogo(entity: Entity, entityType: EntityType, entityRegistry: EntityRegistry) {
    const genericProps = entityRegistry.getGenericEntityProperties(entityType, entity);
    const platform = genericProps?.platform;
    const logoUrl = platform?.properties?.logoUrl || '';
    const label = entityRegistry.getDisplayName(EntityType.DataPlatform, platform);
    const icon: JSX.Element | null = <StyledIcon alt="icon" src={logoUrl} />;

    return { label, icon };
}
