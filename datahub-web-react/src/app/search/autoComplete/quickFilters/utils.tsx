import React from 'react';
import styled from 'styled-components';
import { EntityType, QuickFilter } from '../../../../types.generated';
import { IconStyleType } from '../../../entity/Entity';
import EntityRegistry from '../../../entity/EntityRegistry';

const StyledIcon = styled.img`
    width: 12px;
    height: 12px;
`;

export enum QuickFilterField {
    Platform = 'platform',
    Entity = 'entity',
}

export function getQuickFilterDetails(quickFilter: QuickFilter, entityRegistry: EntityRegistry) {
    let label = '';
    let icon: JSX.Element | null = null;
    if (quickFilter.field === QuickFilterField.Platform) {
        label = entityRegistry.getDisplayName(EntityType.DataPlatform, quickFilter.entity);
        const genericProps = entityRegistry.getGenericEntityProperties(EntityType.DataPlatform, quickFilter.entity);
        const logoUrl = genericProps?.platform?.properties?.logoUrl || '';
        icon = <StyledIcon alt="icon" src={logoUrl} />;
    } else if (quickFilter.field === QuickFilterField.Entity) {
        label = entityRegistry.getCollectionName(quickFilter.value as EntityType);
        icon = entityRegistry.getIcon(quickFilter.value as EntityType, 12, IconStyleType.ACCENT, 'black');
    }

    return { label, icon };
}
