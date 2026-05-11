import React from 'react';
import styled from 'styled-components/macro';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export const EntityTypeLabel = styled.div<{ showBorder?: boolean }>`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
    ${(props) =>
        props.showBorder &&
        `
        border-bottom: 1px solid ${props.theme.colors.bgSurface};
        padding-bottom: 2px;
        `}
`;

const SubtypesDescription = styled.span`
    font-size: 12px;
    font-weight: 400;
    margin-left: 8px;
    color: ${(props) => props.theme.colors.textTertiary};
`;

interface Props {
    entityType: EntityType;
}

export default function SectionHeader({ entityType }: Props) {
    const entityRegistry = useEntityRegistry();
    const isDatasetType = entityType === EntityType.Dataset;

    return (
        <EntityTypeLabel showBorder>
            {entityRegistry.getCollectionName(entityType)}
            {isDatasetType && <SubtypesDescription>tables, topics, views, and more</SubtypesDescription>}
        </EntityTypeLabel>
    );
}
