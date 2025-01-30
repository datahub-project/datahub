import React from 'react';
import styled from 'styled-components/macro';
import { EntityType } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';

export const EntityTypeLabel = styled.div<{ showBorder?: boolean }>`
    font-size: 14px;
    color: ${ANTD_GRAY[8]};
    ${(props) =>
        props.showBorder &&
        `
        border-bottom: 1px solid ${ANTD_GRAY[4]};
        padding-bottom: 2px;
        `}
`;

const SubtypesDescription = styled.span`
    font-size: 12px;
    font-weight: 400;
    margin-left: 8px;
    color: ${ANTD_GRAY[7]};
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
