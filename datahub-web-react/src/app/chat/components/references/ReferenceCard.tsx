import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { PreviewType } from '@app/entity/Entity';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Entity } from '@types';

const CardWrapper = styled.div<{ $isSelected: boolean }>`
    border: 1px solid ${(props) => (props.$isSelected ? colors.primary[500] : colors.gray[100])};
    border-radius: 8px;
    cursor: pointer;
    transition: all 0.2s ease;
    width: 100%;
    &:hover {
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    }
    overflow: hidden;
    padding: 12px;
`;

interface Props {
    entity: Entity;
    isSelected: boolean;
    onClick: (entity: Entity) => void;
}

export const ReferenceCard: React.FC<Props> = ({ entity, isSelected, onClick }) => {
    const entityRegistry = useEntityRegistryV2();

    const handleClick = (e: React.MouseEvent) => {
        e.stopPropagation();
        onClick(entity);
    };

    return (
        <CardWrapper $isSelected={isSelected} onClick={handleClick} data-reference-card>
            {entityRegistry.renderPreview(entity.type, PreviewType.PREVIEW, entity)}
        </CardWrapper>
    );
};
