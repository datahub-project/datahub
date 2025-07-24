import { colors } from '@components';
import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import React from 'react';
import styled from 'styled-components';

import EntityItem from '@app/homeV3/module/components/EntityItem';
import DragHandle from '@app/homeV3/modules/assetCollection/dragAndDrop/DragHandle';

import type { Entity } from '@types';

const DraggableWrapper = styled.div<{ $isDragging: boolean; $transform?: string; $transition?: string }>`
    background-color: ${colors.white};
    box-shadow: ${(props) => (props.$isDragging ? '0px 4px 12px 0px rgba(9, 1, 61, 0.12)' : 'none')};
    cursor: ${(props) => (props.$isDragging ? 'grabbing' : 'inherit')};
    z-index: ${(props) => (props.$isDragging ? '999' : 'auto')};
    transform: ${(props) => props.$transform};
    transition: ${(props) => props.$transition};
    position: ${({ $isDragging }) => ($isDragging ? 'relative' : 'static')};
    border-radius: 8px;
`;

type Props = {
    entity: Entity;
    customDetailsRenderer?: (entity: Entity) => React.ReactNode;
};

export default function DraggableEntityItem({ entity, customDetailsRenderer }: Props) {
    const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id: entity.urn });

    const dragHandle = () => {
        return <DragHandle listeners={listeners} isDragging={isDragging} />;
    };

    return (
        <DraggableWrapper
            ref={setNodeRef}
            {...attributes}
            $isDragging={isDragging}
            $transform={CSS.Transform.toString(transform)}
            $transition={transition}
        >
            <EntityItem
                entity={entity}
                customDetailsRenderer={customDetailsRenderer}
                navigateOnlyOnNameClick
                dragIconRenderer={dragHandle}
            />
        </DraggableWrapper>
    );
}
