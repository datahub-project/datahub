import { Icon } from '@components';
import { SyntheticListenerMap } from '@dnd-kit/core/dist/hooks/utilities';
import React from 'react';
import styled from 'styled-components';

const DragIcon = styled(Icon)<{ $isDragging?: boolean }>`
    cursor: ${(props) => (props.$isDragging ? 'grabbing' : 'grab')};
`;

type Props = {
    isDragging?: boolean;
    listeners?: SyntheticListenerMap;
};

export default function DragHandle({ isDragging, listeners }: Props) {
    return (
        <DragIcon
            {...listeners}
            size="lg"
            color="gray"
            icon="DotsSixVertical"
            source="phosphor"
            $isDragging={isDragging}
        />
    );
}
