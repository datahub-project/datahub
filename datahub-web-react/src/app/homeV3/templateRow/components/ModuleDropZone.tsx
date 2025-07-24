import { useDroppable } from '@dnd-kit/core';
import React, { memo } from 'react';
import styled from 'styled-components';

const DropZone = styled.div<{ $isOver?: boolean; $canDrop?: boolean; $isSmall?: boolean | null }>`
    height: ${(props) => (props.$isSmall ? '64px' : '316px')};
    transition: all 0.2s ease;

    ${({ $isOver, $canDrop }) => {
        if ($isOver && $canDrop) {
            return `
                background-color: rgba(59, 130, 246, 0.1);
                border: 2px solid #CAC3F1;
            `;
        }
        return `
            background-color: transparent;
            border: 1px solid transparent;
        `;
    }}
`;

interface Props {
    rowIndex: number;
    moduleIndex?: number;
    disabled?: boolean;
    isSmall: boolean | null;
}

function ModuleDropZone({ rowIndex, moduleIndex, disabled, isSmall }: Props) {
    const { isOver, setNodeRef } = useDroppable({
        id: `drop-zone-${rowIndex}-${moduleIndex ?? 'end'}`,
        disabled,
        data: {
            rowIndex,
            moduleIndex,
            isSmall,
        },
    });

    return <DropZone ref={setNodeRef} $isOver={isOver} $canDrop={!disabled} $isSmall={isSmall} />;
}

export default memo(ModuleDropZone);
