import { useDroppable } from '@dnd-kit/core';
import React, { memo } from 'react';
import styled from 'styled-components';

const NewRowDropZone = styled.div<{ $isOver?: boolean }>`
    transition: all 0.2s ease;

    ${({ $isOver }) => $isOver && `border: 2px solid #CAC3F1;`}
`;

interface Props {
    rowIndex: number;
    insertNewRow?: boolean;
}

function NewRowDropZoneComponent({ rowIndex, insertNewRow = false }: Props) {
    const { isOver, setNodeRef } = useDroppable({
        id: `new-row-drop-zone-${rowIndex}${insertNewRow ? '-insert' : ''}`,
        data: {
            rowIndex,
            moduleIndex: 0, // First position in new row
            insertNewRow, // Flag to indicate this should create a new row at this position
        },
    });

    return <NewRowDropZone ref={setNodeRef} $isOver={isOver} />;
}

export default memo(NewRowDropZoneComponent);
