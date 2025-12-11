/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useDroppable } from '@dnd-kit/core';
import React, { memo } from 'react';
import styled from 'styled-components';

const NewRowDropZone = styled.div<{ $isOver?: boolean }>`
    transition: all 0.2s ease;
    margin: 0 42px;

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

    return <NewRowDropZone ref={setNodeRef} $isOver={isOver} data-testid="new-row-drop-zone" />;
}

export default memo(NewRowDropZoneComponent);
