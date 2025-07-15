import { spacing } from '@components';
import { DndContext, DragOverlay, closestCenter, useDroppable } from '@dnd-kit/core';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { useDragAndDrop } from '@app/homeV3/context/hooks/useDragAndDrop';
import ModuleModalMapper from '@app/homeV3/moduleModals/ModuleModalMapper';
import useModulesAvailableToAdd from '@app/homeV3/modules/hooks/useModulesAvailableToAdd';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';
import { wrapRows } from '@app/homeV3/templateRow/utils';

import { DataHubPageTemplateRow } from '@types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.md};
`;

// Additional margin to have width of content excluding side buttons
const StyledAddModulesButton = styled(AddModuleButton)<{ $hasRows?: boolean }>`
    ${(props) => props.$hasRows && 'margin: 0 48px;'}
`;

const NewRowDropZone = styled.div<{ $isOver?: boolean }>`
    min-height: 60px;
    border-radius: 8px;
    margin: 0 48px;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: all 0.2s ease;
    border: 2px dashed transparent;

    ${({ $isOver }) =>
        $isOver &&
        `
        background-color: rgba(59, 130, 246, 0.1);
        border-color: #3b82f6;
        min-height: 80px;
    `}
`;

const NewRowDropText = styled.div<{ $isOver?: boolean }>`
    color: #6b7280;
    font-size: 14px;
    text-align: center;
    transition: all 0.2s ease;

    ${({ $isOver }) =>
        $isOver &&
        `
        color: #3b82f6;
        font-weight: 500;
    `}
`;

interface Props {
    className?: string;
}

function NewRowDropZoneComponent({ rowIndex }: { rowIndex: number }) {
    const { isOver, setNodeRef } = useDroppable({
        id: `new-row-drop-zone-${rowIndex}`,
        data: {
            rowIndex,
            moduleIndex: 0, // First position in new row
        },
    });

    return (
        <NewRowDropZone ref={setNodeRef} $isOver={isOver}>
            <NewRowDropText $isOver={isOver}>
                {isOver ? 'Drop here to create a new row' : 'Drop a module here to create a new row'}
            </NewRowDropText>
        </NewRowDropZone>
    );
}

export default function Template({ className }: Props) {
    const { template } = usePageTemplateContext();
    const rows = useMemo(
        () => (template?.properties?.rows ?? []) as DataHubPageTemplateRow[],
        [template?.properties?.rows],
    );
    const hasRows = useMemo(() => !!rows.length, [rows.length]);
    const wrappedRows = useMemo(() => wrapRows(rows), [rows]);

    const modulesAvailableToAdd = useModulesAvailableToAdd();
    const { handleDragEnd } = useDragAndDrop();

    return (
        <Wrapper className={className}>
            <DndContext collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
                {wrappedRows.map((row, i) => {
                    const key = `templateRow-${i}`;
                    return (
                        <TemplateRow key={key} row={row} rowIndex={i} modulesAvailableToAdd={modulesAvailableToAdd} />
                    );
                })}

                {/* Drop zone for creating new rows */}
                <NewRowDropZoneComponent rowIndex={wrappedRows.length} />

                <DragOverlay>
                    {/* Optionally render a preview of the dragged item */}
                    <div style={{ opacity: 0.8 }}>Dragging...</div>
                </DragOverlay>
            </DndContext>
            <StyledAddModulesButton
                orientation="horizontal"
                $hasRows={hasRows}
                modulesAvailableToAdd={modulesAvailableToAdd}
            />
            <ModuleModalMapper />
        </Wrapper>
    );
}
