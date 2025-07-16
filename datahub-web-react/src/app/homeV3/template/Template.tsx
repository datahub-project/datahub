import { spacing } from '@components';
import { DndContext, DragEndEvent, DragOverlay, DragStartEvent, closestCenter, useDroppable } from '@dnd-kit/core';
import React, { memo, useMemo } from 'react';
import styled from 'styled-components';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { useDragAndDrop } from '@app/homeV3/context/hooks/useDragAndDrop';
import ModuleModalMapper from '@app/homeV3/moduleModals/ModuleModalMapper';
import useModulesAvailableToAdd from '@app/homeV3/modules/hooks/useModulesAvailableToAdd';
import AddModuleButton from '@app/homeV3/template/components/AddModuleButton';
import TemplateRow from '@app/homeV3/templateRow/TemplateRow';
import { wrapRows } from '@app/homeV3/templateRow/utils';
import LargeModule from '@app/homeV3/module/components/LargeModule';

import { PageModuleFragment } from '@graphql/template.generated';
import { ModulePositionInput } from '@app/homeV3/template/types';
import Module from '@app/homeV3/module/Module';

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

// Styled wrapper for drag overlay to make it look like it's floating
const DragOverlayWrapper = styled.div`
    transform: rotate(5deg) scale(1.05);
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
    border-radius: 8px;
    opacity: 0.95;
    /* Performance optimizations */
    will-change: transform;
    pointer-events: none;
`;

interface Props {
    className?: string;
}

// Memoized new row drop zone component
const NewRowDropZoneComponent = memo(function NewRowDropZoneComponent({ rowIndex }: { rowIndex: number }) {
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
});

function Template({ className }: Props) {
    const { template } = usePageTemplateContext();
    const rows = useMemo(
        () => (template?.properties?.rows ?? []) as DataHubPageTemplateRow[],
        [template?.properties?.rows],
    );
    const hasRows = useMemo(() => !!rows.length, [rows.length]);
    const wrappedRows = useMemo(() => wrapRows(rows), [rows]);

    const modulesAvailableToAdd = useModulesAvailableToAdd();
    const { handleDragEnd } = useDragAndDrop();

    // State for drag overlay - store the full module and position
    const [activeModule, setActiveModule] = React.useState<{
        module: PageModuleFragment;
        position: ModulePositionInput;
    } | null>(null);

    const handleDragStart = React.useCallback((event: DragStartEvent) => {
        const draggedData = event.active.data.current as { 
            module?: PageModuleFragment; 
            position?: ModulePositionInput;
        } | undefined;
        
        if (draggedData?.module && draggedData?.position) {
            setActiveModule({
                module: draggedData.module,
                position: draggedData.position,
            });
        }
    }, []);

    const handleDragEndWithCleanup = React.useCallback(
        (event: DragEndEvent) => {
            setActiveModule(null);
            handleDragEnd(event);
        },
        [handleDragEnd],
    );

    // Memoize the template rows to prevent unnecessary re-renders
    const templateRows = useMemo(
        () =>
            wrappedRows.map((row, i) => (
                <TemplateRow
                    key={`templateRow-${i}`}
                    row={row}
                    rowIndex={i}
                    modulesAvailableToAdd={modulesAvailableToAdd}
                />
            )),
        [wrappedRows, modulesAvailableToAdd],
    );

    return (
        <Wrapper className={className}>
            <DndContext
                collisionDetection={closestCenter}
                onDragStart={handleDragStart}
                onDragEnd={handleDragEndWithCleanup}
            >
                {templateRows}

                {/* Drop zone for creating new rows */}
                <NewRowDropZoneComponent rowIndex={wrappedRows.length} />

                <DragOverlay>
                    {activeModule && (
                        <DragOverlayWrapper>
                            <Module 
                                module={activeModule.module}
                                position={activeModule.position}
                            />
                        </DragOverlayWrapper>
                    )}
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

// Export memoized component
export default memo(Template);
