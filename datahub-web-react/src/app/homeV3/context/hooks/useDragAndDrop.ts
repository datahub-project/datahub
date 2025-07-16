import { useCallback } from 'react';
import { DragEndEvent } from '@dnd-kit/core';

import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment } from '@graphql/template.generated';

interface DraggedModuleData {
    module: PageModuleFragment;
    position: ModulePositionInput;
}

export interface DroppableData {
    rowIndex: number;
    moduleIndex?: number; // If undefined, drop at the end of the row
    insertNewRow?: boolean; // If true, create a new row at this position
}

export function useDragAndDrop() {
    const { moveModule } = usePageTemplateContext();

    const handleDragEnd = useCallback(
        async (event: DragEndEvent) => {
            const { active, over } = event;

            if (!over || !active.data.current || !over.data.current) {
                return;
            }

            const draggedData = active.data.current as DraggedModuleData;
            const droppableData = over.data.current as DroppableData;

            // Check if we're dropping in the same position
            if (
                draggedData.position.rowIndex === droppableData.rowIndex &&
                draggedData.position.moduleIndex === droppableData.moduleIndex
            ) {
                return;
            }

            // Create the to position based on the drop data
            const toPosition: ModulePositionInput = {
                rowIndex: droppableData.rowIndex,
                moduleIndex: droppableData.moduleIndex,
                // Set rowSide based on module index
                rowSide: droppableData.moduleIndex === 0 ? 'left' : 'right',
            };

            // Use the moveModule function which handles validation, persistence, and error handling
            moveModule({
                module: draggedData.module,
                fromPosition: draggedData.position,
                toPosition,
                insertNewRow: droppableData.insertNewRow,
            });
        },
        [moveModule],
    );

    return {
        handleDragEnd,
    };
} 