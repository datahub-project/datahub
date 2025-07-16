import { useMemo } from 'react';

import { ModulePositionInput } from '@app/homeV3/template/types';
import { WrappedRow } from '@app/homeV3/templateRow/types';

import { useDragRowContext } from './useDragRowContext';

interface ModulePosition {
    module: WrappedRow['modules'][0];
    position: ModulePositionInput;
    key: string;
}

export function useTemplateRowLogic(row: WrappedRow, rowIndex: number) {
    const maxModulesPerRow = 3;
    const currentModuleCount = row.modules.length;
    const isRowFull = currentModuleCount >= maxModulesPerRow;
    
    // Check if we're dragging from this same row
    const isDraggingFromSameRow = useDragRowContext(rowIndex);

    // Calculate whether drop zones should be disabled
    const shouldDisableDropZones = useMemo(
        () => isRowFull && !isDraggingFromSameRow,
        [isRowFull, isDraggingFromSameRow],
    );

    // Generate module positions with all necessary data
    const modulePositions = useMemo(
        (): ModulePosition[] =>
            row.modules.map((module, moduleIndex) => ({
                module,
                position: {
                    rowIndex,
                    rowSide: moduleIndex === 0 ? 'left' : 'right',
                    moduleIndex,
                } as ModulePositionInput,
                key: `${module.urn}-${moduleIndex}`,
            })),
        [row.modules, rowIndex],
    );

    return {
        // Row state
        isRowFull,
        currentModuleCount,
        maxModulesPerRow,
        
        // Drag state
        isDraggingFromSameRow,
        shouldDisableDropZones,
        
        // Module data
        modulePositions,
    };
} 