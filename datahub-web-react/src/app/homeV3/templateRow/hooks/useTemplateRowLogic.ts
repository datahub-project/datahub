import { useMemo } from 'react';

import { SMALL_MODULE_TYPES } from '@app/homeV3/modules/constants';
import { ModulePositionInput } from '@app/homeV3/template/types';
import { useDragRowContext } from '@app/homeV3/templateRow/hooks/useDragRowContext';
import { WrappedRow } from '@app/homeV3/templateRow/types';

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

    const isSmallRow = useMemo(
        () => (row.modules.length > 0 ? SMALL_MODULE_TYPES.includes(row.modules[0].properties.type) : null),
        [row.modules],
    );

    return {
        // Row state
        isRowFull,
        currentModuleCount,
        maxModulesPerRow,
        isSmallRow,

        // Drag state
        isDraggingFromSameRow,
        shouldDisableDropZones,

        // Module data
        modulePositions,
    };
}
