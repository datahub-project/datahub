import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';

/**
 * Helper function to remove a module from rows and clean up empty rows
 */
export function removeModuleFromRows(
    rows: PageTemplateFragment['properties']['rows'],
    fromPosition: ModulePositionInput,
): { updatedRows: typeof rows; wasRowRemoved: boolean } {
    if (!rows || fromPosition.rowIndex === undefined || fromPosition.moduleIndex === undefined) {
        return { updatedRows: rows, wasRowRemoved: false };
    }

    const newRows = [...rows];
    const fromRowIndex = fromPosition.rowIndex;

    if (fromRowIndex >= newRows.length) {
        return { updatedRows: newRows, wasRowRemoved: false };
    }

    const fromRow = { ...newRows[fromRowIndex] };
    const fromModules = [...(fromRow.modules || [])];

    // Remove the module
    fromModules.splice(fromPosition.moduleIndex, 1);
    fromRow.modules = fromModules;
    newRows[fromRowIndex] = fromRow;

    // If the row is now empty, remove it
    if (fromModules.length === 0) {
        newRows.splice(fromRowIndex, 1);
        return { updatedRows: newRows, wasRowRemoved: true };
    }

    return { updatedRows: newRows, wasRowRemoved: false };
}

/**
 * Helper function to calculate adjusted row index after potential row removal
 */
export function calculateAdjustedRowIndex(
    fromPosition: ModulePositionInput,
    toRowIndex: number,
    wasRowRemoved: boolean,
): number {
    if (!wasRowRemoved || fromPosition.rowIndex === undefined || fromPosition.rowIndex >= toRowIndex) {
        return toRowIndex;
    }
    return toRowIndex - 1;
}

/**
 * Helper function to insert module into rows with different strategies
 */
export function insertModuleIntoRows(
    rows: PageTemplateFragment['properties']['rows'],
    module: PageModuleFragment,
    toPosition: ModulePositionInput,
    adjustedRowIndex: number,
    insertNewRow?: boolean,
): typeof rows {
    const newRows = [...(rows || [])];
    const { moduleIndex: toModuleIndex } = toPosition;

    if (insertNewRow) {
        // Insert a new row at the specified position
        const newRow = { modules: [module] };
        newRows.splice(adjustedRowIndex, 0, newRow);
    } else if (adjustedRowIndex >= newRows.length) {
        // Create new row at the end
        newRows.push({ modules: [module] });
    } else {
        // Insert into existing row
        const toRow = { ...newRows[adjustedRowIndex] };
        const toModules = [...(toRow.modules || [])];
        const insertIndex = toModuleIndex !== undefined ? toModuleIndex : toModules.length;

        toModules.splice(insertIndex, 0, module);
        toRow.modules = toModules;
        newRows[adjustedRowIndex] = toRow;
    }

    return newRows;
}

/**
 * Helper function to validate move constraints (3-module limit)
 */
export function validateModuleMoveConstraints(
    template: PageTemplateFragment,
    fromPosition: ModulePositionInput,
    toPosition: ModulePositionInput,
): string | null {
    if (!template.properties?.rows || toPosition.rowIndex === undefined) {
        return null;
    }

    const targetRow = template.properties.rows[toPosition.rowIndex];
    if (!targetRow) {
        return null;
    }

    const currentModuleCount = targetRow.modules?.length || 0;
    const isDraggedFromSameRow = fromPosition.rowIndex === toPosition.rowIndex;

    // Only enforce the 3-module limit when moving between different rows
    if (!isDraggedFromSameRow && currentModuleCount >= 3) {
        return 'Cannot move module: Target row already has maximum number of modules';
    }

    return null;
}
