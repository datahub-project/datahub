import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';

const MAX_MODULES_PER_ROW = 3;

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
        return newRows;
    }
    if (adjustedRowIndex >= newRows.length) {
        // Create new row at the end
        newRows.push({ modules: [module] });
        return newRows;
    }

    const toRow = { ...newRows[adjustedRowIndex] };
    const toModules = [...(toRow.modules || [])];
    const insertIndex = toModuleIndex !== undefined ? toModuleIndex : toModules.length;

    // Row has space, insert the module
    if (toModules.length < MAX_MODULES_PER_ROW) {
        toModules.splice(insertIndex, 0, module);
        toRow.modules = toModules;
        newRows[adjustedRowIndex] = toRow;
        return newRows;
    }
    // Row is full with 3 modules
    // Insert new module at the position
    toModules.splice(insertIndex, 0, module);

    // Remove the last module
    const lastModule = toModules.pop();
    toRow.modules = toModules;
    newRows[adjustedRowIndex] = toRow;

    if (lastModule) {
        // Insert new row after the current row with last module
        newRows.splice(adjustedRowIndex + 1, 0, { modules: [lastModule] });
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

    return null;
}
