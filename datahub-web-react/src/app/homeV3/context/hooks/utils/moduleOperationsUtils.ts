import { LARGE_MODULE_TYPES, SMALL_MODULE_TYPES } from '@app/homeV3/modules/constants';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType } from '@types';

const MAX_MODULES_PER_ROW = 3;

/**
 * Helper function to check if a module type is a small module
 */
export function isSmallModule(moduleType: DataHubPageModuleType): boolean {
    return SMALL_MODULE_TYPES.includes(moduleType);
}

/**
 * Helper function to check if a module type is a large module
 */
export function isLargeModule(moduleType: DataHubPageModuleType): boolean {
    return LARGE_MODULE_TYPES.includes(moduleType);
}

/**
 * Helper function to check if there's a size mismatch between two modules
 */
export function hasModuleSizeMismatch(moduleType1: DataHubPageModuleType, moduleType2: DataHubPageModuleType): boolean {
    const isModule1Small = isSmallModule(moduleType1);
    const isModule2Small = isSmallModule(moduleType2);
    return isModule1Small !== isModule2Small;
}

/**
 * Helper function to check if a module would create a size mismatch in a given row
 */
export function wouldCreateSizeMismatch(
    newModuleType: DataHubPageModuleType,
    existingModuleTypes: DataHubPageModuleType[],
): boolean {
    if (existingModuleTypes.length === 0) {
        return false; // No mismatch in empty row
    }

    return existingModuleTypes.some((existingType) => hasModuleSizeMismatch(newModuleType, existingType));
}

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

/**
 * Helper function to detect size mismatch and adjust position to avoid it
 */
export function adjustPositionForSizeMismatch(
    template: PageTemplateFragment | null,
    moduleType: DataHubPageModuleType,
    originalPosition: ModulePositionInput,
): { position: ModulePositionInput; shouldInsertNewRow: boolean } {
    // If no template or no specific row index, return original position
    if (!template || originalPosition.rowIndex === undefined) {
        return { position: originalPosition, shouldInsertNewRow: false };
    }

    const targetRow = template.properties?.rows?.[originalPosition.rowIndex];

    // If target row doesn't exist or is empty, no mismatch possible
    if (!targetRow || !targetRow.modules || targetRow.modules.length === 0) {
        return { position: originalPosition, shouldInsertNewRow: false };
    }

    // Get existing module types in the target row
    const existingModuleTypes = targetRow.modules.map((module) => module.properties.type);

    // Check if adding this module would create a size mismatch
    if (wouldCreateSizeMismatch(moduleType, existingModuleTypes)) {
        console.log(
            `Size mismatch detected when adding ${moduleType} to row ${originalPosition.rowIndex}. Creating new row below and pushing subsequent rows down.`,
        );

        // Create a new row below the target row
        const newPosition = {
            rowIndex: originalPosition.rowIndex + 1,
            rowSide: 'left' as const, // Always start new rows on the left
            moduleIndex: 0,
        };

        return { position: newPosition, shouldInsertNewRow: true };
    }

    // No mismatch, return original position
    return { position: originalPosition, shouldInsertNewRow: false };
}

/**
 * Helper function to handle module addition with size mismatch detection
 * Encapsulates the conditional logic for creating new rows vs normal insertion
 */
export function handleModuleAdditionWithSizeMismatch(
    template: PageTemplateFragment,
    module: PageModuleFragment,
    position: ModulePositionInput,
    updateTemplateWithModule: (
        templateToUpdate: PageTemplateFragment | null,
        module: PageModuleFragment,
        position: ModulePositionInput,
        isEditing: boolean,
    ) => PageTemplateFragment | null,
    isEditingModule = false,
): PageTemplateFragment | null {
    // Check for size mismatch and adjust position if needed
    const { position: adjustedPosition, shouldInsertNewRow } = adjustPositionForSizeMismatch(
        template,
        module.properties.type,
        position,
    );

    if (shouldInsertNewRow) {
        // Handle size mismatch by inserting a new row
        const newRows = insertModuleIntoRows(
            template.properties?.rows || [],
            module,
            adjustedPosition,
            adjustedPosition.rowIndex!,
            true, // insertNewRow = true
        );

        return {
            ...template,
            properties: {
                ...template.properties,
                rows: newRows,
            },
        };
    }
    // Normal flow without size mismatch
    return updateTemplateWithModule(template, module, adjustedPosition, isEditingModule);
}

/**
 * Helper function to handle references to removed modules.
 * Removes not existing modules from row and cleans up empty rows.
 */
export function filterOutNonExistentModulesFromTemplate(
    template: PageTemplateFragment | undefined | null,
): PageTemplateFragment | undefined | null {
    if (!template) return template;

    const updatedRows = template.properties.rows
        .map((row) => ({
            ...row,
            modules: row.modules.filter((module) => module.exists),
        }))
        .filter((row) => row.modules.length > 0);

    return { ...template, ...{ properties: { ...template.properties, rows: updatedRows } } };
}
