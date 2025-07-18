import { describe, expect, it } from 'vitest';

import {
    calculateAdjustedRowIndex,
    insertModuleIntoRows,
    removeModuleFromRows,
    validateModuleMoveConstraints,
} from '@app/homeV3/context/hooks/utils/moduleOperationsUtils';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Mock data helpers
const createMockModule = (name: string, urn: string): PageModuleFragment => ({
    urn,
    type: EntityType.DatahubPageModule,
    properties: {
        name,
        type: DataHubPageModuleType.OwnedAssets,
        visibility: { scope: PageModuleScope.Personal },
        params: {},
    },
});

const createMockTemplate = (rows: any[]): PageTemplateFragment => ({
    urn: 'urn:li:pageTemplate:test',
    type: EntityType.DatahubPageTemplate,
    properties: {
        rows,
        surface: { surfaceType: PageTemplateSurfaceType.HomePage },
        visibility: { scope: PageTemplateScope.Personal },
    },
});

describe('Module Operations Utility Functions', () => {
    describe('removeModuleFromRows', () => {
        const module1 = createMockModule('Module 1', 'urn:li:module:1');
        const module2 = createMockModule('Module 2', 'urn:li:module:2');
        const module3 = createMockModule('Module 3', 'urn:li:module:3');

        it('should remove module and keep row when other modules exist', () => {
            const rows = [{ modules: [module1, module2, module3] }];
            const position: ModulePositionInput = { rowIndex: 0, moduleIndex: 1 };

            const result = removeModuleFromRows(rows, position);

            expect(result.wasRowRemoved).toBe(false);
            expect(result.updatedRows).toHaveLength(1);
            expect(result.updatedRows[0].modules).toHaveLength(2);
            expect(result.updatedRows[0].modules).toEqual([module1, module3]);
        });

        it('should remove entire row when removing last module', () => {
            const rows = [{ modules: [module1] }, { modules: [module2, module3] }];
            const position: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };

            const result = removeModuleFromRows(rows, position);

            expect(result.wasRowRemoved).toBe(true);
            expect(result.updatedRows).toHaveLength(1);
            expect(result.updatedRows[0].modules).toEqual([module2, module3]);
        });

        it('should handle invalid positions gracefully', () => {
            const rows = [{ modules: [module1] }];
            const position: ModulePositionInput = { rowIndex: 5, moduleIndex: 0 };

            const result = removeModuleFromRows(rows, position);

            expect(result.wasRowRemoved).toBe(false);
            expect(result.updatedRows).toEqual(rows);
        });

        it('should handle undefined positions gracefully', () => {
            const rows = [{ modules: [module1] }];
            const position: ModulePositionInput = { rowIndex: undefined, moduleIndex: 0 };

            const result = removeModuleFromRows(rows, position);

            expect(result.wasRowRemoved).toBe(false);
            expect(result.updatedRows).toEqual(rows);
        });

        it('should handle null rows gracefully', () => {
            const rows = null as any;
            const position: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };

            const result = removeModuleFromRows(rows, position);

            expect(result.wasRowRemoved).toBe(false);
            expect(result.updatedRows).toEqual(rows);
        });
    });

    describe('calculateAdjustedRowIndex', () => {
        it('should not adjust when no row was removed', () => {
            const fromPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };
            const toRowIndex = 2;
            const wasRowRemoved = false;

            const result = calculateAdjustedRowIndex(fromPosition, toRowIndex, wasRowRemoved);

            expect(result).toBe(2);
        });

        it('should adjust when row was removed before target', () => {
            const fromPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };
            const toRowIndex = 2;
            const wasRowRemoved = true;

            const result = calculateAdjustedRowIndex(fromPosition, toRowIndex, wasRowRemoved);

            expect(result).toBe(1); // 2 - 1 = 1
        });

        it('should not adjust when removed row is after target', () => {
            const fromPosition: ModulePositionInput = { rowIndex: 2, moduleIndex: 0 };
            const toRowIndex = 1;
            const wasRowRemoved = true;

            const result = calculateAdjustedRowIndex(fromPosition, toRowIndex, wasRowRemoved);

            expect(result).toBe(1); // No change
        });

        it('should not adjust when removed row is same as target', () => {
            const fromPosition: ModulePositionInput = { rowIndex: 1, moduleIndex: 0 };
            const toRowIndex = 1;
            const wasRowRemoved = true;

            const result = calculateAdjustedRowIndex(fromPosition, toRowIndex, wasRowRemoved);

            expect(result).toBe(1); // No change
        });

        it('should handle undefined fromPosition rowIndex gracefully', () => {
            const fromPosition: ModulePositionInput = { rowIndex: undefined, moduleIndex: 0 };
            const toRowIndex = 2;
            const wasRowRemoved = true;

            const result = calculateAdjustedRowIndex(fromPosition, toRowIndex, wasRowRemoved);

            expect(result).toBe(2); // No change when fromPosition is undefined
        });
    });

    describe('insertModuleIntoRows', () => {
        const module1 = createMockModule('Module 1', 'urn:li:module:1');
        const module2 = createMockModule('Module 2', 'urn:li:module:2');
        const newModule = createMockModule('New Module', 'urn:li:module:new');

        it('should insert new row when insertNewRow is true', () => {
            const rows = [{ modules: [module1] }];
            const position: ModulePositionInput = { rowIndex: 1, moduleIndex: 0 };

            const result = insertModuleIntoRows(rows, newModule, position, 1, true);

            expect(result).toHaveLength(2);
            expect(result[1].modules).toEqual([newModule]);
            expect(result[0].modules).toEqual([module1]); // Original row unchanged
        });

        it('should append new row when target index exceeds rows length', () => {
            const rows = [{ modules: [module1] }];
            const position: ModulePositionInput = { rowIndex: 5, moduleIndex: 0 };

            const result = insertModuleIntoRows(rows, newModule, position, 5, false);

            expect(result).toHaveLength(2);
            expect(result[1].modules).toEqual([newModule]);
        });

        it('should insert into existing row at specific position', () => {
            const rows = [{ modules: [module1, module2] }];
            const position: ModulePositionInput = { rowIndex: 0, moduleIndex: 1 };

            const result = insertModuleIntoRows(rows, newModule, position, 0, false);

            expect(result[0].modules).toHaveLength(3);
            expect(result[0].modules[1]).toEqual(newModule);
            expect(result[0].modules[0]).toEqual(module1);
            expect(result[0].modules[2]).toEqual(module2);
        });

        it('should append to existing row when moduleIndex is undefined', () => {
            const rows = [{ modules: [module1, module2] }];
            const position: ModulePositionInput = { rowIndex: 0, moduleIndex: undefined };

            const result = insertModuleIntoRows(rows, newModule, position, 0, false);

            expect(result[0].modules).toHaveLength(3);
            expect(result[0].modules[2]).toEqual(newModule); // Added at end
        });

        it('should handle empty rows array', () => {
            const rows: any[] = [];
            const position: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };

            const result = insertModuleIntoRows(rows, newModule, position, 0, false);

            expect(result).toHaveLength(1);
            expect(result[0].modules).toEqual([newModule]);
        });

        it('should handle null rows gracefully', () => {
            const rows = null as any;
            const position: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };

            const result = insertModuleIntoRows(rows, newModule, position, 0, false);

            expect(result).toHaveLength(1);
            expect(result[0].modules).toEqual([newModule]);
        });
    });

    describe('validateModuleMoveConstraints', () => {
        const module1 = createMockModule('Module 1', 'urn:li:module:1');
        const module2 = createMockModule('Module 2', 'urn:li:module:2');
        const module3 = createMockModule('Module 3', 'urn:li:module:3');

        it('should allow move when target row has space', () => {
            const template = createMockTemplate([
                { modules: [module1, module2] }, // Only 2 modules
            ]);
            const fromPosition: ModulePositionInput = { rowIndex: 1, moduleIndex: 0 };
            const toPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 2 };

            const result = validateModuleMoveConstraints(template, fromPosition, toPosition);

            expect(result).toBeNull();
        });

        it('should prevent move when target row is full and dragging from different row', () => {
            const template = createMockTemplate([
                { modules: [module1, module2, module3] }, // Full row (3 modules)
            ]);
            const fromPosition: ModulePositionInput = { rowIndex: 1, moduleIndex: 0 };
            const toPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 3 };

            const result = validateModuleMoveConstraints(template, fromPosition, toPosition);

            expect(result).toBe('Cannot move module: Target row already has maximum number of modules');
        });

        it('should allow move within same row even when full', () => {
            const template = createMockTemplate([
                { modules: [module1, module2, module3] }, // Full row
            ]);
            const fromPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };
            const toPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 2 };

            const result = validateModuleMoveConstraints(template, fromPosition, toPosition);

            expect(result).toBeNull();
        });

        it('should handle missing template data gracefully', () => {
            const template = createMockTemplate([]);
            const fromPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };
            const toPosition: ModulePositionInput = { rowIndex: 1, moduleIndex: 0 };

            const result = validateModuleMoveConstraints(template, fromPosition, toPosition);

            expect(result).toBeNull();
        });

        it('should handle undefined toPosition rowIndex gracefully', () => {
            const template = createMockTemplate([{ modules: [module1, module2, module3] }]);
            const fromPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };
            const toPosition: ModulePositionInput = { rowIndex: undefined, moduleIndex: 0 };

            const result = validateModuleMoveConstraints(template, fromPosition, toPosition);

            expect(result).toBeNull();
        });

        it('should handle missing template properties gracefully', () => {
            const template = {
                urn: 'urn:li:pageTemplate:test',
                type: EntityType.DatahubPageTemplate,
                properties: null,
            } as any;
            const fromPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };
            const toPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 0 };

            const result = validateModuleMoveConstraints(template, fromPosition, toPosition);

            expect(result).toBeNull();
        });
    });
});
