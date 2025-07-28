import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    adjustPositionForSizeMismatch,
    calculateAdjustedRowIndex,
    filterOutNonExistentModulesFromTemplate,
    handleModuleAdditionWithSizeMismatch,
    hasModuleSizeMismatch,
    insertModuleIntoRows,
    isLargeModule,
    isSmallModule,
    removeModuleFromRows,
    validateModuleMoveConstraints,
    wouldCreateSizeMismatch,
} from '@app/homeV3/context/hooks/utils/moduleOperationsUtils';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Mock data helpers
const createMockModule = (name: string, urn: string, exists = true): PageModuleFragment => ({
    urn,
    type: EntityType.DatahubPageModule,
    exists,
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
        const module4 = createMockModule('Module 4', 'urn:li:module:4');

        it('should allow move when target row has space', () => {
            const template = createMockTemplate([
                { modules: [module1, module2] }, // Only 2 modules
            ]);
            const fromPosition: ModulePositionInput = { rowIndex: 1, moduleIndex: 0 };
            const toPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 2 };

            const result = validateModuleMoveConstraints(template, fromPosition, toPosition);

            expect(result).toBeNull();
        });

        it('should allow move when moving module into a full row', () => {
            const template = createMockTemplate([
                { modules: [module1, module2, module3] }, // Full row (3 modules)
                { modules: [module4] },
            ]);
            const fromPosition: ModulePositionInput = { rowIndex: 1, moduleIndex: 0 };
            const toPosition: ModulePositionInput = { rowIndex: 0, moduleIndex: 3 };

            const result = validateModuleMoveConstraints(template, fromPosition, toPosition);

            expect(result).toBeNull();
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

    describe('Module Size Detection Functions', () => {
        describe('isSmallModule', () => {
            it('should return true for small module types', () => {
                expect(isSmallModule(DataHubPageModuleType.Link)).toBe(true);
            });

            it('should return false for large module types', () => {
                expect(isSmallModule(DataHubPageModuleType.OwnedAssets)).toBe(false);
                expect(isSmallModule(DataHubPageModuleType.Domains)).toBe(false);
                expect(isSmallModule(DataHubPageModuleType.AssetCollection)).toBe(false);
                expect(isSmallModule(DataHubPageModuleType.RichText)).toBe(false);
                expect(isSmallModule(DataHubPageModuleType.Hierarchy)).toBe(false);
            });
        });

        describe('isLargeModule', () => {
            it('should return false for small module types', () => {
                expect(isLargeModule(DataHubPageModuleType.Link)).toBe(false);
            });

            it('should return true for large module types', () => {
                expect(isLargeModule(DataHubPageModuleType.OwnedAssets)).toBe(true);
                expect(isLargeModule(DataHubPageModuleType.Domains)).toBe(true);
                expect(isLargeModule(DataHubPageModuleType.AssetCollection)).toBe(true);
                expect(isLargeModule(DataHubPageModuleType.RichText)).toBe(true);
                expect(isLargeModule(DataHubPageModuleType.Hierarchy)).toBe(true);
            });
        });

        describe('hasModuleSizeMismatch', () => {
            it('should return true when comparing small and large modules', () => {
                expect(hasModuleSizeMismatch(DataHubPageModuleType.Link, DataHubPageModuleType.OwnedAssets)).toBe(true);
                expect(hasModuleSizeMismatch(DataHubPageModuleType.OwnedAssets, DataHubPageModuleType.Link)).toBe(true);
            });

            it('should return false when comparing same size modules', () => {
                expect(hasModuleSizeMismatch(DataHubPageModuleType.Link, DataHubPageModuleType.Link)).toBe(false);
                expect(hasModuleSizeMismatch(DataHubPageModuleType.OwnedAssets, DataHubPageModuleType.Domains)).toBe(
                    false,
                );
                expect(
                    hasModuleSizeMismatch(DataHubPageModuleType.AssetCollection, DataHubPageModuleType.RichText),
                ).toBe(false);
            });
        });

        describe('wouldCreateSizeMismatch', () => {
            it('should return false for empty row', () => {
                expect(wouldCreateSizeMismatch(DataHubPageModuleType.Link, [])).toBe(false);
                expect(wouldCreateSizeMismatch(DataHubPageModuleType.OwnedAssets, [])).toBe(false);
            });

            it('should return true when adding small module to large module row', () => {
                const existingTypes = [DataHubPageModuleType.OwnedAssets];
                expect(wouldCreateSizeMismatch(DataHubPageModuleType.Link, existingTypes)).toBe(true);
            });

            it('should return true when adding large module to small module row', () => {
                const existingTypes = [DataHubPageModuleType.Link];
                expect(wouldCreateSizeMismatch(DataHubPageModuleType.OwnedAssets, existingTypes)).toBe(true);
            });

            it('should return false when adding same size modules', () => {
                const smallTypes = [DataHubPageModuleType.Link];
                expect(wouldCreateSizeMismatch(DataHubPageModuleType.Link, smallTypes)).toBe(false);

                const largeTypes = [DataHubPageModuleType.OwnedAssets, DataHubPageModuleType.Domains];
                expect(wouldCreateSizeMismatch(DataHubPageModuleType.AssetCollection, largeTypes)).toBe(false);
            });

            it('should return true if any existing module causes mismatch', () => {
                const mixedTypes = [DataHubPageModuleType.OwnedAssets, DataHubPageModuleType.Domains];
                expect(wouldCreateSizeMismatch(DataHubPageModuleType.Link, mixedTypes)).toBe(true);
            });
        });
    });

    describe('adjustPositionForSizeMismatch', () => {
        const smallModule: PageModuleFragment = {
            urn: 'urn:li:module:small',
            type: EntityType.DatahubPageModule,
            properties: {
                name: 'Small Module',
                type: DataHubPageModuleType.Link,
                visibility: { scope: PageModuleScope.Personal },
                params: {},
            },
        };

        const largeModule1: PageModuleFragment = {
            urn: 'urn:li:module:large1',
            type: EntityType.DatahubPageModule,
            properties: {
                name: 'Large Module 1',
                type: DataHubPageModuleType.OwnedAssets,
                visibility: { scope: PageModuleScope.Personal },
                params: {},
            },
        };

        const largeModule2: PageModuleFragment = {
            urn: 'urn:li:module:large2',
            type: EntityType.DatahubPageModule,
            properties: {
                name: 'Large Module 2',
                type: DataHubPageModuleType.Domains,
                visibility: { scope: PageModuleScope.Personal },
                params: {},
            },
        };

        it('should return original position when no template provided', () => {
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'left' };
            const result = adjustPositionForSizeMismatch(null, DataHubPageModuleType.Link, position);

            expect(result.position).toEqual(position);
            expect(result.shouldInsertNewRow).toBe(false);
        });

        it('should return original position when no rowIndex provided', () => {
            const template = createMockTemplate([{ modules: [largeModule1] }]);
            const position: ModulePositionInput = { rowSide: 'left' };
            const result = adjustPositionForSizeMismatch(template, DataHubPageModuleType.Link, position);

            expect(result.position).toEqual(position);
            expect(result.shouldInsertNewRow).toBe(false);
        });

        it('should return original position when target row is empty', () => {
            const template = createMockTemplate([{ modules: [] }]);
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'left' };
            const result = adjustPositionForSizeMismatch(template, DataHubPageModuleType.Link, position);

            expect(result.position).toEqual(position);
            expect(result.shouldInsertNewRow).toBe(false);
        });

        it('should return original position when target row does not exist', () => {
            const template = createMockTemplate([{ modules: [largeModule1] }]);
            const position: ModulePositionInput = { rowIndex: 5, rowSide: 'left' };
            const result = adjustPositionForSizeMismatch(template, DataHubPageModuleType.Link, position);

            expect(result.position).toEqual(position);
            expect(result.shouldInsertNewRow).toBe(false);
        });

        it('should return original position when no size mismatch', () => {
            const template = createMockTemplate([{ modules: [largeModule1, largeModule2] }]);
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'left' };
            const result = adjustPositionForSizeMismatch(template, DataHubPageModuleType.AssetCollection, position);

            expect(result.position).toEqual(position);
            expect(result.shouldInsertNewRow).toBe(false);
        });

        it('should redirect to new row when size mismatch detected', () => {
            const template = createMockTemplate([{ modules: [largeModule1, largeModule2] }]);
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'left' };
            const result = adjustPositionForSizeMismatch(template, DataHubPageModuleType.Link, position);

            expect(result.position).toEqual({
                rowIndex: 1,
                rowSide: 'left',
                moduleIndex: 0,
            });
            expect(result.shouldInsertNewRow).toBe(true);
        });

        it('should redirect small module when adding to large module row', () => {
            const template = createMockTemplate([{ modules: [largeModule1] }]);
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'right' };
            const result = adjustPositionForSizeMismatch(template, DataHubPageModuleType.Link, position);

            expect(result.position.rowIndex).toBe(1);
            expect(result.shouldInsertNewRow).toBe(true);
        });

        it('should redirect large module when adding to small module row', () => {
            const template = createMockTemplate([{ modules: [smallModule] }]);
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'left' };
            const result = adjustPositionForSizeMismatch(template, DataHubPageModuleType.OwnedAssets, position);

            expect(result.position.rowIndex).toBe(1);
            expect(result.shouldInsertNewRow).toBe(true);
        });
    });

    describe('handleModuleAdditionWithSizeMismatch', () => {
        const mockUpdateTemplateWithModule = vi.fn();

        const smallModule: PageModuleFragment = {
            urn: 'urn:li:module:small',
            type: EntityType.DatahubPageModule,
            properties: {
                name: 'Small Module',
                type: DataHubPageModuleType.Link,
                visibility: { scope: PageModuleScope.Personal },
                params: {},
            },
        };

        const largeModule: PageModuleFragment = {
            urn: 'urn:li:module:large',
            type: EntityType.DatahubPageModule,
            properties: {
                name: 'Large Module',
                type: DataHubPageModuleType.OwnedAssets,
                visibility: { scope: PageModuleScope.Personal },
                params: {},
            },
        };

        beforeEach(() => {
            mockUpdateTemplateWithModule.mockClear();
        });

        it('should use normal flow when no size mismatch', () => {
            const template = createMockTemplate([{ modules: [largeModule] }]);
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'right' };
            const newLargeModule: PageModuleFragment = {
                urn: 'urn:li:module:newLarge',
                type: EntityType.DatahubPageModule,
                properties: {
                    name: 'New Large Module',
                    type: DataHubPageModuleType.Domains,
                    visibility: { scope: PageModuleScope.Personal },
                    params: {},
                },
            };

            const mockUpdatedTemplate = createMockTemplate([{ modules: [largeModule, newLargeModule] }]);
            mockUpdateTemplateWithModule.mockReturnValue(mockUpdatedTemplate);

            const result = handleModuleAdditionWithSizeMismatch(
                template,
                newLargeModule,
                position,
                mockUpdateTemplateWithModule,
                false,
            );

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(template, newLargeModule, position, false);
            expect(result).toBe(mockUpdatedTemplate);
        });

        it('should create new row when size mismatch detected', () => {
            const template = createMockTemplate([{ modules: [largeModule] }]);
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'right' };

            const result = handleModuleAdditionWithSizeMismatch(
                template,
                smallModule,
                position,
                mockUpdateTemplateWithModule,
                false,
            );

            // Should NOT call updateTemplateWithModule when handling size mismatch
            expect(mockUpdateTemplateWithModule).not.toHaveBeenCalled();

            // Should return template with new row inserted
            expect(result).not.toBeNull();
            expect(result?.properties?.rows).toHaveLength(2);
            expect(result?.properties?.rows?.[0]?.modules).toHaveLength(1);
            expect(result?.properties?.rows?.[0]?.modules?.[0]).toBe(largeModule);
            expect(result?.properties?.rows?.[1]?.modules).toHaveLength(1);
            expect(result?.properties?.rows?.[1]?.modules?.[0]).toBe(smallModule);
        });

        it('should handle editing mode correctly', () => {
            const template = createMockTemplate([{ modules: [largeModule] }]);
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'right' };
            const newLargeModule: PageModuleFragment = {
                urn: 'urn:li:module:newLarge',
                type: EntityType.DatahubPageModule,
                properties: {
                    name: 'New Large Module',
                    type: DataHubPageModuleType.Domains,
                    visibility: { scope: PageModuleScope.Personal },
                    params: {},
                },
            };

            const mockUpdatedTemplate = createMockTemplate([{ modules: [largeModule, newLargeModule] }]);
            mockUpdateTemplateWithModule.mockReturnValue(mockUpdatedTemplate);

            const result = handleModuleAdditionWithSizeMismatch(
                template,
                newLargeModule,
                position,
                mockUpdateTemplateWithModule,
                true, // isEditingModule = true
            );

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(template, newLargeModule, position, true);
            expect(result).toBe(mockUpdatedTemplate);
        });

        it('should create new row at correct position when multiple rows exist', () => {
            const anotherLargeModule: PageModuleFragment = {
                urn: 'urn:li:module:another',
                type: EntityType.DatahubPageModule,
                properties: {
                    name: 'Another Large Module',
                    type: DataHubPageModuleType.AssetCollection,
                    visibility: { scope: PageModuleScope.Personal },
                    params: {},
                },
            };

            const template = createMockTemplate([{ modules: [largeModule] }, { modules: [anotherLargeModule] }]);
            const position: ModulePositionInput = { rowIndex: 0, rowSide: 'right' };

            const result = handleModuleAdditionWithSizeMismatch(
                template,
                smallModule,
                position,
                mockUpdateTemplateWithModule,
                false,
            );

            // Should have 3 rows: [large], [small], [another]
            expect(result?.properties?.rows).toHaveLength(3);
            expect(result?.properties?.rows?.[0]?.modules?.[0]).toBe(largeModule);
            expect(result?.properties?.rows?.[1]?.modules?.[0]).toBe(smallModule);
            expect(result?.properties?.rows?.[2]?.modules?.[0]).toBe(anotherLargeModule);
        });
    });

    describe('filterOutNonExistentModulesFromTemplate', () => {
        it('should remove not existing modules', () => {
            const existingModule = createMockModule('existing', 'urn:li:module:existing');
            const notExistingModule = createMockModule('existing', 'urn:li:module:existing', false);

            const template = createMockTemplate([{ modules: [existingModule, notExistingModule] }]);

            const result = filterOutNonExistentModulesFromTemplate(template);

            expect(result?.properties.rows).toHaveLength(1);
            expect(result?.properties?.rows?.[0]?.modules).toHaveLength(1);
            expect(result?.properties?.rows?.[0]?.modules?.[0]).toBe(existingModule);
        });

        it('should clean up empty rows', () => {
            const notExistingModule = createMockModule('existing', 'urn:li:module:existing', false);

            const template = createMockTemplate([{ modules: [notExistingModule] }]);

            const result = filterOutNonExistentModulesFromTemplate(template);

            expect(result?.properties.rows).toHaveLength(0);
        });
    });
});
