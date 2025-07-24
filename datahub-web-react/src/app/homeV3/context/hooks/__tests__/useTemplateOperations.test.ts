import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useTemplateOperations } from '@app/homeV3/context/hooks/useTemplateOperations';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment, useUpsertPageTemplateMutation } from '@graphql/template.generated';
import { useUpdateUserHomePageSettingsMutation } from '@graphql/user.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Mock GraphQL hooks
vi.mock('@graphql/template.generated');
vi.mock('@graphql/user.generated');

const mockUpsertPageTemplateMutation = vi.fn();
const mockUpdateUserHomePageSettings = vi.fn();

// Mock template data
const mockTemplate: PageTemplateFragment = {
    urn: 'urn:li:pageTemplate:test',
    type: EntityType.DatahubPageTemplate,
    properties: {
        rows: [
            {
                modules: [
                    {
                        urn: 'urn:li:pageModule:1',
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Module 1',
                            type: DataHubPageModuleType.Link,
                            visibility: { scope: PageModuleScope.Personal },
                            params: {},
                        },
                    },
                ],
            },
        ],
        surface: { surfaceType: PageTemplateSurfaceType.HomePage },
        visibility: { scope: PageTemplateScope.Personal },
    },
};

const mockModule: PageModuleFragment = {
    urn: 'urn:li:pageModule:new',
    type: EntityType.DatahubPageModule,
    properties: {
        name: 'New Module',
        type: DataHubPageModuleType.Link,
        visibility: { scope: PageModuleScope.Personal },
        params: {},
    },
};

const setPersonalTemplate = vi.fn();

describe('useTemplateOperations', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        (useUpsertPageTemplateMutation as any).mockReturnValue([mockUpsertPageTemplateMutation]);
        (useUpdateUserHomePageSettingsMutation as any).mockReturnValue([mockUpdateUserHomePageSettings]);
    });

    describe('updateTemplateWithModule', () => {
        it('should add module to new row when rowIndex is undefined', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const position: ModulePositionInput = {
                rowIndex: undefined,
                rowSide: 'left',
            };

            const updatedTemplate = result.current.updateTemplateWithModule(mockTemplate, mockModule, position, false);

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(2);
            expect(updatedTemplate?.properties?.rows?.[1]?.modules).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[1]?.modules?.[0]).toBe(mockModule);
        });

        it('should add module to existing row on the left side', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const updatedTemplate = result.current.updateTemplateWithModule(mockTemplate, mockModule, position, false);

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules).toHaveLength(2);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[0]).toBe(mockModule);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[1]?.urn).toBe('urn:li:pageModule:1');
        });

        it('should add module to existing row on the right side', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'right',
            };

            const updatedTemplate = result.current.updateTemplateWithModule(mockTemplate, mockModule, position, false);

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules).toHaveLength(2);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[0]?.urn).toBe('urn:li:pageModule:1');
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[1]).toBe(mockModule);
        });

        it('should create new row when rowIndex is out of bounds', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const position: ModulePositionInput = {
                rowIndex: 5,
                rowSide: 'left',
            };

            const updatedTemplate = result.current.updateTemplateWithModule(mockTemplate, mockModule, position, false);

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(2);
            expect(updatedTemplate?.properties?.rows?.[1]?.modules).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[1]?.modules?.[0]).toBe(mockModule);
        });

        it('should handle template with no rows', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const templateWithoutRows: PageTemplateFragment = {
                urn: 'urn:li:pageTemplate:empty',
                type: EntityType.DatahubPageTemplate,
                properties: {
                    rows: [],
                    surface: {
                        surfaceType: PageTemplateSurfaceType.HomePage,
                    },
                    visibility: {
                        scope: PageTemplateScope.Personal,
                    },
                },
            };

            const position: ModulePositionInput = {
                rowIndex: undefined,
                rowSide: 'left',
            };

            const updatedTemplate = result.current.updateTemplateWithModule(
                templateWithoutRows,
                mockModule,
                position,
                false,
            );

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[0]).toBe(mockModule);
        });

        it('should return null when template is null', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const updatedTemplate = result.current.updateTemplateWithModule(null, mockModule, position, false);

            expect(updatedTemplate).toBeNull();
        });
    });

    describe('removeModuleFromTemplate', () => {
        it('should remove module by moduleIndex when provided', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const templateWithMultipleModules: PageTemplateFragment = {
                ...mockTemplate,
                properties: {
                    ...mockTemplate.properties!,
                    rows: [
                        {
                            modules: [
                                {
                                    urn: 'urn:li:pageModule:1',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Module 1',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                                {
                                    urn: 'urn:li:pageModule:2',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Module 2',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                            ],
                        },
                    ],
                },
            };

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 1, // Remove second module
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(
                templateWithMultipleModules,
                'urn:li:pageModule:2',
                position,
            );

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[0]?.urn).toBe('urn:li:pageModule:1');
        });

        it('should handle duplicate URNs correctly with moduleIndex', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const templateWithDuplicateUrns: PageTemplateFragment = {
                ...mockTemplate,
                properties: {
                    ...mockTemplate.properties!,
                    rows: [
                        {
                            modules: [
                                {
                                    urn: 'urn:li:pageModule:duplicate',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Duplicate Module 1',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                                {
                                    urn: 'urn:li:pageModule:duplicate',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Duplicate Module 2',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                                {
                                    urn: 'urn:li:pageModule:unique',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Unique Module',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                            ],
                        },
                    ],
                },
            };

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 1, // Remove second duplicate module
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(
                templateWithDuplicateUrns,
                'urn:li:pageModule:duplicate',
                position,
            );

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules).toHaveLength(2);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[0]?.urn).toBe('urn:li:pageModule:duplicate');
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[0]?.properties?.name).toBe('Duplicate Module 1');
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[1]?.urn).toBe('urn:li:pageModule:unique');
        });

        it('should fall back to URN search when moduleIndex is invalid', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const templateWithMultipleModules: PageTemplateFragment = {
                ...mockTemplate,
                properties: {
                    ...mockTemplate.properties!,
                    rows: [
                        {
                            modules: [
                                {
                                    urn: 'urn:li:pageModule:1',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Module 1',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                                {
                                    urn: 'urn:li:pageModule:2',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Module 2',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                            ],
                        },
                    ],
                },
            };

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 10, // Invalid index, should fall back to URN search
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(
                templateWithMultipleModules,
                'urn:li:pageModule:2',
                position,
            );

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[0]?.urn).toBe('urn:li:pageModule:1');
        });

        it('should fall back to URN search when moduleIndex URN does not match', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const templateWithMultipleModules: PageTemplateFragment = {
                ...mockTemplate,
                properties: {
                    ...mockTemplate.properties!,
                    rows: [
                        {
                            modules: [
                                {
                                    urn: 'urn:li:pageModule:1',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Module 1',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                                {
                                    urn: 'urn:li:pageModule:2',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Module 2',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                            ],
                        },
                    ],
                },
            };

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0, // Points to module 1, but we're looking for module 2
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(
                templateWithMultipleModules,
                'urn:li:pageModule:2',
                position,
            );

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[0]?.urn).toBe('urn:li:pageModule:1');
        });

        it('should remove entire row when last module is removed', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const templateWithSingleModule: PageTemplateFragment = {
                ...mockTemplate,
                properties: {
                    ...mockTemplate.properties!,
                    rows: [
                        {
                            modules: [
                                {
                                    urn: 'urn:li:pageModule:1',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Module 1',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                            ],
                        },
                        {
                            modules: [
                                {
                                    urn: 'urn:li:pageModule:2',
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Module 2',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                            ],
                        },
                    ],
                },
            };

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(
                templateWithSingleModule,
                'urn:li:pageModule:1',
                position,
            );

            expect(updatedTemplate).not.toBeNull();
            expect(updatedTemplate?.properties?.rows).toHaveLength(1);
            expect(updatedTemplate?.properties?.rows?.[0]?.modules?.[0]?.urn).toBe('urn:li:pageModule:2');
        });

        it('should return original template when module is not found', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(
                mockTemplate,
                'urn:li:pageModule:nonexistent',
                position,
            );

            expect(updatedTemplate).toBe(mockTemplate);
        });

        it('should return original template when rowIndex is invalid', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const position: ModulePositionInput = {
                rowIndex: 5, // Invalid index
                rowSide: 'left',
                moduleIndex: 0,
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(
                mockTemplate,
                'urn:li:pageModule:1',
                position,
            );

            expect(updatedTemplate).toBe(mockTemplate);
        });

        it('should return original template when rowIndex is undefined', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const position: ModulePositionInput = {
                rowIndex: undefined,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(
                mockTemplate,
                'urn:li:pageModule:1',
                position,
            );

            expect(updatedTemplate).toBe(mockTemplate);
        });

        it('should return original template when template is null', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(null, 'urn:li:pageModule:1', position);

            expect(updatedTemplate).toBeNull();
        });

        it('should handle edge case with no modules in row', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const templateWithEmptyRow: PageTemplateFragment = {
                ...mockTemplate,
                properties: {
                    ...mockTemplate.properties!,
                    rows: [
                        {
                            modules: [],
                        },
                    ],
                },
            };

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const updatedTemplate = result.current.removeModuleFromTemplate(
                templateWithEmptyRow,
                'urn:li:pageModule:1',
                position,
            );

            expect(updatedTemplate).toBe(templateWithEmptyRow);
        });
    });

    describe('upsertTemplate', () => {
        it('should upsert personal template with correct input', async () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            mockUpsertPageTemplateMutation.mockResolvedValue({
                data: {
                    upsertPageTemplate: {
                        urn: 'urn:li:pageTemplate:new',
                    },
                },
            });

            await act(async () => {
                await result.current.upsertTemplate(mockTemplate, true, null);
            });

            expect(mockUpsertPageTemplateMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        urn: undefined, // undefined for create
                        rows: [
                            {
                                modules: ['urn:li:pageModule:1'],
                            },
                        ],
                        scope: PageTemplateScope.Personal,
                        surfaceType: PageTemplateSurfaceType.HomePage,
                    },
                },
            });
        });

        it('should upsert existing personal template with URN', async () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            mockUpsertPageTemplateMutation.mockResolvedValue({
                data: {
                    upsertPageTemplate: {
                        urn: 'urn:li:pageTemplate:existing',
                    },
                },
            });

            await act(async () => {
                await result.current.upsertTemplate(mockTemplate, true, mockTemplate);
            });

            expect(mockUpsertPageTemplateMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        urn: 'urn:li:pageTemplate:test',
                        rows: [
                            {
                                modules: ['urn:li:pageModule:1'],
                            },
                        ],
                        scope: PageTemplateScope.Personal,
                        surfaceType: PageTemplateSurfaceType.HomePage,
                    },
                },
            });
        });

        it('should upsert global template with correct input', async () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            mockUpsertPageTemplateMutation.mockResolvedValue({
                data: {
                    upsertPageTemplate: {
                        urn: 'urn:li:pageTemplate:global',
                    },
                },
            });

            await act(async () => {
                await result.current.upsertTemplate(mockTemplate, false, null);
            });

            expect(mockUpsertPageTemplateMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        urn: 'urn:li:pageTemplate:test',
                        rows: [
                            {
                                modules: ['urn:li:pageModule:1'],
                            },
                        ],
                        scope: PageTemplateScope.Global,
                        surfaceType: PageTemplateSurfaceType.HomePage,
                    },
                },
            });
        });

        it('should update user settings when creating personal template', async () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            mockUpsertPageTemplateMutation.mockResolvedValue({
                data: {
                    upsertPageTemplate: {
                        urn: 'urn:li:pageTemplate:new',
                    },
                },
            });

            await act(async () => {
                await result.current.upsertTemplate(mockTemplate, true, null);
            });

            expect(mockUpdateUserHomePageSettings).toHaveBeenCalledWith({
                variables: {
                    input: {
                        pageTemplate: 'urn:li:pageTemplate:new',
                    },
                },
            });
        });

        it('should not update user settings when updating existing personal template', async () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            mockUpsertPageTemplateMutation.mockResolvedValue({
                data: {
                    upsertPageTemplate: {
                        urn: 'urn:li:pageTemplate:existing',
                    },
                },
            });

            await act(async () => {
                await result.current.upsertTemplate(mockTemplate, true, mockTemplate);
            });

            expect(mockUpdateUserHomePageSettings).not.toHaveBeenCalled();
        });

        it('should handle template with empty rows', async () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const templateWithEmptyRows: PageTemplateFragment = {
                urn: 'urn:li:pageTemplate:empty',
                type: EntityType.DatahubPageTemplate,
                properties: {
                    rows: [
                        {
                            modules: [],
                        },
                    ],
                    surface: { surfaceType: PageTemplateSurfaceType.HomePage },
                    visibility: { scope: PageTemplateScope.Personal },
                },
            };

            mockUpsertPageTemplateMutation.mockResolvedValue({
                data: {
                    upsertPageTemplate: {
                        urn: 'urn:li:pageTemplate:empty',
                    },
                },
            });

            await act(async () => {
                await result.current.upsertTemplate(templateWithEmptyRows, true, null);
            });

            expect(mockUpsertPageTemplateMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        urn: undefined,
                        rows: [
                            {
                                modules: [],
                            },
                        ],
                        scope: PageTemplateScope.Personal,
                        surfaceType: PageTemplateSurfaceType.HomePage,
                    },
                },
            });
        });

        it('should handle mutation error', async () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            const error = new Error('Mutation failed');
            mockUpsertPageTemplateMutation.mockRejectedValue(error);

            await expect(result.current.upsertTemplate(mockTemplate, true, null)).rejects.toThrow('Mutation failed');
        });
    });

    describe('resetTemplateToDefault', () => {
        it('should call setPersonalTemplate with null', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            act(() => {
                result.current.resetTemplateToDefault();
            });

            expect(setPersonalTemplate).toHaveBeenCalledWith(null);
        });

        it('should call updateUserHomePageSettingsMutation with pageTemplate: null', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            act(() => {
                result.current.resetTemplateToDefault();
            });

            expect(mockUpdateUserHomePageSettings).toHaveBeenCalledWith({
                variables: {
                    input: {
                        removePageTemplate: true,
                    },
                },
            });
        });

        it('should call setPersonalTemplate and mutation exactly once on multiple calls', () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            act(() => {
                result.current.resetTemplateToDefault();
                result.current.resetTemplateToDefault();
            });

            expect(setPersonalTemplate).toHaveBeenCalledTimes(2);
            expect(mockUpdateUserHomePageSettings).toHaveBeenCalledTimes(2);
        });

        it('should handle async mutation call correctly', async () => {
            const { result } = renderHook(() => useTemplateOperations(setPersonalTemplate));

            await act(async () => {
                await result.current.resetTemplateToDefault();
            });

            expect(setPersonalTemplate).toHaveBeenCalledWith(null);
            expect(mockUpdateUserHomePageSettings).toHaveBeenCalledWith({
                variables: {
                    input: {
                        removePageTemplate: true,
                    },
                },
            });
        });
    });
});
