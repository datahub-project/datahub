import { act, renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import { vi } from 'vitest';

import { useModuleOperations } from '@app/homeV3/context/hooks/useModuleOperations';
import { DEFAULT_MODULE_URNS } from '@app/homeV3/modules/constants';
import { ModulePositionInput } from '@app/homeV3/template/types';

import {
    PageModuleFragment,
    PageTemplateFragment,
    useDeletePageModuleMutation,
    useUpsertPageModuleMutation,
} from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Mock GraphQL hooks
vi.mock('@graphql/template.generated');

// Mock antd message
vi.mock('antd', () => ({
    message: {
        error: vi.fn(() => ({ key: 'test-message' })),
        warning: vi.fn(() => ({ key: 'test-message' })),
    },
}));

const mockUpsertPageModuleMutation = vi.fn();
const mockDeletePageModuleMutation = vi.fn();

// Mock template data
const mockPersonalTemplate: PageTemplateFragment = {
    urn: 'urn:li:pageTemplate:personal',
    type: EntityType.DatahubPageTemplate,
    properties: {
        rows: [
            {
                modules: [
                    {
                        urn: 'urn:li:pageModule:1',
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Personal Module 1',
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

const mockGlobalTemplate: PageTemplateFragment = {
    urn: 'urn:li:pageTemplate:global',
    type: EntityType.DatahubPageTemplate,
    properties: {
        rows: [
            {
                modules: [
                    {
                        urn: 'urn:li:pageModule:2',
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Global Module 1',
                            type: DataHubPageModuleType.Link,
                            visibility: { scope: PageModuleScope.Global },
                            params: {},
                        },
                    },
                ],
            },
        ],
        surface: { surfaceType: PageTemplateSurfaceType.HomePage },
        visibility: { scope: PageTemplateScope.Global },
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

// Mock functions
const mockSetPersonalTemplate = vi.fn();
const mockSetGlobalTemplate = vi.fn();
const mockUpdateTemplateWithModule = vi.fn();
const mockRemoveModuleFromTemplate = vi.fn();
const mockUpsertTemplate = vi.fn();

describe('useModuleOperations', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        (useUpsertPageModuleMutation as any).mockReturnValue([mockUpsertPageModuleMutation]);
        (useDeletePageModuleMutation as any).mockReturnValue([mockDeletePageModuleMutation]);
    });

    describe('addModule', () => {
        it('should add module to personal template when not editing global', () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const updatedTemplate: PageTemplateFragment = {
                ...mockPersonalTemplate,
                properties: {
                    ...mockPersonalTemplate.properties!,
                    rows: [
                        {
                            modules: [mockModule, ...mockPersonalTemplate.properties!.rows![0].modules!],
                        },
                    ],
                },
            };

            mockUpdateTemplateWithModule.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockResolvedValue({});

            act(() => {
                result.current.addModule({
                    module: mockModule,
                    position,
                });
            });

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(
                mockPersonalTemplate,
                mockModule,
                position,
                false,
            );
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, mockPersonalTemplate);
        });

        it('should add module to global template when editing global', () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    true, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'right',
            };

            const updatedTemplate: PageTemplateFragment = {
                ...mockGlobalTemplate,
                properties: {
                    ...mockGlobalTemplate.properties!,
                    rows: [
                        {
                            modules: [...mockGlobalTemplate.properties!.rows![0].modules!, mockModule],
                        },
                    ],
                },
            };

            mockUpdateTemplateWithModule.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockResolvedValue({});

            act(() => {
                result.current.addModule({
                    module: mockModule,
                    position,
                });
            });

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(mockGlobalTemplate, mockModule, position, false);
            expect(mockSetGlobalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, false, mockPersonalTemplate);
        });

        it('should use global template when personal template is null', () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    null, // personalTemplate
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false,
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: undefined,
                rowSide: 'left',
            };

            const updatedTemplate: PageTemplateFragment = {
                ...mockGlobalTemplate,
                properties: {
                    ...mockGlobalTemplate.properties!,
                    rows: [
                        ...mockGlobalTemplate.properties!.rows!,
                        {
                            modules: [mockModule],
                        },
                    ],
                },
            };

            mockUpdateTemplateWithModule.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockResolvedValue({});

            act(() => {
                result.current.addModule({
                    module: mockModule,
                    position,
                });
            });

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(mockGlobalTemplate, mockModule, position, false);
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, null);
        });

        it('should revert state on template upsert error', async () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false,
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const updatedTemplate: PageTemplateFragment = {
                ...mockPersonalTemplate,
                properties: {
                    ...mockPersonalTemplate.properties!,
                    rows: [
                        {
                            modules: [mockModule, ...mockPersonalTemplate.properties!.rows![0].modules!],
                        },
                    ],
                },
            };

            const error = new Error('Upsert failed');
            mockUpdateTemplateWithModule.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockRejectedValue(error);

            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

            act(() => {
                result.current.addModule({
                    module: mockModule,
                    position,
                });
            });

            // Wait for the promise to resolve
            await new Promise((resolve) => {
                setTimeout(resolve, 0);
            });

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(
                mockPersonalTemplate,
                mockModule,
                position,
                false,
            );
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, mockPersonalTemplate);
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(mockPersonalTemplate); // Revert call
            expect(consoleSpy).toHaveBeenCalledWith('Failed to add module:', error);

            consoleSpy.mockRestore();
        });
    });

    describe('removeModule', () => {
        it('should remove module from personal template when not editing global', () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const removeModuleInput = {
                module: mockPersonalTemplate.properties!.rows![0].modules![0],
                position,
            };

            const updatedTemplate: PageTemplateFragment = {
                ...mockPersonalTemplate,
                properties: {
                    ...mockPersonalTemplate.properties!,
                    rows: [],
                },
            };

            mockRemoveModuleFromTemplate.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockResolvedValue({});

            act(() => {
                result.current.removeModule(removeModuleInput);
            });

            expect(mockRemoveModuleFromTemplate).toHaveBeenCalledWith(
                mockPersonalTemplate,
                'urn:li:pageModule:1',
                position,
                true,
            );
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, mockPersonalTemplate);
            expect(mockDeletePageModuleMutation).toHaveBeenCalledWith({
                variables: { input: { urn: 'urn:li:pageModule:1' } },
            });
        });

        it('should remove module from global template when editing global', () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    true, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'right',
                moduleIndex: 1,
            };

            const removeModuleInput = {
                module: mockGlobalTemplate.properties!.rows![0].modules![0],
                position,
            };

            const updatedTemplate: PageTemplateFragment = {
                ...mockGlobalTemplate,
                properties: {
                    ...mockGlobalTemplate.properties!,
                    rows: [],
                },
            };

            mockRemoveModuleFromTemplate.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockResolvedValue({});

            act(() => {
                result.current.removeModule(removeModuleInput);
            });

            expect(mockRemoveModuleFromTemplate).toHaveBeenCalledWith(
                mockGlobalTemplate,
                'urn:li:pageModule:2',
                position,
                true,
            );
            expect(mockSetGlobalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, false, mockPersonalTemplate);
            expect(mockDeletePageModuleMutation).toHaveBeenCalledWith({
                variables: { input: { urn: 'urn:li:pageModule:2' } },
            });
        });

        it('should use global template when personal template is null', () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    null, // personalTemplate
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const removeModuleInput = {
                module: mockGlobalTemplate.properties!.rows![0].modules![0],
                position,
            };

            const updatedTemplate: PageTemplateFragment = {
                ...mockGlobalTemplate,
                properties: {
                    ...mockGlobalTemplate.properties!,
                    rows: [],
                },
            };

            mockRemoveModuleFromTemplate.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockResolvedValue({});

            act(() => {
                result.current.removeModule(removeModuleInput);
            });

            expect(mockRemoveModuleFromTemplate).toHaveBeenCalledWith(
                mockGlobalTemplate,
                'urn:li:pageModule:2',
                position,
                true,
            );
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, null);
            // Should NOT delete global module when removing from personal template
            expect(mockDeletePageModuleMutation).not.toHaveBeenCalled();
        });

        it('should revert state on template upsert error', async () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const error = new Error('Upsert failed');

            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const removeModuleInput = {
                module: mockPersonalTemplate.properties!.rows![0].modules![0],
                position,
            };

            const updatedTemplate: PageTemplateFragment = {
                ...mockPersonalTemplate,
                properties: {
                    ...mockPersonalTemplate.properties!,
                    rows: [],
                },
            };

            mockRemoveModuleFromTemplate.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockRejectedValue(error);

            await act(async () => {
                result.current.removeModule(removeModuleInput);
            });

            expect(mockRemoveModuleFromTemplate).toHaveBeenCalledWith(
                mockPersonalTemplate,
                'urn:li:pageModule:1',
                position,
                true,
            );
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, mockPersonalTemplate);
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(mockPersonalTemplate); // Revert call
            expect(consoleSpy).toHaveBeenCalledWith('Failed to remove module:', error);
            // Should still attempt to delete module even if template upsert fails
            expect(mockDeletePageModuleMutation).toHaveBeenCalledWith({
                variables: { input: { urn: 'urn:li:pageModule:1' } },
            });

            consoleSpy.mockRestore();
        });

        it('should validate input and show error for missing module URN', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const messageSpy = vi.spyOn(message, 'error').mockReturnValue({ key: 'test-message' } as any);

            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const removeModuleInput = {
                module: { ...mockPersonalTemplate.properties!.rows![0].modules![0], urn: '' }, // Invalid empty URN
                position,
            };

            act(() => {
                result.current.removeModule(removeModuleInput);
            });

            expect(consoleSpy).toHaveBeenCalledWith(
                'Invalid removeModule input:',
                'Module URN is required for removal',
            );
            expect(messageSpy).toHaveBeenCalledWith('Module URN is required for removal');
            expect(mockRemoveModuleFromTemplate).not.toHaveBeenCalled();
            expect(mockUpsertTemplate).not.toHaveBeenCalled();

            consoleSpy.mockRestore();
            messageSpy.mockRestore();
        });

        it('should validate input and show error for missing position', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const messageSpy = vi.spyOn(message, 'error').mockReturnValue({ key: 'test-message' } as any);

            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const removeModuleInput = {
                module: mockPersonalTemplate.properties!.rows![0].modules![0],
                position: null as any, // Invalid null position
            };

            act(() => {
                result.current.removeModule(removeModuleInput);
            });

            expect(consoleSpy).toHaveBeenCalledWith(
                'Invalid removeModule input:',
                'Module position is required for removal',
            );
            expect(messageSpy).toHaveBeenCalledWith('Module position is required for removal');
            expect(mockRemoveModuleFromTemplate).not.toHaveBeenCalled();
            expect(mockUpsertTemplate).not.toHaveBeenCalled();

            consoleSpy.mockRestore();
            messageSpy.mockRestore();
        });

        it('should validate input and show error for invalid rowIndex', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const messageSpy = vi.spyOn(message, 'error').mockReturnValue({ key: 'test-message' } as any);

            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: -1, // Invalid negative rowIndex
                rowSide: 'left',
                moduleIndex: 0,
            };

            const removeModuleInput = {
                module: mockPersonalTemplate.properties!.rows![0].modules![0],
                position,
            };

            act(() => {
                result.current.removeModule(removeModuleInput);
            });

            expect(consoleSpy).toHaveBeenCalledWith(
                'Invalid removeModule input:',
                'Valid row index is required for removal',
            );
            expect(messageSpy).toHaveBeenCalledWith('Valid row index is required for removal');
            expect(mockRemoveModuleFromTemplate).not.toHaveBeenCalled();
            expect(mockUpsertTemplate).not.toHaveBeenCalled();

            consoleSpy.mockRestore();
            messageSpy.mockRestore();
        });

        it('should handle case when no template is available', () => {
            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
            const messageSpy = vi.spyOn(message, 'error').mockReturnValue({ key: 'test-message' } as any);

            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    null, // personalTemplate
                    null, // globalTemplate
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
                moduleIndex: 0,
            };

            const removeModuleInput = {
                module: mockPersonalTemplate.properties!.rows![0].modules![0],
                position,
            };

            act(() => {
                result.current.removeModule(removeModuleInput);
            });

            expect(consoleSpy).toHaveBeenCalledWith('No template provided to update');
            expect(messageSpy).toHaveBeenCalledWith('No template available to update');
            expect(mockRemoveModuleFromTemplate).not.toHaveBeenCalled();
            expect(mockUpsertTemplate).not.toHaveBeenCalled();

            consoleSpy.mockRestore();
            messageSpy.mockRestore();
        });

        describe('module deletion logic', () => {
            it('should NOT delete global module when removing from personal template', () => {
                const globalModule: PageModuleFragment = {
                    urn: 'urn:li:pageModule:global',
                    type: EntityType.DatahubPageModule,
                    properties: {
                        name: 'Global Module',
                        type: DataHubPageModuleType.Link,
                        visibility: { scope: PageModuleScope.Global },
                        params: {},
                    },
                };

                const { result } = renderHook(() =>
                    useModuleOperations(
                        false, // isEditingGlobalTemplate - editing personal
                        mockPersonalTemplate,
                        mockGlobalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        false,
                        null,
                    ),
                );

                const position: ModulePositionInput = {
                    rowIndex: 0,
                    rowSide: 'left',
                    moduleIndex: 0,
                };

                const removeModuleInput = {
                    module: globalModule,
                    position,
                };

                mockRemoveModuleFromTemplate.mockReturnValue(mockPersonalTemplate);
                mockUpsertTemplate.mockResolvedValue({});

                act(() => {
                    result.current.removeModule(removeModuleInput);
                });

                expect(mockDeletePageModuleMutation).not.toHaveBeenCalled();
            });

            it('should NOT delete default module from DEFAULT_MODULE_URNS', () => {
                const defaultModule: PageModuleFragment = {
                    urn: DEFAULT_MODULE_URNS[0], // Use first default module URN
                    type: EntityType.DatahubPageModule,
                    properties: {
                        name: 'Default Module',
                        type: DataHubPageModuleType.OwnedAssets,
                        visibility: { scope: PageModuleScope.Personal },
                        params: {},
                    },
                };

                const { result } = renderHook(() =>
                    useModuleOperations(
                        false,
                        mockPersonalTemplate,
                        mockGlobalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        false,
                        null,
                    ),
                );

                const position: ModulePositionInput = {
                    rowIndex: 0,
                    rowSide: 'left',
                    moduleIndex: 0,
                };

                const removeModuleInput = {
                    module: defaultModule,
                    position,
                };

                mockRemoveModuleFromTemplate.mockReturnValue(mockPersonalTemplate);
                mockUpsertTemplate.mockResolvedValue({});

                act(() => {
                    result.current.removeModule(removeModuleInput);
                });

                expect(mockDeletePageModuleMutation).not.toHaveBeenCalled();
            });

            it('should DELETE regular personal module when removing from personal template', () => {
                const regularModule: PageModuleFragment = {
                    urn: 'urn:li:pageModule:regular',
                    type: EntityType.DatahubPageModule,
                    properties: {
                        name: 'Regular Module',
                        type: DataHubPageModuleType.Link,
                        visibility: { scope: PageModuleScope.Personal },
                        params: {},
                    },
                };

                const { result } = renderHook(() =>
                    useModuleOperations(
                        false,
                        mockPersonalTemplate,
                        mockGlobalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        false,
                        null,
                    ),
                );

                const position: ModulePositionInput = {
                    rowIndex: 0,
                    rowSide: 'left',
                    moduleIndex: 0,
                };

                const removeModuleInput = {
                    module: regularModule,
                    position,
                };

                mockRemoveModuleFromTemplate.mockReturnValue(mockPersonalTemplate);
                mockUpsertTemplate.mockResolvedValue({});

                act(() => {
                    result.current.removeModule(removeModuleInput);
                });

                expect(mockDeletePageModuleMutation).toHaveBeenCalledWith({
                    variables: { input: { urn: 'urn:li:pageModule:regular' } },
                });
            });

            it('should DELETE global module when removing from global template', () => {
                const globalModule: PageModuleFragment = {
                    urn: 'urn:li:pageModule:global',
                    type: EntityType.DatahubPageModule,
                    properties: {
                        name: 'Global Module',
                        type: DataHubPageModuleType.Link,
                        visibility: { scope: PageModuleScope.Global },
                        params: {},
                    },
                };

                const { result } = renderHook(() =>
                    useModuleOperations(
                        true, // isEditingGlobalTemplate - editing global
                        mockPersonalTemplate,
                        mockGlobalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        false,
                        null,
                    ),
                );

                const position: ModulePositionInput = {
                    rowIndex: 0,
                    rowSide: 'left',
                    moduleIndex: 0,
                };

                const removeModuleInput = {
                    module: globalModule,
                    position,
                };

                mockRemoveModuleFromTemplate.mockReturnValue(mockGlobalTemplate);
                mockUpsertTemplate.mockResolvedValue({});

                act(() => {
                    result.current.removeModule(removeModuleInput);
                });

                expect(mockDeletePageModuleMutation).toHaveBeenCalledWith({
                    variables: { input: { urn: 'urn:li:pageModule:global' } },
                });
            });

            it('should handle deletion error gracefully', () => {
                const error = new Error('Delete failed');
                mockDeletePageModuleMutation.mockRejectedValue(error);

                const regularModule: PageModuleFragment = {
                    urn: 'urn:li:pageModule:regular',
                    type: EntityType.DatahubPageModule,
                    properties: {
                        name: 'Regular Module',
                        type: DataHubPageModuleType.Link,
                        visibility: { scope: PageModuleScope.Personal },
                        params: {},
                    },
                };

                const { result } = renderHook(() =>
                    useModuleOperations(
                        false,
                        mockPersonalTemplate,
                        mockGlobalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        false,
                        null,
                    ),
                );

                const position: ModulePositionInput = {
                    rowIndex: 0,
                    rowSide: 'left',
                    moduleIndex: 0,
                };

                const removeModuleInput = {
                    module: regularModule,
                    position,
                };

                mockRemoveModuleFromTemplate.mockReturnValue(mockPersonalTemplate);
                mockUpsertTemplate.mockResolvedValue({});

                // Should not throw error even if deletion fails
                expect(() => {
                    act(() => {
                        result.current.removeModule(removeModuleInput);
                    });
                }).not.toThrow();

                expect(mockDeletePageModuleMutation).toHaveBeenCalledWith({
                    variables: { input: { urn: 'urn:li:pageModule:regular' } },
                });
            });
        });
    });

    describe('upsertModule', () => {
        it('should create module and add it to template', async () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false,
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const upsertModuleInput = {
                name: 'Test Module',
                type: DataHubPageModuleType.Link,
                scope: PageModuleScope.Personal,
                params: { limit: 10 },
                position,
            };

            const createdModuleUrn = 'urn:li:pageModule:created';
            mockUpsertPageModuleMutation.mockResolvedValue({
                data: {
                    upsertPageModule: {
                        urn: createdModuleUrn,
                    },
                },
            });

            const updatedTemplate: PageTemplateFragment = {
                ...mockPersonalTemplate,
                properties: {
                    ...mockPersonalTemplate.properties!,
                    rows: [
                        {
                            modules: [
                                {
                                    urn: createdModuleUrn,
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Test Module',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Personal },
                                        params: {},
                                    },
                                },
                                ...mockPersonalTemplate.properties!.rows![0].modules!,
                            ],
                        },
                    ],
                },
            };

            mockUpdateTemplateWithModule.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockResolvedValue({});

            await act(async () => {
                result.current.upsertModule(upsertModuleInput);
            });

            expect(mockUpsertPageModuleMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        name: 'Test Module',
                        type: DataHubPageModuleType.Link,
                        scope: PageModuleScope.Personal,
                        params: { limit: 10 },
                        urn: undefined,
                    },
                },
            });

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(
                mockPersonalTemplate,
                {
                    urn: createdModuleUrn,
                    type: EntityType.DatahubPageModule,
                    properties: {
                        name: 'Test Module',
                        type: DataHubPageModuleType.Link,
                        visibility: { scope: PageModuleScope.Personal },
                        params: { limit: 10 },
                    },
                },
                position,
                false,
            );

            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, mockPersonalTemplate);
        });

        it('should use default scope when not provided', async () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false,
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: undefined,
                rowSide: 'right',
            };

            const upsertModuleInput = {
                name: 'Test Module',
                type: DataHubPageModuleType.Link,
                scope: PageModuleScope.Personal,
                position,
            };

            const createdModuleUrn = 'urn:li:pageModule:created';
            mockUpsertPageModuleMutation.mockResolvedValue({
                data: {
                    upsertPageModule: {
                        urn: createdModuleUrn,
                    },
                },
            });

            mockUpdateTemplateWithModule.mockReturnValue(mockPersonalTemplate);
            mockUpsertTemplate.mockResolvedValue({});

            await act(async () => {
                await result.current.upsertModule(upsertModuleInput);
            });

            expect(mockUpsertPageModuleMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        name: 'Test Module',
                        type: DataHubPageModuleType.Link,
                        scope: PageModuleScope.Personal, // Default scope
                        params: {}, // Default empty params
                        urn: undefined,
                    },
                },
            });
        });

        it('should throw error when module creation fails', async () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false,
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const upsertModuleInput = {
                name: 'Test Module',
                type: DataHubPageModuleType.Link,
                scope: PageModuleScope.Personal,
                position,
            };

            const error = new Error('Module creation failed');
            mockUpsertPageModuleMutation.mockRejectedValue(error);

            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

            // Mock the global unhandled rejection handler
            const originalUnhandledRejectionHandler = window.addEventListener;
            const unhandledRejectionSpy = vi.fn();
            window.addEventListener = vi.fn().mockImplementation((event, handler) => {
                if (event === 'unhandledrejection') {
                    unhandledRejectionSpy.mockImplementation(handler);
                }
            });

            await act(async () => {
                result.current.upsertModule(upsertModuleInput);
            });

            // Wait for the async operation to complete
            await new Promise((resolve) => {
                setTimeout(resolve, 0);
            });

            expect(consoleSpy).toHaveBeenCalledWith('Failed to create module:', error);

            consoleSpy.mockRestore();
            window.addEventListener = originalUnhandledRejectionHandler;
        });

        it('should create module for global template when editing global', async () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    true, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false,
                    null, // originalModuleData
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const upsertModuleInput = {
                name: 'Global Module',
                type: DataHubPageModuleType.Link,
                scope: PageModuleScope.Global,
                position,
            };

            const createdModuleUrn = 'urn:li:pageModule:global';
            mockUpsertPageModuleMutation.mockResolvedValue({
                data: {
                    upsertPageModule: {
                        urn: createdModuleUrn,
                    },
                },
            });

            const updatedTemplate: PageTemplateFragment = {
                ...mockGlobalTemplate,
                properties: {
                    ...mockGlobalTemplate.properties!,
                    rows: [
                        {
                            modules: [
                                {
                                    urn: createdModuleUrn,
                                    type: EntityType.DatahubPageModule,
                                    properties: {
                                        name: 'Global Module',
                                        type: DataHubPageModuleType.Link,
                                        visibility: { scope: PageModuleScope.Global },
                                        params: {},
                                    },
                                },
                                ...mockGlobalTemplate.properties!.rows![0].modules!,
                            ],
                        },
                    ],
                },
            };

            mockUpdateTemplateWithModule.mockReturnValue(updatedTemplate);
            mockUpsertTemplate.mockResolvedValue({});

            await act(async () => {
                result.current.upsertModule(upsertModuleInput);
            });

            expect(mockUpsertPageModuleMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        name: 'Global Module',
                        type: DataHubPageModuleType.Link,
                        scope: PageModuleScope.Global,
                        params: {},
                        urn: undefined,
                    },
                },
            });

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(
                mockGlobalTemplate,
                {
                    urn: createdModuleUrn,
                    type: EntityType.DatahubPageModule,
                    properties: {
                        name: 'Global Module',
                        type: DataHubPageModuleType.Link,
                        visibility: { scope: PageModuleScope.Global },
                        params: {},
                    },
                },
                position,
                false,
            );

            expect(mockSetGlobalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, false, mockPersonalTemplate);
        });

        // Tests for global module replacement functionality
        describe('global module replacement', () => {
            const mockGlobalModuleToEdit: PageModuleFragment = {
                urn: 'urn:li:pageModule:globalToEdit',
                type: EntityType.DatahubPageModule,
                properties: {
                    name: 'Global Module To Edit',
                    type: DataHubPageModuleType.Link,
                    visibility: { scope: PageModuleScope.Global },
                    params: {},
                },
            };

            const mockPersonalModuleToEdit: PageModuleFragment = {
                urn: 'urn:li:pageModule:personalToEdit',
                type: EntityType.DatahubPageModule,
                properties: {
                    name: 'Personal Module To Edit',
                    type: DataHubPageModuleType.Link,
                    visibility: { scope: PageModuleScope.Personal },
                    params: {},
                },
            };

            it('should create new personal module when editing global module on personal template', async () => {
                const { result } = renderHook(() =>
                    useModuleOperations(
                        false, // isEditingGlobalTemplate = false (editing personal template)
                        mockPersonalTemplate,
                        mockGlobalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        true, // isEditingModule = true
                        mockGlobalModuleToEdit, // originalModuleData = global module
                    ),
                );

                const position: ModulePositionInput = {
                    rowIndex: 0,
                    moduleIndex: 0,
                };

                const upsertModuleInput = {
                    urn: mockGlobalModuleToEdit.urn,
                    name: 'Updated Global Module Name',
                    type: DataHubPageModuleType.Link,
                    position,
                    params: { newParam: 'updatedValue' },
                };

                const newPersonalModuleUrn = 'urn:li:pageModule:newPersonal';
                mockUpsertPageModuleMutation.mockResolvedValue({
                    data: {
                        upsertPageModule: {
                            urn: newPersonalModuleUrn,
                        },
                    },
                });

                const templateAfterRemoval = {
                    ...mockPersonalTemplate,
                    properties: {
                        ...mockPersonalTemplate.properties!,
                        rows: [{ modules: [] }], // Global module removed
                    },
                };

                const templateAfterReplacement = {
                    ...templateAfterRemoval,
                    properties: {
                        ...templateAfterRemoval.properties!,
                        rows: [
                            {
                                modules: [
                                    {
                                        urn: newPersonalModuleUrn,
                                        type: EntityType.DatahubPageModule,
                                        properties: {
                                            name: 'Updated Global Module Name',
                                            type: DataHubPageModuleType.Link,
                                            visibility: { scope: PageModuleScope.Personal },
                                            params: { newParam: 'updatedValue' },
                                        },
                                    },
                                ],
                            },
                        ],
                    },
                };

                mockRemoveModuleFromTemplate.mockReturnValue(templateAfterRemoval);
                mockUpdateTemplateWithModule.mockReturnValue(templateAfterReplacement);
                mockUpsertTemplate.mockResolvedValue({});

                await act(async () => {
                    result.current.upsertModule(upsertModuleInput);
                });

                // Should create new module WITHOUT the original urn (creates new instead of updating)
                expect(mockUpsertPageModuleMutation).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            name: 'Updated Global Module Name',
                            type: DataHubPageModuleType.Link,
                            scope: PageModuleScope.Personal, // Should use personal scope
                            params: { newParam: 'updatedValue' },
                            urn: undefined, // Should NOT pass original urn (creates new module)
                        },
                    },
                });

                // Should remove the original global module
                expect(mockRemoveModuleFromTemplate).toHaveBeenCalledWith(
                    mockPersonalTemplate,
                    mockGlobalModuleToEdit.urn,
                    position,
                    false, // shouldRemoveEmptyRow = false (to replace a module we should keep empty row)
                );

                // Should add the new personal module in the same position
                expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(
                    templateAfterRemoval,
                    {
                        urn: newPersonalModuleUrn,
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Updated Global Module Name',
                            type: DataHubPageModuleType.Link,
                            visibility: { scope: PageModuleScope.Personal },
                            params: { newParam: 'updatedValue' },
                        },
                    },
                    position,
                    false, // isEditing = false (adding new module)
                );

                // Should update the personal template
                expect(mockSetPersonalTemplate).toHaveBeenCalledWith(templateAfterReplacement);
                expect(mockUpsertTemplate).toHaveBeenCalledWith(templateAfterReplacement, true, mockPersonalTemplate);
            });

            it('should edit global module in place when editing global template', async () => {
                const { result } = renderHook(() =>
                    useModuleOperations(
                        true, // isEditingGlobalTemplate = true (editing global template)
                        mockPersonalTemplate,
                        mockGlobalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        true, // isEditingModule = true
                        mockGlobalModuleToEdit, // originalModuleData = global module
                    ),
                );

                const position: ModulePositionInput = {
                    rowIndex: 0,
                    moduleIndex: 0,
                };

                const upsertModuleInput = {
                    urn: mockGlobalModuleToEdit.urn,
                    name: 'Updated Global Module Name',
                    type: DataHubPageModuleType.Link,
                    position,
                    params: { newParam: 'updatedValue' },
                };

                const updatedModuleUrn = mockGlobalModuleToEdit.urn; // Same urn (editing in place)
                mockUpsertPageModuleMutation.mockResolvedValue({
                    data: {
                        upsertPageModule: {
                            urn: updatedModuleUrn,
                        },
                    },
                });

                const templateAfterUpdate = {
                    ...mockGlobalTemplate,
                    // Template updated with modified module
                };

                mockUpdateTemplateWithModule.mockReturnValue(templateAfterUpdate);
                mockUpsertTemplate.mockResolvedValue({});

                await act(async () => {
                    result.current.upsertModule(upsertModuleInput);
                });

                // Should edit the existing module (pass the original urn)
                expect(mockUpsertPageModuleMutation).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            name: 'Updated Global Module Name',
                            type: DataHubPageModuleType.Link,
                            scope: PageModuleScope.Global, // Should keep global scope
                            params: { newParam: 'updatedValue' },
                            urn: mockGlobalModuleToEdit.urn, // Should pass original urn (edit in place)
                        },
                    },
                });

                // Should NOT remove and replace - just add the updated module normally
                expect(mockRemoveModuleFromTemplate).not.toHaveBeenCalled();

                // Should use normal addModule flow (not replacement flow)
                expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(
                    mockGlobalTemplate,
                    {
                        urn: updatedModuleUrn,
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Updated Global Module Name',
                            type: DataHubPageModuleType.Link,
                            visibility: { scope: PageModuleScope.Global },
                            params: { newParam: 'updatedValue' },
                        },
                    },
                    position,
                    true, // isEditingModule = true (normal editing flow)
                );

                // Should update the global template
                expect(mockSetGlobalTemplate).toHaveBeenCalledWith(templateAfterUpdate);
                expect(mockUpsertTemplate).toHaveBeenCalledWith(templateAfterUpdate, false, mockPersonalTemplate);
            });

            it('should edit personal module in place when editing personal template', async () => {
                const { result } = renderHook(() =>
                    useModuleOperations(
                        false, // isEditingGlobalTemplate = false (editing personal template)
                        mockPersonalTemplate,
                        mockGlobalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        true, // isEditingModule = true
                        mockPersonalModuleToEdit, // originalModuleData = personal module
                    ),
                );

                const position: ModulePositionInput = {
                    rowIndex: 0,
                    moduleIndex: 0,
                };

                const upsertModuleInput = {
                    urn: mockPersonalModuleToEdit.urn,
                    name: 'Updated Personal Module Name',
                    type: DataHubPageModuleType.Link,
                    position,
                    params: { newParam: 'updatedValue' },
                };

                const updatedModuleUrn = mockPersonalModuleToEdit.urn; // Same urn (editing in place)
                mockUpsertPageModuleMutation.mockResolvedValue({
                    data: {
                        upsertPageModule: {
                            urn: updatedModuleUrn,
                        },
                    },
                });

                const templateAfterUpdate = {
                    ...mockPersonalTemplate,
                    // Template updated with modified module
                };

                mockUpdateTemplateWithModule.mockReturnValue(templateAfterUpdate);
                mockUpsertTemplate.mockResolvedValue({});

                await act(async () => {
                    result.current.upsertModule(upsertModuleInput);
                });

                // Should edit the existing personal module (pass the original urn)
                expect(mockUpsertPageModuleMutation).toHaveBeenCalledWith({
                    variables: {
                        input: {
                            name: 'Updated Personal Module Name',
                            type: DataHubPageModuleType.Link,
                            scope: PageModuleScope.Personal, // Should keep personal scope
                            params: { newParam: 'updatedValue' },
                            urn: mockPersonalModuleToEdit.urn, // Should pass original urn (edit in place)
                        },
                    },
                });

                // Should NOT use replacement flow for personal modules
                expect(mockRemoveModuleFromTemplate).not.toHaveBeenCalled();

                // Should use normal addModule flow (not replacement flow)
                expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(
                    mockPersonalTemplate,
                    {
                        urn: updatedModuleUrn,
                        type: EntityType.DatahubPageModule,
                        properties: {
                            name: 'Updated Personal Module Name',
                            type: DataHubPageModuleType.Link,
                            visibility: { scope: PageModuleScope.Personal },
                            params: { newParam: 'updatedValue' },
                        },
                    },
                    position,
                    true, // isEditingModule = true (normal editing flow)
                );

                // Should update the personal template
                expect(mockSetPersonalTemplate).toHaveBeenCalledWith(templateAfterUpdate);
                expect(mockUpsertTemplate).toHaveBeenCalledWith(templateAfterUpdate, true, mockPersonalTemplate);
            });

            it('should handle error when replacement flow fails during global module edit', async () => {
                const { result } = renderHook(() =>
                    useModuleOperations(
                        false, // isEditingGlobalTemplate = false
                        mockPersonalTemplate,
                        mockGlobalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        true, // isEditingModule = true
                        mockGlobalModuleToEdit, // originalModuleData = global module
                    ),
                );

                const position: ModulePositionInput = {
                    rowIndex: 0,
                    moduleIndex: 0,
                };

                const upsertModuleInput = {
                    urn: mockGlobalModuleToEdit.urn,
                    name: 'Updated Global Module Name',
                    type: DataHubPageModuleType.Link,
                    position,
                };

                const newPersonalModuleUrn = 'urn:li:pageModule:newPersonal';
                mockUpsertPageModuleMutation.mockResolvedValue({
                    data: {
                        upsertPageModule: {
                            urn: newPersonalModuleUrn,
                        },
                    },
                });

                // Mock template update failure
                mockRemoveModuleFromTemplate.mockReturnValue(null);

                const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

                await act(async () => {
                    result.current.upsertModule(upsertModuleInput);
                });

                // Should still create the new module
                expect(mockUpsertPageModuleMutation).toHaveBeenCalled();

                // Should attempt to remove the original module
                expect(mockRemoveModuleFromTemplate).toHaveBeenCalledWith(
                    mockPersonalTemplate,
                    mockGlobalModuleToEdit.urn,
                    position,
                    false,
                );

                // Should not proceed with template update if removal fails
                expect(mockUpdateTemplateWithModule).not.toHaveBeenCalled();
                expect(mockSetPersonalTemplate).not.toHaveBeenCalled();
                expect(mockUpsertTemplate).not.toHaveBeenCalled();

                consoleSpy.mockRestore();
            });
        });
    });

    describe('moveModule with 3-module row constraints', () => {
        const templateWith3ModulesInFirstRow: PageTemplateFragment = {
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
                                    type: DataHubPageModuleType.OwnedAssets,
                                    visibility: { scope: PageModuleScope.Personal },
                                    params: {},
                                },
                            },
                            {
                                urn: 'urn:li:pageModule:2',
                                type: EntityType.DatahubPageModule,
                                properties: {
                                    name: 'Module 2',
                                    type: DataHubPageModuleType.Domains,
                                    visibility: { scope: PageModuleScope.Personal },
                                    params: {},
                                },
                            },
                            {
                                urn: 'urn:li:pageModule:3',
                                type: EntityType.DatahubPageModule,
                                properties: {
                                    name: 'Module 3',
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
                                urn: 'urn:li:pageModule:4',
                                type: EntityType.DatahubPageModule,
                                properties: {
                                    name: 'Module 4',
                                    type: DataHubPageModuleType.OwnedAssets,
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

        beforeEach(() => {
            vi.clearAllMocks();
        });

        it('should allow rearranging modules within a row that has 3 modules', () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    null, // personalTemplate
                    templateWith3ModulesInFirstRow, // globalTemplate
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            act(() => {
                // Move the first module to the third position within the same row
                result.current.moveModule({
                    module: templateWith3ModulesInFirstRow.properties.rows[0].modules[0],
                    fromPosition: { rowIndex: 0, moduleIndex: 0 },
                    toPosition: { rowIndex: 0, moduleIndex: 2 },
                });
            });

            expect(mockUpsertTemplate).toHaveBeenCalled();
            expect(mockSetPersonalTemplate).toHaveBeenCalled();
        });

        it('should allow moving a module from another row to a full row, creating new row', () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    null, // personalTemplate
                    templateWith3ModulesInFirstRow, // globalTemplate
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            act(() => {
                // Move module from row 1 to row 0 (which has 3 modules)
                result.current.moveModule({
                    module: templateWith3ModulesInFirstRow.properties.rows[1].modules[0],
                    fromPosition: { rowIndex: 1, moduleIndex: 0 },
                    toPosition: { rowIndex: 0, moduleIndex: 3 },
                });
            });

            expect(mockUpsertTemplate).toHaveBeenCalled();
            expect(mockSetPersonalTemplate).toHaveBeenCalled();
        });
        it('should allow moving a module from a row with 3 modules to another row', () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    null, // personalTemplate
                    templateWith3ModulesInFirstRow, // globalTemplate
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockRemoveModuleFromTemplate,
                    mockUpsertTemplate,
                    false, // isEditingModule
                    null, // originalModuleData
                ),
            );

            act(() => {
                // Move module from row 0 (which has 3 modules) to row 1
                result.current.moveModule({
                    module: templateWith3ModulesInFirstRow.properties.rows[0].modules[0],
                    fromPosition: { rowIndex: 0, moduleIndex: 0 },
                    toPosition: { rowIndex: 1, moduleIndex: 1 },
                });
            });

            expect(mockUpsertTemplate).toHaveBeenCalled();
            expect(mockSetPersonalTemplate).toHaveBeenCalled();
        });
    });

    describe('hook dependencies', () => {
        it('should update addModule when dependencies change', () => {
            const { result, rerender } = renderHook(
                ({ isEditingGlobalTemplate, personalTemplate, globalTemplate }) =>
                    useModuleOperations(
                        isEditingGlobalTemplate,
                        personalTemplate,
                        globalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        false,
                        null, // originalModuleData
                    ),
                {
                    initialProps: {
                        isEditingGlobalTemplate: false,
                        personalTemplate: mockPersonalTemplate,
                        globalTemplate: mockGlobalTemplate,
                    },
                },
            );

            const initialAddModule = result.current.addModule;

            rerender({
                isEditingGlobalTemplate: true,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
            });

            expect(result.current.addModule).not.toBe(initialAddModule);
        });

        it('should update upsertModule when dependencies change', () => {
            const { result, rerender } = renderHook(
                ({ isEditingGlobalTemplate, personalTemplate, globalTemplate }) =>
                    useModuleOperations(
                        isEditingGlobalTemplate,
                        personalTemplate,
                        globalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockRemoveModuleFromTemplate,
                        mockUpsertTemplate,
                        false,
                        null, // originalModuleData
                    ),
                {
                    initialProps: {
                        isEditingGlobalTemplate: false,
                        personalTemplate: mockPersonalTemplate,
                        globalTemplate: mockGlobalTemplate,
                    },
                },
            );

            const initialUpsertModule = result.current.upsertModule;

            rerender({
                isEditingGlobalTemplate: true,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
            });

            expect(result.current.upsertModule).not.toBe(initialUpsertModule);
        });
    });
});
