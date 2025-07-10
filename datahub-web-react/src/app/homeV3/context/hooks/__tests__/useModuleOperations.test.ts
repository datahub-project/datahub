import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useModuleOperations } from '@app/homeV3/context/hooks/useModuleOperations';
import { ModulePositionInput } from '@app/homeV3/template/types';

import { PageModuleFragment, PageTemplateFragment, useUpsertPageModuleMutation } from '@graphql/template.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope, PageTemplateScope, PageTemplateSurfaceType } from '@types';

// Mock GraphQL hooks
vi.mock('@graphql/template.generated');

const mockUpsertPageModuleMutation = vi.fn();

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
const mockUpsertTemplate = vi.fn();

describe('useModuleOperations', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        (useUpsertPageModuleMutation as any).mockReturnValue([mockUpsertPageModuleMutation]);
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
                    mockUpsertTemplate,
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

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(mockPersonalTemplate, mockModule, position);
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
                    mockUpsertTemplate,
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

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(mockGlobalTemplate, mockModule, position);
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
                    mockUpsertTemplate,
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

            expect(mockUpdateTemplateWithModule).toHaveBeenCalledWith(mockGlobalTemplate, mockModule, position);
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
                    mockUpsertTemplate,
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
            const error = new Error('Template upsert failed');
            mockUpsertTemplate.mockRejectedValue(error);

            const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

            await act(async () => {
                try {
                    result.current.addModule({
                        module: mockModule,
                        position,
                    });
                } catch (e) {
                    // Expected to throw
                }
            });

            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, true, mockPersonalTemplate);
            expect(mockSetPersonalTemplate).toHaveBeenCalledWith(mockPersonalTemplate); // Revert call
            expect(consoleSpy).toHaveBeenCalledWith('Failed to update template:', error);

            consoleSpy.mockRestore();
        });
    });

    describe('createModule', () => {
        it('should create module and add it to template', async () => {
            const { result } = renderHook(() =>
                useModuleOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpdateTemplateWithModule,
                    mockUpsertTemplate,
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const createModuleInput = {
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
                result.current.createModule(createModuleInput);
            });

            expect(mockUpsertPageModuleMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        name: 'Test Module',
                        type: DataHubPageModuleType.Link,
                        scope: PageModuleScope.Personal,
                        visibility: {
                            scope: PageModuleScope.Personal,
                        },
                        params: { limit: 10 },
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
                    mockUpsertTemplate,
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: undefined,
                rowSide: 'right',
            };

            const createModuleInput = {
                name: 'Test Module',
                type: DataHubPageModuleType.Link,
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
                result.current.createModule(createModuleInput);
            });

            expect(mockUpsertPageModuleMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        name: 'Test Module',
                        type: DataHubPageModuleType.Link,
                        scope: PageModuleScope.Personal, // Default scope
                        visibility: {
                            scope: PageModuleScope.Personal,
                        },
                        params: {}, // Default empty params
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
                    mockUpsertTemplate,
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const createModuleInput = {
                name: 'Test Module',
                type: DataHubPageModuleType.Link,
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
                result.current.createModule(createModuleInput);
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
                    mockUpsertTemplate,
                ),
            );

            const position: ModulePositionInput = {
                rowIndex: 0,
                rowSide: 'left',
            };

            const createModuleInput = {
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
                result.current.createModule(createModuleInput);
            });

            expect(mockUpsertPageModuleMutation).toHaveBeenCalledWith({
                variables: {
                    input: {
                        name: 'Global Module',
                        type: DataHubPageModuleType.Link,
                        scope: PageModuleScope.Global,
                        visibility: {
                            scope: PageModuleScope.Global,
                        },
                        params: {},
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
            );

            expect(mockSetGlobalTemplate).toHaveBeenCalledWith(updatedTemplate);
            expect(mockUpsertTemplate).toHaveBeenCalledWith(updatedTemplate, false, mockPersonalTemplate);
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
                        mockUpsertTemplate,
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

        it('should update createModule when dependencies change', () => {
            const { result, rerender } = renderHook(
                ({ isEditingGlobalTemplate, personalTemplate, globalTemplate }) =>
                    useModuleOperations(
                        isEditingGlobalTemplate,
                        personalTemplate,
                        globalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
                        mockUpdateTemplateWithModule,
                        mockUpsertTemplate,
                    ),
                {
                    initialProps: {
                        isEditingGlobalTemplate: false,
                        personalTemplate: mockPersonalTemplate,
                        globalTemplate: mockGlobalTemplate,
                    },
                },
            );

            const initialCreateModule = result.current.createModule;

            rerender({
                isEditingGlobalTemplate: true,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
            });

            expect(result.current.createModule).not.toBe(initialCreateModule);
        });
    });
});
