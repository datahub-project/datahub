import { act, renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import {
    AddSummaryElementInput,
    ReplaceSummaryElementInput,
    useAssetSummaryOperations,
} from '@app/homeV3/context/hooks/useAssetSummaryOperations';
import {
    getTemplateToUpdate,
    handleValidationError,
    persistTemplateChanges,
    updateTemplateStateOptimistically,
    validateTemplateAvailability,
} from '@app/homeV3/context/hooks/utils/templateOperationUtils';
import {
    validateArrayBounds,
    validateElementType,
    validatePosition,
    validateStructuredProperty,
} from '@app/homeV3/context/hooks/utils/validationUtils';

import { StructuredPropertyFieldsFragment } from '@graphql/fragments.generated';
import { PageTemplateFragment } from '@graphql/template.generated';
import { EntityType, PageTemplateScope, PageTemplateSurfaceType, SummaryElementType } from '@types';

// Mock antd message
vi.mock('antd', () => ({
    message: {
        error: vi.fn(() => ({ key: 'test-message' })),
    },
}));

// Mock template operation utils
vi.mock('@app/homeV3/context/hooks/utils/templateOperationUtils', () => ({
    getTemplateToUpdate: vi.fn(),
    updateTemplateStateOptimistically: vi.fn(),
    persistTemplateChanges: vi.fn(),
    handleValidationError: vi.fn(),
    validateTemplateAvailability: vi.fn(),
}));

// Mock validation utils
vi.mock('@app/homeV3/context/hooks/utils/validationUtils', () => ({
    validatePosition: vi.fn(),
    validateArrayBounds: vi.fn(),
    validateElementType: vi.fn(),
    validateStructuredProperty: vi.fn(),
}));

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
                            type: 'LINK' as any,
                            visibility: { scope: 'PERSONAL' as any },
                            params: {},
                        },
                    },
                ],
            },
        ],
        surface: { surfaceType: PageTemplateSurfaceType.AssetSummary },
        visibility: { scope: PageTemplateScope.Personal },
        assetSummary: {
            summaryElements: [
                {
                    __typename: 'SummaryElement',
                    elementType: SummaryElementType.Created,
                },
                {
                    __typename: 'SummaryElement',
                    elementType: SummaryElementType.Owners,
                },
            ],
        },
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
                            type: 'LINK' as any,
                            visibility: { scope: 'GLOBAL' as any },
                            params: {},
                        },
                    },
                ],
            },
        ],
        surface: { surfaceType: PageTemplateSurfaceType.AssetSummary },
        visibility: { scope: PageTemplateScope.Global },
        assetSummary: {
            summaryElements: [
                {
                    __typename: 'SummaryElement',
                    elementType: SummaryElementType.Domain,
                },
            ],
        },
    },
};

const mockStructuredProperty: StructuredPropertyFieldsFragment = {
    urn: 'urn:li:structuredProperty:test',
    type: EntityType.StructuredProperty,
    definition: {
        qualifiedName: 'test.property',
        displayName: 'Test Property',
        description: 'Test structured property',
        valueType: 'STRING' as any,
        allowedValues: [],
        cardinality: 'SINGLE' as any,
        entityTypes: [],
        typeQualifier: {},
    } as any,
};

// Mock functions
const mockSetPersonalTemplate = vi.fn();
const mockSetGlobalTemplate = vi.fn();
const mockUpsertTemplate = vi.fn();

describe('useAssetSummaryOperations', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        // Setup default mock implementations
        (getTemplateToUpdate as any).mockReturnValue({
            template: mockPersonalTemplate,
            isPersonal: true,
        });
        (validateTemplateAvailability as any).mockReturnValue(true);
        (handleValidationError as any).mockReturnValue(false);
        (validateElementType as any).mockReturnValue(null);
        (validateStructuredProperty as any).mockReturnValue(null);
        (validatePosition as any).mockReturnValue(null);
        (validateArrayBounds as any).mockReturnValue(null);
        (persistTemplateChanges as any).mockResolvedValue(undefined);
    });

    describe('addSummaryElement', () => {
        it('should add a basic summary element to personal template', () => {
            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: AddSummaryElementInput = {
                elementType: SummaryElementType.Tags,
            };

            act(() => {
                result.current.addSummaryElement(input);
            });

            expect(validateElementType).toHaveBeenCalledWith(SummaryElementType.Tags);
            expect(validateStructuredProperty).toHaveBeenCalledWith(SummaryElementType.Tags, undefined);
            expect(getTemplateToUpdate).toHaveBeenCalled();
            expect(validateTemplateAvailability).toHaveBeenCalledWith(mockPersonalTemplate);
            expect(updateTemplateStateOptimistically).toHaveBeenCalled();
            expect(persistTemplateChanges).toHaveBeenCalledWith(
                expect.any(Object),
                expect.objectContaining({
                    properties: expect.objectContaining({
                        assetSummary: expect.objectContaining({
                            summaryElements: expect.arrayContaining([
                                expect.objectContaining({
                                    elementType: SummaryElementType.Tags,
                                }),
                            ]),
                        }),
                    }),
                }),
                true,
                'add summary element',
            );
        });

        it('should add structured property summary element', () => {
            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: AddSummaryElementInput = {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: mockStructuredProperty,
            };

            act(() => {
                result.current.addSummaryElement(input);
            });

            expect(validateElementType).toHaveBeenCalledWith(SummaryElementType.StructuredProperty);
            expect(validateStructuredProperty).toHaveBeenCalledWith(
                SummaryElementType.StructuredProperty,
                mockStructuredProperty.urn,
            );
            expect(persistTemplateChanges).toHaveBeenCalledWith(
                expect.any(Object),
                expect.objectContaining({
                    properties: expect.objectContaining({
                        assetSummary: expect.objectContaining({
                            summaryElements: expect.arrayContaining([
                                expect.objectContaining({
                                    elementType: SummaryElementType.StructuredProperty,
                                    structuredProperty: mockStructuredProperty,
                                }),
                            ]),
                        }),
                    }),
                }),
                true,
                'add summary element',
            );
        });

        it('should add to global template when editing global', () => {
            (getTemplateToUpdate as any).mockReturnValue({
                template: mockGlobalTemplate,
                isPersonal: false,
            });

            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    true, // isEditingGlobalTemplate
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: AddSummaryElementInput = {
                elementType: SummaryElementType.GlossaryTerms,
            };

            act(() => {
                result.current.addSummaryElement(input);
            });

            expect(getTemplateToUpdate).toHaveBeenCalled();
            expect(validateTemplateAvailability).toHaveBeenCalledWith(mockGlobalTemplate);
            expect(persistTemplateChanges).toHaveBeenCalledWith(
                expect.any(Object),
                expect.any(Object),
                false, // isPersonal = false
                'add summary element',
            );
        });

        it('should handle validation error and not proceed', () => {
            (handleValidationError as any).mockReturnValue(true); // Validation failed

            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: AddSummaryElementInput = {
                elementType: SummaryElementType.Tags,
            };

            act(() => {
                result.current.addSummaryElement(input);
            });

            expect(handleValidationError).toHaveBeenCalledWith(null, 'addSummaryElement');
            expect(getTemplateToUpdate).not.toHaveBeenCalled();
            expect(persistTemplateChanges).not.toHaveBeenCalled();
        });

        it('should handle template unavailability and not proceed', () => {
            (validateTemplateAvailability as any).mockReturnValue(false);

            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: AddSummaryElementInput = {
                elementType: SummaryElementType.Tags,
            };

            act(() => {
                result.current.addSummaryElement(input);
            });

            expect(validateTemplateAvailability).toHaveBeenCalled();
            expect(updateTemplateStateOptimistically).not.toHaveBeenCalled();
            expect(persistTemplateChanges).not.toHaveBeenCalled();
        });
    });

    describe('removeSummaryElement', () => {
        it('should remove summary element at specified position', () => {
            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const position = 1;

            act(() => {
                result.current.removeSummaryElement(position, SummaryElementType.Created);
            });

            expect(validatePosition).toHaveBeenCalledWith(position, 'remove summary element');
            expect(validateArrayBounds).toHaveBeenCalledWith(position, 2, 'removal'); // 2 elements in mock template
            expect(persistTemplateChanges).toHaveBeenCalledWith(
                expect.any(Object),
                expect.objectContaining({
                    properties: expect.objectContaining({
                        assetSummary: expect.objectContaining({
                            summaryElements: expect.arrayContaining([
                                expect.objectContaining({
                                    elementType: SummaryElementType.Created,
                                }),
                            ]),
                        }),
                    }),
                }),
                true,
                'remove summary element',
            );
        });

        it('should remove first element correctly', () => {
            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            act(() => {
                result.current.removeSummaryElement(0, SummaryElementType.Created);
            });

            expect(validatePosition).toHaveBeenCalledWith(0, 'remove summary element');
            expect(persistTemplateChanges).toHaveBeenCalledWith(
                expect.any(Object),
                expect.objectContaining({
                    properties: expect.objectContaining({
                        assetSummary: expect.objectContaining({
                            summaryElements: expect.arrayContaining([
                                expect.objectContaining({
                                    elementType: SummaryElementType.Owners,
                                }),
                            ]),
                        }),
                    }),
                }),
                true,
                'remove summary element',
            );
        });

        it('should handle validation error and not proceed', () => {
            (handleValidationError as any).mockReturnValue(true);

            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            act(() => {
                result.current.removeSummaryElement(0, SummaryElementType.Created);
            });

            expect(handleValidationError).toHaveBeenCalled();
            expect(getTemplateToUpdate).not.toHaveBeenCalled();
            expect(persistTemplateChanges).not.toHaveBeenCalled();
        });

        it('should handle array bounds error and not proceed', () => {
            (validateArrayBounds as any).mockReturnValue('Position is out of bounds');
            (handleValidationError as any).mockImplementation((error) => error !== null);

            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            act(() => {
                result.current.removeSummaryElement(10, SummaryElementType.Created);
            });

            expect(validateArrayBounds).toHaveBeenCalledWith(10, 2, 'removal');
            expect(handleValidationError).toHaveBeenCalledWith('Position is out of bounds', 'removeSummaryElement');
            expect(persistTemplateChanges).not.toHaveBeenCalled();
        });
    });

    describe('replaceSummaryElement', () => {
        it('should replace summary element at specified position', () => {
            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: ReplaceSummaryElementInput = {
                elementType: SummaryElementType.Tags,
                position: 0,
                currentElementType: SummaryElementType.Created,
            };

            act(() => {
                result.current.replaceSummaryElement(input);
            });

            expect(validateElementType).toHaveBeenCalledWith(SummaryElementType.Tags);
            expect(validateStructuredProperty).toHaveBeenCalledWith(SummaryElementType.Tags, undefined);
            expect(validatePosition).toHaveBeenCalledWith(0, 'replace summary element');
            expect(validateArrayBounds).toHaveBeenCalledWith(0, 2, 'replacement');
            expect(persistTemplateChanges).toHaveBeenCalledWith(
                expect.any(Object),
                expect.objectContaining({
                    properties: expect.objectContaining({
                        assetSummary: expect.objectContaining({
                            summaryElements: expect.arrayContaining([
                                expect.objectContaining({
                                    elementType: SummaryElementType.Tags,
                                }),
                                expect.objectContaining({
                                    elementType: SummaryElementType.Owners,
                                }),
                            ]),
                        }),
                    }),
                }),
                true,
                'replace summary element',
            );
        });

        it('should replace with structured property element', () => {
            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: ReplaceSummaryElementInput = {
                elementType: SummaryElementType.StructuredProperty,
                structuredProperty: mockStructuredProperty,
                position: 1,
                currentElementType: SummaryElementType.Created,
            };

            act(() => {
                result.current.replaceSummaryElement(input);
            });

            expect(validateStructuredProperty).toHaveBeenCalledWith(
                SummaryElementType.StructuredProperty,
                mockStructuredProperty.urn,
            );
            expect(persistTemplateChanges).toHaveBeenCalledWith(
                expect.any(Object),
                expect.objectContaining({
                    properties: expect.objectContaining({
                        assetSummary: expect.objectContaining({
                            summaryElements: expect.arrayContaining([
                                expect.objectContaining({
                                    elementType: SummaryElementType.Created,
                                }),
                                expect.objectContaining({
                                    elementType: SummaryElementType.StructuredProperty,
                                    structuredProperty: mockStructuredProperty,
                                }),
                            ]),
                        }),
                    }),
                }),
                true,
                'replace summary element',
            );
        });

        it('should handle validation error and not proceed', () => {
            (validateElementType as any).mockReturnValue('Element type is required');
            (handleValidationError as any).mockImplementation((error) => error !== null);

            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    mockPersonalTemplate,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: ReplaceSummaryElementInput = {
                elementType: null as any,
                position: 0,
                currentElementType: SummaryElementType.Created,
            };

            act(() => {
                result.current.replaceSummaryElement(input);
            });

            expect(handleValidationError).toHaveBeenCalledWith('Element type is required', 'replaceSummaryElement');
            expect(persistTemplateChanges).not.toHaveBeenCalled();
        });
    });

    describe('hook dependencies', () => {
        it('should update functions when dependencies change', () => {
            const { result, rerender } = renderHook(
                ({ isEditingGlobalTemplate, personalTemplate, globalTemplate }) =>
                    useAssetSummaryOperations(
                        isEditingGlobalTemplate,
                        personalTemplate,
                        globalTemplate,
                        mockSetPersonalTemplate,
                        mockSetGlobalTemplate,
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

            const initialAddSummaryElement = result.current.addSummaryElement;

            rerender({
                isEditingGlobalTemplate: true,
                personalTemplate: mockPersonalTemplate,
                globalTemplate: mockGlobalTemplate,
            });

            expect(result.current.addSummaryElement).not.toBe(initialAddSummaryElement);
        });
    });

    describe('edge cases', () => {
        it('should handle template with no existing summary elements', () => {
            const templateWithNoSummary: PageTemplateFragment = {
                ...mockPersonalTemplate,
                properties: {
                    ...mockPersonalTemplate.properties,
                    assetSummary: {
                        summaryElements: [],
                    },
                },
            };

            (getTemplateToUpdate as any).mockReturnValue({
                template: templateWithNoSummary,
                isPersonal: true,
            });

            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    templateWithNoSummary,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: AddSummaryElementInput = {
                elementType: SummaryElementType.Tags,
            };

            act(() => {
                result.current.addSummaryElement(input);
            });

            expect(persistTemplateChanges).toHaveBeenCalledWith(
                expect.any(Object),
                expect.objectContaining({
                    properties: expect.objectContaining({
                        assetSummary: expect.objectContaining({
                            summaryElements: [
                                expect.objectContaining({
                                    elementType: SummaryElementType.Tags,
                                }),
                            ],
                        }),
                    }),
                }),
                true,
                'add summary element',
            );
        });

        it('should handle template with no assetSummary property', () => {
            const templateWithoutAssetSummary: PageTemplateFragment = {
                ...mockPersonalTemplate,
                properties: {
                    ...mockPersonalTemplate.properties,
                    assetSummary: undefined,
                },
            };

            (getTemplateToUpdate as any).mockReturnValue({
                template: templateWithoutAssetSummary,
                isPersonal: true,
            });

            const { result } = renderHook(() =>
                useAssetSummaryOperations(
                    false,
                    templateWithoutAssetSummary,
                    mockGlobalTemplate,
                    mockSetPersonalTemplate,
                    mockSetGlobalTemplate,
                    mockUpsertTemplate,
                ),
            );

            const input: AddSummaryElementInput = {
                elementType: SummaryElementType.Tags,
            };

            act(() => {
                result.current.addSummaryElement(input);
            });

            expect(persistTemplateChanges).toHaveBeenCalledWith(
                expect.any(Object),
                expect.objectContaining({
                    properties: expect.objectContaining({
                        assetSummary: expect.objectContaining({
                            summaryElements: [
                                expect.objectContaining({
                                    elementType: SummaryElementType.Tags,
                                }),
                            ],
                        }),
                    }),
                }),
                true,
                'add summary element',
            );
        });
    });
});
