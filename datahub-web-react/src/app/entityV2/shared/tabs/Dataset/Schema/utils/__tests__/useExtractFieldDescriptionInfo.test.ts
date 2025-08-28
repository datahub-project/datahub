import { renderHook } from '@testing-library/react-hooks';

import useExtractFieldDescriptionInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldDescriptionInfo';

import { EditableSchemaMetadata, EntityType, SchemaField, SchemaFieldDataType } from '@types';

// Mock dependencies using vi.hoisted
const {
    mockGetFieldDescriptionDetails,
    mockSanitizeRichText,
    mockPathMatchesExact,
    mockUseIsDocumentationInferenceEnabled,
} = vi.hoisted(() => {
    return {
        mockGetFieldDescriptionDetails: vi.fn(),
        mockSanitizeRichText: vi.fn(),
        mockPathMatchesExact: vi.fn(),
        mockUseIsDocumentationInferenceEnabled: vi.fn(),
    };
});

vi.mock('../getFieldDescriptionDetails', () => ({
    getFieldDescriptionDetails: mockGetFieldDescriptionDetails,
}));

vi.mock('@app/entityV2/shared/tabs/Documentation/components/editor/utils', () => ({
    sanitizeRichText: mockSanitizeRichText,
}));

vi.mock('@src/app/entityV2/dataset/profile/schema/utils/utils', () => ({
    pathMatchesExact: mockPathMatchesExact,
}));

vi.mock('@src/app/entityV2/shared/components/inferredDocs/utils', () => ({
    useIsDocumentationInferenceEnabled: mockUseIsDocumentationInferenceEnabled,
}));

describe('useExtractFieldDescriptionInfo', () => {
    const mockSchemaField: SchemaField = {
        fieldPath: 'testField',
        nullable: true,
        recursive: false,
        type: SchemaFieldDataType.String,
        description: 'Record description',
    };

    const mockSchemaFieldWithEntity: SchemaField = {
        ...mockSchemaField,
        schemaFieldEntity: {
            urn: 'urn:li:schemaField:test',
            type: EntityType.SchemaField,
            fieldPath: 'testField',
            parent: { urn: 'urn:li:dataset:test', type: EntityType.Dataset },
        },
    };

    const mockEditableSchemaMetadata: EditableSchemaMetadata = {
        editableSchemaFieldInfo: [
            {
                fieldPath: 'testField',
                description: 'Editable description',
            },
            {
                fieldPath: 'otherField',
                description: 'Other editable description',
            },
        ],
    };

    const mockFieldDescriptionDetails = {
        displayedDescription: 'Displayed description',
        isPropagated: false,
        isInferred: false,
        sourceDetail: [{ key: 'test', value: 'value' }],
        attribution: { actor: { urn: 'test' }, time: 100 },
        propagatedDescription: undefined,
        inferredDescription: undefined,
    };

    beforeEach(() => {
        // Reset all mocks
        vi.clearAllMocks();

        // Set up default mock implementations
        mockUseIsDocumentationInferenceEnabled.mockReturnValue(true);
        mockPathMatchesExact.mockReturnValue(false);
        mockGetFieldDescriptionDetails.mockReturnValue(mockFieldDescriptionDetails);
        mockSanitizeRichText.mockImplementation((text) => `sanitized: ${text}`);
    });

    describe('basic functionality', () => {
        it('should return a function when called', () => {
            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            expect(typeof result.current).toBe('function');
        });

        it('should call getFieldDescriptionDetails with correct parameters', () => {
            mockPathMatchesExact.mockReturnValue(true);

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            result.current(mockSchemaFieldWithEntity, 'Parameter description');

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith({
                schemaFieldEntity: mockSchemaFieldWithEntity.schemaFieldEntity,
                editableFieldInfo: mockEditableSchemaMetadata.editableSchemaFieldInfo![0],
                defaultDescription: 'Parameter description',
            });
        });

        it('should sanitize the displayed description', () => {
            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            const extractedInfo = result.current(mockSchemaField);

            expect(mockSanitizeRichText).toHaveBeenCalledWith('Displayed description');
            expect(extractedInfo.sanitizedDescription).toBe('sanitized: Displayed description');
        });

        it('should return all expected properties', () => {
            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            const extractedInfo = result.current(mockSchemaField);

            expect(extractedInfo).toEqual({
                displayedDescription: 'Displayed description',
                sanitizedDescription: 'sanitized: Displayed description',
                isPropagated: false,
                sourceDetail: [{ key: 'test', value: 'value' }],
                attribution: { actor: { urn: 'test' }, time: 100 },
            });
        });
    });

    describe('path matching logic', () => {
        it('should find matching editableFieldInfo when paths match exactly', () => {
            mockPathMatchesExact.mockImplementation((pathA, pathB) => pathA === pathB);

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            result.current(mockSchemaField);

            expect(mockPathMatchesExact).toHaveBeenCalledWith('testField', 'testField');
            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    editableFieldInfo: mockEditableSchemaMetadata.editableSchemaFieldInfo![0],
                }),
            );
        });

        it('should not find editableFieldInfo when no paths match', () => {
            mockPathMatchesExact.mockReturnValue(false);

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            result.current(mockSchemaField);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    editableFieldInfo: undefined,
                }),
            );
        });

        it('should return first matching editableFieldInfo when multiple matches exist', () => {
            const editableSchemaWithDuplicates: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [
                    { fieldPath: 'testField', description: 'First match' },
                    { fieldPath: 'testField', description: 'Second match' },
                ],
            };

            mockPathMatchesExact.mockReturnValue(true);

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(editableSchemaWithDuplicates));

            result.current(mockSchemaField);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    editableFieldInfo: editableSchemaWithDuplicates.editableSchemaFieldInfo![0],
                }),
            );
        });
    });

    describe('description fallback logic', () => {
        it('should prioritize parameter description over record description', () => {
            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            result.current(mockSchemaField, 'Parameter description');

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    defaultDescription: 'Parameter description',
                }),
            );
        });

        it('should use record description when parameter is null', () => {
            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            result.current(mockSchemaField, null);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    defaultDescription: 'Record description',
                }),
            );
        });

        it('should use record description when parameter is undefined', () => {
            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            result.current(mockSchemaField, undefined);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    defaultDescription: 'Record description',
                }),
            );
        });

        it('should fall back to record description when parameter is an empty string', () => {
            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            result.current(mockSchemaField, '');

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    defaultDescription: 'Record description',
                }),
            );
        });

        it('should use record description when parameter is null and record has no description', () => {
            const schemaFieldWithoutDescription = { ...mockSchemaField, description: undefined };

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            result.current(schemaFieldWithoutDescription, null);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    defaultDescription: undefined,
                }),
            );
        });
    });

    describe('edge cases', () => {
        it('should handle null editableSchemaMetadata', () => {
            const { result } = renderHook(() => useExtractFieldDescriptionInfo(null));

            result.current(mockSchemaField);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    editableFieldInfo: undefined,
                }),
            );
        });

        it('should handle undefined editableSchemaMetadata', () => {
            const { result } = renderHook(() => useExtractFieldDescriptionInfo(undefined));

            result.current(mockSchemaField);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    editableFieldInfo: undefined,
                }),
            );
        });

        it('should handle editableSchemaMetadata without editableSchemaFieldInfo', () => {
            const emptyMetadata: any = {};

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(emptyMetadata));

            result.current(mockSchemaField);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    editableFieldInfo: undefined,
                }),
            );
        });

        it('should handle empty editableSchemaFieldInfo array', () => {
            const emptyMetadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [],
            };

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(emptyMetadata));

            result.current(mockSchemaField);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    editableFieldInfo: undefined,
                }),
            );
        });

        it('should handle schema field without schemaFieldEntity', () => {
            const schemaFieldWithoutEntity = { ...mockSchemaField };
            delete schemaFieldWithoutEntity.schemaFieldEntity;

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            result.current(schemaFieldWithoutEntity);

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith(
                expect.objectContaining({
                    schemaFieldEntity: undefined,
                }),
            );
        });

        it('should handle when sanitizeRichText returns empty string', () => {
            mockSanitizeRichText.mockReturnValue('');

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            const extractedInfo = result.current(mockSchemaField);

            expect(extractedInfo.sanitizedDescription).toBe('');
        });

        it('should handle when getFieldDescriptionDetails returns empty description', () => {
            mockGetFieldDescriptionDetails.mockReturnValue({
                ...mockFieldDescriptionDetails,
                displayedDescription: '',
            });

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            const extractedInfo = result.current(mockSchemaField);

            expect(extractedInfo.displayedDescription).toBe('');
            expect(extractedInfo.sanitizedDescription).toBe('sanitized: ');
        });
    });

    describe('return value consistency', () => {
        it('should return a new function reference on re-renders even when inputs do not change', () => {
            const { result, rerender } = renderHook(({ metadata }) => useExtractFieldDescriptionInfo(metadata), {
                initialProps: { metadata: mockEditableSchemaMetadata },
            });

            const firstFunction = result.current;

            rerender({ metadata: mockEditableSchemaMetadata });

            const secondFunction = result.current;

            // React hooks create new function instances on each render
            expect(firstFunction).not.toBe(secondFunction);
        });

        it('should return a new function reference when editableSchemaMetadata changes', () => {
            const { result, rerender } = renderHook(({ metadata }) => useExtractFieldDescriptionInfo(metadata), {
                initialProps: { metadata: mockEditableSchemaMetadata },
            });

            const firstFunction = result.current;

            const newMetadata: EditableSchemaMetadata = {
                editableSchemaFieldInfo: [{ fieldPath: 'newField', description: 'New description' }],
            };

            rerender({ metadata: newMetadata });

            const secondFunction = result.current;

            expect(firstFunction).not.toBe(secondFunction);
        });
    });

    describe('integration behavior', () => {
        it('should work with complex field descriptions containing all possible data', () => {
            const complexFieldDescriptionDetails = {
                displayedDescription: 'Complex description with <em>formatting</em>',
                isPropagated: true,
                isInferred: true,
                sourceDetail: [
                    { key: 'propagated', value: 'true' },
                    { key: 'inferred', value: 'true' },
                ],
                attribution: {
                    actor: { urn: 'urn:li:corpuser:test', type: EntityType.CorpUser },
                    time: 1234567890,
                    sourceDetail: [{ key: 'source', value: 'AI' }],
                },
                propagatedDescription: 'Propagated description',
                inferredDescription: 'Inferred description',
            };

            mockGetFieldDescriptionDetails.mockReturnValue(complexFieldDescriptionDetails);
            mockSanitizeRichText.mockReturnValue('Complex description with formatting');
            mockPathMatchesExact.mockReturnValue(true);

            const { result } = renderHook(() => useExtractFieldDescriptionInfo(mockEditableSchemaMetadata));

            const extractedInfo = result.current(mockSchemaFieldWithEntity, 'Override description');

            expect(extractedInfo).toEqual({
                displayedDescription: 'Complex description with <em>formatting</em>',
                sanitizedDescription: 'Complex description with formatting',
                isPropagated: true,
                sourceDetail: [
                    { key: 'propagated', value: 'true' },
                    { key: 'inferred', value: 'true' },
                ],
                attribution: {
                    actor: { urn: 'urn:li:corpuser:test', type: EntityType.CorpUser },
                    time: 1234567890,
                    sourceDetail: [{ key: 'source', value: 'AI' }],
                },
            });

            expect(mockGetFieldDescriptionDetails).toHaveBeenCalledWith({
                schemaFieldEntity: mockSchemaFieldWithEntity.schemaFieldEntity,
                editableFieldInfo: mockEditableSchemaMetadata.editableSchemaFieldInfo![0],
                defaultDescription: 'Override description',
            });
        });
    });
});
