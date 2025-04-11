import { EditableSchemaMetadata, EntityType, SchemaField, SchemaFieldDataType, Tag } from '@src/types.generated';
import { renderHook } from '@testing-library/react-hooks';
import useExtractFieldTagsInfo from '../useExtractFieldTagsInfo';

describe('useExtractFieldTagsInfo', () => {
    const testTag: Tag = {
        urn: 'urn:testField',
        type: EntityType.Tag,
        name: 'testTagName',
        properties: {
            name: 'testTagName',
        },
    };
    const emptyEditableSchemaMetadata: EditableSchemaMetadata = { editableSchemaFieldInfo: [] };

    const filledEditableSchemaMetadata: EditableSchemaMetadata = {
        editableSchemaFieldInfo: [
            {
                fieldPath: 'testField',
                globalTags: {
                    tags: [
                        {
                            associatedUrn: 'urn:li:globalTags:test.testTagName',
                            tag: testTag,
                        },
                    ],
                },
            },
        ],
    };

    const emptySchemaField: SchemaField = {
        fieldPath: 'testField',
        nullable: true,
        recursive: false,
        type: SchemaFieldDataType.String,
    };

    const filledSchemaField: SchemaField = {
        fieldPath: 'testField',
        nullable: true,
        recursive: false,
        type: SchemaFieldDataType.String,
        globalTags: {
            tags: [
                {
                    associatedUrn: 'urn:li:globalTag:test.testTagName',
                    tag: testTag,
                },
            ],
        },
    };

    const emptyBaseEntity = {};

    const { mockedUseBaseEntity } = vi.hoisted(() => {
        return { mockedUseBaseEntity: vi.fn() };
    });

    vi.mock('@src/app/entity/shared/EntityContext', async (importOriginal) => {
        const original = await importOriginal<object>();
        return {
            ...original,
            useBaseEntity: vi.fn(() => mockedUseBaseEntity()),
        };
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('should extract uneditableTags when they were provided in SchemaFild only', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(filledSchemaField);

        expect(editableTags).toBeUndefined();
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name === 'testTagName').toBeTruthy();
        expect(numberOfTags).toBe(1);
    });

    it('should extract editableTags when they were provided in editableSchemaMetadata only', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(filledEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(editableTags?.tags?.[0]?.tag?.properties?.name === 'testTagName').toBeTruthy();
        expect(uneditableTags).toBeUndefined();
        expect(numberOfTags).toBe(1);
    });

    it('should extract all tags when they were provided', () => {
        mockedUseBaseEntity.mockReturnValue(emptyBaseEntity);
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(filledEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(filledSchemaField);

        expect(editableTags?.tags?.[0]?.tag?.properties?.name === 'testTagName').toBeTruthy();
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name === 'testTagName').toBeTruthy();
        expect(numberOfTags).toBe(2);
    });

    it('should not extract any tags when they are not provided', () => {
        mockedUseBaseEntity.mockReturnValue(emptyBaseEntity);
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(editableTags).toBeUndefined();
        expect(uneditableTags).toBeUndefined();
        expect(numberOfTags).toBe(0);
    });
});
