import { renderHook } from '@testing-library/react-hooks';

import { pathMatchesExact, pathMatchesInsensitiveToV2 } from '@app/entityV2/dataset/profile/schema/utils/utils';
import useExtractFieldTagsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldTagsInfo';
import { EditableSchemaMetadata, EntityType, SchemaField, SchemaFieldDataType, Tag } from '@src/types.generated';

describe('useExtractFieldTagsInfo', () => {
    const testTag: Tag = {
        urn: 'urn:testField',
        type: EntityType.Tag,
        name: 'testTagName',
        properties: {
            name: 'testTagName',
        },
    };

    const extraTag: Tag = {
        urn: 'urn:extraField',
        type: EntityType.Tag,
        name: 'extraTagName',
        properties: {
            name: 'extraTagName',
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

    const editableSchemaMetadataWithExtraTags: EditableSchemaMetadata = {
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
            {
                fieldPath: '[version=2.0].[type=record].testField',
                globalTags: {
                    tags: [
                        {
                            associatedUrn: 'urn:li:globalTags:test.extraTagName',
                            tag: extraTag,
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

    beforeAll(() => {
        expect(pathMatchesExact('testField', 'testField')).toBe(true);
        expect(pathMatchesExact('testField', '[version=2.0].[type=record].testField')).toBe(false);
        expect(pathMatchesInsensitiveToV2('testField', '[version=2.0].[type=record].testField')).toBe(true);
        expect(pathMatchesInsensitiveToV2('[version=2.0].[type=record].testField', 'testField')).toBe(true);
    });

    afterEach(() => {
        vi.restoreAllMocks();
        vi.resetAllMocks();
    });

    it('should extract uneditableTags when they were provided in SchemaField only', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(filledSchemaField);

        expect(editableTags?.tags).toHaveLength(0);
        expect(uneditableTags?.tags).toHaveLength(1);
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(numberOfTags).toBe(1);
    });

    it('should extract editableTags when they were provided in editableSchemaMetadata only', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(filledEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(uneditableTags?.tags).toHaveLength(0);
        expect(numberOfTags).toBe(1);
    });

    it('should extract all tags when they were provided in both schema and editable metadata, but exclude duplicates', () => {
        mockedUseBaseEntity.mockReturnValue(emptyBaseEntity);
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(filledEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(filledSchemaField);

        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(uneditableTags?.tags).toHaveLength(0);
        expect(numberOfTags).toBe(1);
    });

    it('should not extract any tags when they are not provided', () => {
        mockedUseBaseEntity.mockReturnValue(emptyBaseEntity);
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(editableTags?.tags).toHaveLength(0);
        expect(uneditableTags?.tags).toHaveLength(0);
        expect(numberOfTags).toBe(0);
    });

    it('should extract extra uneditable tags from fields that match the field path insensitive to V2', () => {
        mockedUseBaseEntity.mockReturnValue(emptyBaseEntity);

        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(editableSchemaMetadataWithExtraTags))
            .result.current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');

        expect(uneditableTags?.tags).toHaveLength(1);
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name).toBe('extraTagName');

        expect(numberOfTags).toBe(2);
    });
});
