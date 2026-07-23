import { renderHook } from '@testing-library/react-hooks';

import useExtractFieldTagsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldTagsInfo';
import { pathMatchesExact, pathMatchesInsensitiveToV2 } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
import {
    BusinessAttribute,
    EditableSchemaMetadata,
    EntityType,
    SchemaField,
    SchemaFieldDataType,
    Tag,
} from '@src/types.generated';

const mockUseBaseEntity = vi.hoisted(() => vi.fn());

vi.mock('@src/app/entity/shared/EntityContext', async (importOriginal) => {
    const original = await importOriginal<object>();
    return {
        ...original,
        useBaseEntity: mockUseBaseEntity,
    };
});

describe('useExtractFieldTagsInfo', () => {
    const dummySchemaFieldUrn = 'urn:li:schemaField:testField';

    const testTag: Tag = {
        urn: 'urn:li:tag:testTagName',
        type: EntityType.Tag,
        name: 'testTagName',
        properties: {
            name: 'testTagName',
        },
    };

    const testTag2: Tag = {
        urn: 'urn:li:tag:testTagName2',
        type: EntityType.Tag,
        name: 'testTagName2',
        properties: {
            name: 'testTagName2',
        },
    };

    const testTag3: Tag = {
        urn: 'urn:li:tag:testTagName3',
        type: EntityType.Tag,
        name: 'testTagName3',
        properties: {
            name: 'testTagName3',
        },
    };

    const extraTag: Tag = {
        urn: 'urn:li:tag:extraTagName',
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
                            associatedUrn: dummySchemaFieldUrn,
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
                            associatedUrn: dummySchemaFieldUrn,
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
                            associatedUrn: dummySchemaFieldUrn,
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
        ...emptySchemaField,
        globalTags: {
            tags: [
                {
                    associatedUrn: dummySchemaFieldUrn,
                    tag: testTag,
                },
            ],
        },
    };

    const directSchemaField: SchemaField = {
        ...emptySchemaField,
        schemaFieldEntity: {
            urn: '',
            type: EntityType.SchemaField,
            fieldPath: 'testField',
            parent: { urn: '', type: EntityType.Dataset },
            tags: {
                tags: [
                    {
                        associatedUrn: dummySchemaFieldUrn,
                        tag: testTag,
                    },
                ],
            },
        },
    };

    const complexSchemaField: SchemaField = {
        ...directSchemaField,
        ...filledSchemaField,
    };

    const businessAttributeTag: Tag = {
        urn: 'urn:businessAttributeTag',
        type: EntityType.Tag,
        name: 'businessAttributeTagName',
        properties: {
            name: 'businessAttributeTagName',
        },
    };

    const mockBusinessAttribute: BusinessAttribute = {
        urn: 'urn:li:businessAttribute:testBA',
        type: EntityType.BusinessAttribute,
        properties: {
            name: 'Test Business Attribute',
            description: 'Test description',
            tags: {
                tags: [
                    {
                        associatedUrn: 'urn:li:globalTag:test.businessAttributeTagName',
                        tag: businessAttributeTag,
                    },
                ],
            },
            created: {
                time: Date.now(),
                actor: 'urn:li:corpuser:test',
            },
            lastModified: {
                time: Date.now(),
                actor: 'urn:li:corpuser:test',
            },
        },
    };

    const schemaFieldWithBusinessAttribute: SchemaField = {
        fieldPath: 'testField',
        nullable: true,
        recursive: false,
        type: SchemaFieldDataType.String,
        schemaFieldEntity: {
            urn: 'urn:li:schemaField:testField',
            type: EntityType.SchemaField,
            fieldPath: 'testField',
            parent: {
                urn: 'urn:li:dataset:test',
                type: EntityType.Dataset,
            },
            businessAttributes: {
                businessAttribute: {
                    businessAttribute: mockBusinessAttribute,
                    associatedUrn: 'urn:li:schemaField:testField',
                },
            },
        },
    };

    const emptyBaseEntity = {};

    beforeAll(() => {
        expect(pathMatchesExact('testField', 'testField')).toBe(true);
        expect(pathMatchesExact('testField', '[version=2.0].[type=record].testField')).toBe(false);
        expect(pathMatchesInsensitiveToV2('testField', '[version=2.0].[type=record].testField')).toBe(true);
        expect(pathMatchesInsensitiveToV2('[version=2.0].[type=record].testField', 'testField')).toBe(true);
    });

    beforeEach(() => {
        mockUseBaseEntity.mockReturnValue(emptyBaseEntity);
    });

    afterEach(() => {
        vi.restoreAllMocks();
        vi.resetAllMocks();
    });

    it('should extract uneditableTags when they were provided in SchemaField only', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { directTags, editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(filledSchemaField);

        expect(directTags?.tags).toHaveLength(0);
        expect(editableTags?.tags).toHaveLength(0);
        expect(uneditableTags?.tags).toHaveLength(1);
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(numberOfTags).toBe(1);
    });

    it('should extract uneditableTags when they were provided on Schema Field Entity only', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { directTags, editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(directSchemaField);

        expect(directTags?.tags).toHaveLength(1);
        expect(directTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(editableTags?.tags).toHaveLength(0);
        expect(uneditableTags?.tags).toHaveLength(0);
        expect(numberOfTags).toBe(1);
    });

    it('should extract editableTags when they were provided in editableSchemaMetadata only', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(filledEditableSchemaMetadata)).result
            .current;

        const { directTags, editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(directTags?.tags).toHaveLength(0);
        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(uneditableTags?.tags).toHaveLength(0);
        expect(numberOfTags).toBe(1);
    });

    it('should extract all tags when they were provided in both schema and editable metadata, but exclude duplicates', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(filledEditableSchemaMetadata)).result
            .current;

        const { directTags, editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(filledSchemaField);

        expect(directTags?.tags).toHaveLength(0);
        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(uneditableTags?.tags).toHaveLength(0);
        expect(numberOfTags).toBe(1);
    });

    it('should extract uneditableTags when they were provided everywhere, with duplicates', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(filledEditableSchemaMetadata)).result
            .current;

        const { directTags, editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(complexSchemaField);

        expect(directTags?.tags).toHaveLength(1);
        expect(directTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(editableTags?.tags).toHaveLength(0);
        expect(uneditableTags?.tags).toHaveLength(0);
        expect(numberOfTags).toBe(1);
    });

    it('should extract uneditableTags when they were provided everywhere, no duplicates', () => {
        const editableSchemaMetadata: EditableSchemaMetadata = {
            editableSchemaFieldInfo: [
                {
                    fieldPath: 'testField',
                    globalTags: { tags: [{ tag: testTag2, associatedUrn: dummySchemaFieldUrn }] },
                },
            ],
        };
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(editableSchemaMetadata)).result.current;

        const schemaField: SchemaField = {
            ...directSchemaField,
            globalTags: { tags: [{ tag: testTag3, associatedUrn: dummySchemaFieldUrn }] },
        };
        const { directTags, editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(schemaField);

        expect(directTags?.tags).toHaveLength(1);
        expect(directTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName2');
        expect(uneditableTags?.tags).toHaveLength(1);
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName3');
        expect(numberOfTags).toBe(3);
    });

    it('should not extract any tags when they are not provided', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { directTags, editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(directTags?.tags).toHaveLength(0);
        expect(editableTags?.tags).toHaveLength(0);
        expect(uneditableTags?.tags).toHaveLength(0);
        expect(numberOfTags).toBe(0);
    });

    it('should extract extra uneditable tags from fields that match the field path insensitive to V2', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(editableSchemaMetadataWithExtraTags))
            .result.current;

        const { directTags, editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(directTags?.tags).toHaveLength(0);

        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');

        expect(uneditableTags?.tags).toHaveLength(1);
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name).toBe('extraTagName');

        expect(numberOfTags).toBe(2);
    });

    it('should extract business attribute tags when schema field has business attribute', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(schemaFieldWithBusinessAttribute);

        expect(editableTags?.tags).toHaveLength(0);
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name === 'businessAttributeTagName').toBeTruthy();
        expect(numberOfTags).toBe(1);
    });

    it('should combine field tags and business attribute tags', () => {
        const schemaFieldWithBothTags: SchemaField = {
            ...filledSchemaField,
            schemaFieldEntity: schemaFieldWithBusinessAttribute.schemaFieldEntity,
        };

        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, numberOfTags } = extractFieldTagsInfo(schemaFieldWithBothTags);

        expect(editableTags?.tags).toHaveLength(0);
        expect(uneditableTags?.tags).toHaveLength(2);
        expect(uneditableTags?.tags?.some((tagAssoc) => tagAssoc.tag?.properties?.name === 'testTagName')).toBeTruthy();
        expect(
            uneditableTags?.tags?.some((tagAssoc) => tagAssoc.tag?.properties?.name === 'businessAttributeTagName'),
        ).toBeTruthy();
        expect(numberOfTags).toBe(2);
    });
});
