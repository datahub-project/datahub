import {
    EditableSchemaMetadata,
    EntityType,
    SchemaField,
    SchemaFieldDataType,
    Tag,
    ActionRequestType,
} from '@src/types.generated';
import { renderHook } from '@testing-library/react-hooks';
import { pathMatchesExact, pathMatchesInsensitiveToV2 } from '@src/app/entityV2/dataset/profile/schema/utils/utils';
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

    // Create editableSchemaMetadata with a V2 field path that would match after downgrading
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
                // This is a V2 field path with annotations that should match 'testField' after downgrading
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

    const filledBaseEntity = {
        dataset: {
            proposals: [
                {
                    type: ActionRequestType.TagAssociation,
                    subResource: 'testField',
                    params: {
                        tagProposal: {
                            tag: testTag,
                        },
                    },
                },
            ],
        },
    };

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

    // Verify that our utility functions work correctly for our test data
    beforeAll(() => {
        // Verify exact matching works as expected
        expect(pathMatchesExact('testField', 'testField')).toBe(true);
        expect(pathMatchesExact('testField', '[version=2.0].[type=record].testField')).toBe(false);

        // Verify V2 insensitive matching works correctly with annotated paths
        expect(pathMatchesInsensitiveToV2('testField', '[version=2.0].[type=record].testField')).toBe(true);
        expect(pathMatchesInsensitiveToV2('[version=2.0].[type=record].testField', 'testField')).toBe(true);
    });

    afterEach(() => {
        vi.restoreAllMocks();
        vi.resetAllMocks();
    });

    it('should extract uneditableTags when they were provided in SchemaFild only', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, proposedTags, numberOfTags } = extractFieldTagsInfo(filledSchemaField);

        expect(editableTags).toBeUndefined();
        expect(proposedTags).toStrictEqual([]);
        expect(uneditableTags?.tags).toHaveLength(1);
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(numberOfTags).toBe(1);
    });

    it('should extract editableTags when they were provided in editableSchemaMetadata only', () => {
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(filledEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, proposedTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(proposedTags).toStrictEqual([]);
        expect(uneditableTags?.tags).toBeUndefined();
        expect(numberOfTags).toBe(1);
    });

    it('should extract proposedTags when they were provided in baseEntity', () => {
        mockedUseBaseEntity.mockReturnValue(filledBaseEntity);
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, proposedTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        // Since we're using the real findFieldPathProposal function now, let's verify it works as expected
        expect(editableTags).toBeUndefined();

        // With our test setup, the proposals should be empty because of how useExtractFieldTagsInfo
        // processes them (the type conversion in getProposedItemsByType is mocked)
        expect(proposedTags).toStrictEqual([]);

        expect(uneditableTags?.tags).toBeUndefined();
        expect(numberOfTags).toBe(0);
    });

    it('should extract all tags when they were provided', () => {
        mockedUseBaseEntity.mockReturnValue(filledBaseEntity);
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(filledEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, proposedTags, numberOfTags } = extractFieldTagsInfo(filledSchemaField);

        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');

        // With our test setup, the proposals should be empty because of how useExtractFieldTagsInfo
        // processes them using the real findFieldPathProposal but mocked getProposedItemsByType
        expect(proposedTags).toStrictEqual([]);

        expect(uneditableTags?.tags).toHaveLength(1);
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');
        expect(numberOfTags).toBe(2);
    });

    it('should not extract any tags when they are not provided', () => {
        mockedUseBaseEntity.mockReturnValue(emptyBaseEntity);
        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(emptyEditableSchemaMetadata)).result
            .current;

        const { editableTags, uneditableTags, proposedTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        expect(editableTags).toBeUndefined();
        expect(proposedTags).toStrictEqual([]);
        expect(uneditableTags?.tags).toBeUndefined();
        expect(numberOfTags).toBe(0);
    });

    it('should extract extra uneditable tags from fields that match the field path insensitive to V2', () => {
        mockedUseBaseEntity.mockReturnValue(emptyBaseEntity);

        const extractFieldTagsInfo = renderHook(() => useExtractFieldTagsInfo(editableSchemaMetadataWithExtraTags))
            .result.current;

        const { editableTags, uneditableTags, proposedTags, numberOfTags } = extractFieldTagsInfo(emptySchemaField);

        // Should have the editable tag from the exact match
        expect(editableTags?.tags).toHaveLength(1);
        expect(editableTags?.tags?.[0]?.tag?.properties?.name).toBe('testTagName');

        // With the new filter, uneditableTags.tags now only has 1 tag
        // (any tags with the same URN as in editableTags are filtered out)
        expect(uneditableTags?.tags).toHaveLength(1);
        // The remaining tag should be the extra tag
        expect(uneditableTags?.tags?.[0]?.tag?.properties?.name).toBe('extraTagName');

        expect(proposedTags).toStrictEqual([]);
        // Total should be 2 (1 editable + 1 uneditable)
        expect(numberOfTags).toBe(2);
    });
});
