import { renderHook } from '@testing-library/react-hooks';
import { beforeEach } from 'vitest';

import { pathMatchesExact, pathMatchesInsensitiveToV2 } from '@app/entityV2/dataset/profile/schema/utils/utils';
import useExtractFieldGlossaryTermsInfo from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useExtractFieldGlossaryTermsInfo';
import {
    BusinessAttribute,
    EditableSchemaMetadata,
    EntityType,
    GlossaryTerm,
    SchemaField,
    SchemaFieldDataType,
} from '@src/types.generated';

const mockUseBaseEntity = vi.hoisted(() => vi.fn());

vi.mock('@src/app/entity/shared/EntityContext', async (importOriginal) => {
    const original = await importOriginal<object>();
    return {
        ...original,
        useBaseEntity: mockUseBaseEntity,
    };
});

describe('useExtractFieldTermsInfo', () => {
    const dummySchemaFieldUrn = 'urn:li:schemaField:testField';

    const testTerm: GlossaryTerm = {
        urn: 'urn:li:glossaryTerm:testTermName',
        type: EntityType.GlossaryTerm,
        name: 'testTermName',
        hierarchicalName: 'test.testTermName',
        properties: {
            name: 'testTermName',
            definition: 'test',
            termSource: 'INTERNAL',
        },
    };

    const testTerm2: GlossaryTerm = {
        urn: 'urn:li:glossaryTerm:testTermName2',
        type: EntityType.GlossaryTerm,
        name: 'testTermName2',
        hierarchicalName: 'test.testTermName2',
        properties: {
            name: 'testTermName2',
            definition: 'test',
            termSource: 'INTERNAL',
        },
    };

    const testTerm3: GlossaryTerm = {
        urn: 'urn:li:glossaryTerm:testTermName3',
        type: EntityType.GlossaryTerm,
        name: 'testTermName3',
        hierarchicalName: 'test.testTermName3',
        properties: {
            name: 'testTermName3',
            definition: 'test',
            termSource: 'INTERNAL',
        },
    };

    const extraTerm: GlossaryTerm = {
        urn: 'urn:li:glossaryTerm:extraTermName',
        type: EntityType.GlossaryTerm,
        name: 'extraTermName',
        hierarchicalName: 'test.extraTermName',
        properties: {
            name: 'extraTermName',
            definition: 'test',
            termSource: 'INTERNAL',
        },
    };

    const emptyEditableSchemaMetadata: EditableSchemaMetadata = { editableSchemaFieldInfo: [] };

    const filledEditableSchemaMetadata: EditableSchemaMetadata = {
        editableSchemaFieldInfo: [
            {
                fieldPath: 'testField',
                glossaryTerms: {
                    terms: [
                        {
                            associatedUrn: dummySchemaFieldUrn,
                            term: testTerm,
                        },
                    ],
                },
            },
        ],
    };

    const editableSchemaMetadataWithExtraTerms: EditableSchemaMetadata = {
        editableSchemaFieldInfo: [
            {
                fieldPath: 'testField',
                glossaryTerms: {
                    terms: [
                        {
                            associatedUrn: dummySchemaFieldUrn,
                            term: testTerm,
                        },
                    ],
                },
            },
            {
                fieldPath: '[version=2.0].[type=record].testField',
                glossaryTerms: {
                    terms: [
                        {
                            associatedUrn: dummySchemaFieldUrn,
                            term: extraTerm,
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
        glossaryTerms: {
            terms: [
                {
                    associatedUrn: dummySchemaFieldUrn,
                    term: testTerm,
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
            glossaryTerms: {
                terms: [
                    {
                        associatedUrn: dummySchemaFieldUrn,
                        term: testTerm,
                    },
                ],
            },
        },
    };

    const complexSchemaField: SchemaField = {
        ...directSchemaField,
        ...filledSchemaField,
    };

    const businessAttributeGlossaryTerm: GlossaryTerm = {
        urn: 'urn:businessAttributeTerm',
        type: EntityType.GlossaryTerm,
        name: 'businessAttributeTermName',
        hierarchicalName: 'business.businessAttributeTermName',
        properties: {
            name: 'businessAttributeTermName',
            definition: 'business attribute term',
            termSource: 'INTERNAL',
        },
    };

    const mockBusinessAttribute: BusinessAttribute = {
        urn: 'urn:li:businessAttribute:testBA',
        type: EntityType.BusinessAttribute,
        properties: {
            name: 'Test Business Attribute',
            description: 'Test description',
            glossaryTerms: {
                terms: [
                    {
                        associatedUrn: 'urn:li:glossaryTerm:business.businessAttributeTermName',
                        term: businessAttributeGlossaryTerm,
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

    it('should extract uneditableTerms when they were provided in SchemaField only', () => {
        const extractFieldTermsInfo = renderHook(() => useExtractFieldGlossaryTermsInfo(emptyEditableSchemaMetadata))
            .result.current;

        const { directTerms, editableTerms, uneditableTerms, numberOfTerms } = extractFieldTermsInfo(filledSchemaField);

        expect(directTerms?.terms).toHaveLength(0);
        expect(editableTerms?.terms).toHaveLength(0);
        expect(uneditableTerms?.terms).toHaveLength(1);
        expect(uneditableTerms?.terms?.[0]?.term?.properties?.name).toBe('testTermName');
        expect(numberOfTerms).toBe(1);
    });

    it('should extract uneditableTerms when they were provided on Schema Field Entity only', () => {
        const extractFieldTermsInfo = renderHook(() => useExtractFieldGlossaryTermsInfo(emptyEditableSchemaMetadata))
            .result.current;

        const { directTerms, editableTerms, uneditableTerms, numberOfTerms } = extractFieldTermsInfo(directSchemaField);

        expect(directTerms?.terms).toHaveLength(1);
        expect(directTerms?.terms?.[0]?.term?.properties?.name).toBe('testTermName');
        expect(editableTerms?.terms).toHaveLength(0);
        expect(uneditableTerms?.terms).toHaveLength(0);
        expect(numberOfTerms).toBe(1);
    });

    it('should extract editableTerms when they were provided in editableSchemaMetadata only', () => {
        const extractFieldTermsInfo = renderHook(() => useExtractFieldGlossaryTermsInfo(filledEditableSchemaMetadata))
            .result.current;

        const { directTerms, editableTerms, uneditableTerms, numberOfTerms } = extractFieldTermsInfo(emptySchemaField);

        expect(directTerms?.terms).toHaveLength(0);
        expect(editableTerms?.terms).toHaveLength(1);
        expect(editableTerms?.terms?.[0]?.term?.properties?.name).toBe('testTermName');
        expect(uneditableTerms?.terms).toHaveLength(0);
        expect(numberOfTerms).toBe(1);
    });

    it('should extract all terms when they were provided in both schema and editable metadata, but exclude duplicates', () => {
        const extractFieldTermsInfo = renderHook(() => useExtractFieldGlossaryTermsInfo(filledEditableSchemaMetadata))
            .result.current;

        const { directTerms, editableTerms, uneditableTerms, numberOfTerms } = extractFieldTermsInfo(filledSchemaField);

        expect(directTerms?.terms).toHaveLength(0);
        expect(editableTerms?.terms).toHaveLength(1);
        expect(editableTerms?.terms?.[0]?.term?.properties?.name).toBe('testTermName');
        expect(uneditableTerms?.terms).toHaveLength(0);
        expect(numberOfTerms).toBe(1);
    });

    it('should extract uneditableTerms when they were provided everywhere, with duplicates', () => {
        const extractFieldTermsInfo = renderHook(() => useExtractFieldGlossaryTermsInfo(filledEditableSchemaMetadata))
            .result.current;

        const { directTerms, editableTerms, uneditableTerms, numberOfTerms } =
            extractFieldTermsInfo(complexSchemaField);

        expect(directTerms?.terms).toHaveLength(1);
        expect(directTerms?.terms?.[0]?.term?.properties?.name).toBe('testTermName');
        expect(editableTerms?.terms).toHaveLength(0);
        expect(uneditableTerms?.terms).toHaveLength(0);
        expect(numberOfTerms).toBe(1);
    });

    it('should extract uneditableTerms when they were provided everywhere, no duplicates', () => {
        const editableSchemaMetadata: EditableSchemaMetadata = {
            editableSchemaFieldInfo: [
                {
                    fieldPath: 'testField',
                    glossaryTerms: { terms: [{ term: testTerm2, associatedUrn: dummySchemaFieldUrn }] },
                },
            ],
        };
        const extractFieldTermsInfo = renderHook(() => useExtractFieldGlossaryTermsInfo(editableSchemaMetadata)).result
            .current;

        const schemaField: SchemaField = {
            ...directSchemaField,
            glossaryTerms: { terms: [{ term: testTerm3, associatedUrn: dummySchemaFieldUrn }] },
        };
        const { directTerms, editableTerms, uneditableTerms, numberOfTerms } = extractFieldTermsInfo(schemaField);

        expect(directTerms?.terms).toHaveLength(1);
        expect(directTerms?.terms?.[0]?.term?.properties?.name).toBe('testTermName');
        expect(editableTerms?.terms).toHaveLength(1);
        expect(editableTerms?.terms?.[0]?.term?.properties?.name).toBe('testTermName2');
        expect(uneditableTerms?.terms).toHaveLength(1);
        expect(uneditableTerms?.terms?.[0]?.term?.properties?.name).toBe('testTermName3');
        expect(numberOfTerms).toBe(3);
    });

    it('should not extract any terms when they are not provided', () => {
        const extractFieldTermsInfo = renderHook(() => useExtractFieldGlossaryTermsInfo(emptyEditableSchemaMetadata))
            .result.current;

        const { directTerms, editableTerms, uneditableTerms, numberOfTerms } = extractFieldTermsInfo(emptySchemaField);

        expect(directTerms?.terms).toHaveLength(0);
        expect(editableTerms?.terms).toHaveLength(0);
        expect(uneditableTerms?.terms).toHaveLength(0);
        expect(numberOfTerms).toBe(0);
    });

    it('should extract business attribute glossary terms when schema field has business attribute', () => {
        const extractFieldGlossaryTermsInfo = renderHook(() =>
            useExtractFieldGlossaryTermsInfo(emptyEditableSchemaMetadata),
        ).result.current;

        const { editableTerms, uneditableTerms, numberOfTerms } = extractFieldGlossaryTermsInfo(
            schemaFieldWithBusinessAttribute,
        );

        expect(editableTerms?.terms).toHaveLength(0);
        expect(uneditableTerms?.terms?.[0]?.term?.properties?.name === 'businessAttributeTermName').toBeTruthy();
        expect(numberOfTerms).toBe(1);
    });

    it('should combine field glossary terms and business attribute glossary terms', () => {
        const schemaFieldWithBothTerms: SchemaField = {
            ...filledSchemaField,
            schemaFieldEntity: schemaFieldWithBusinessAttribute.schemaFieldEntity,
        };

        const extractFieldGlossaryTermsInfo = renderHook(() =>
            useExtractFieldGlossaryTermsInfo(emptyEditableSchemaMetadata),
        ).result.current;

        const { editableTerms, uneditableTerms, numberOfTerms } =
            extractFieldGlossaryTermsInfo(schemaFieldWithBothTerms);

        expect(editableTerms?.terms).toHaveLength(0);
        expect(uneditableTerms?.terms).toHaveLength(2);
        expect(
            uneditableTerms?.terms?.some((termAssoc) => termAssoc.term?.properties?.name === 'testTermName'),
        ).toBeTruthy();
        expect(
            uneditableTerms?.terms?.some(
                (termAssoc) => termAssoc.term?.properties?.name === 'businessAttributeTermName',
            ),
        ).toBeTruthy();
        expect(numberOfTerms).toBe(2);
    });

    it('should extract extra uneditable terms from fields that match the field path insensitive to V2', () => {
        const extractFieldTermsInfo = renderHook(() =>
            useExtractFieldGlossaryTermsInfo(editableSchemaMetadataWithExtraTerms),
        ).result.current;

        const { directTerms, editableTerms, uneditableTerms, numberOfTerms } = extractFieldTermsInfo(emptySchemaField);

        expect(directTerms?.terms).toHaveLength(0);

        expect(editableTerms?.terms).toHaveLength(1);
        expect(editableTerms?.terms?.[0]?.term?.properties?.name).toBe('testTermName');

        expect(uneditableTerms?.terms).toHaveLength(1);
        expect(uneditableTerms?.terms?.[0]?.term?.properties?.name).toBe('extraTermName');

        expect(numberOfTerms).toBe(2);
    });
});
