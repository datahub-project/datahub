import {
    EditableSchemaMetadata,
    EntityType,
    GlossaryTerm,
    SchemaField,
    SchemaFieldDataType,
} from '@src/types.generated';
import { renderHook } from '@testing-library/react-hooks';
import useExtractFieldGlossaryTermsInfo from '../useExtractFieldGlossaryTermsInfo';

describe('useExtractFieldGlossaryTermsInfo', () => {
    const testGlossaryTerm: GlossaryTerm = {
        urn: 'urn:testField',
        type: EntityType.GlossaryTerm,
        name: 'testTermName',
        hierarchicalName: 'test.testTermName',
        properties: {
            name: 'testTermName',
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
                            associatedUrn: 'urn:li:glossaryTerm:test.testTermName',
                            term: testGlossaryTerm,
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
        glossaryTerms: {
            terms: [
                {
                    associatedUrn: 'urn:li:glossaryTerm:test.testTermName',
                    term: testGlossaryTerm,
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

    it('should extract uneditableTerms when they were provided in SchemaFild only', () => {
        const extractFieldGlossaryTermsInfo = renderHook(() =>
            useExtractFieldGlossaryTermsInfo(emptyEditableSchemaMetadata),
        ).result.current;

        const { editableTerms, uneditableTerms, numberOfTerms } = extractFieldGlossaryTermsInfo(filledSchemaField);

        expect(editableTerms).toBeUndefined();
        expect(uneditableTerms?.terms?.[0]?.term?.properties?.name === 'testTermName').toBeTruthy();
        expect(numberOfTerms).toBe(1);
    });

    it('should extract editableTerms when they were provided in editableSchemaMetadata only', () => {
        const extractFieldGlossaryTermsInfo = renderHook(() =>
            useExtractFieldGlossaryTermsInfo(filledEditableSchemaMetadata),
        ).result.current;

        const { editableTerms, uneditableTerms, numberOfTerms } = extractFieldGlossaryTermsInfo(emptySchemaField);

        expect(editableTerms?.terms?.[0]?.term?.properties?.name === 'testTermName').toBeTruthy();
        expect(uneditableTerms).toBeUndefined();
        expect(numberOfTerms).toBe(1);
    });

    it('should extract all terms when they were provided', () => {
        mockedUseBaseEntity.mockReturnValue(emptyBaseEntity);
        const extractFieldGlossaryTermsInfo = renderHook(() =>
            useExtractFieldGlossaryTermsInfo(filledEditableSchemaMetadata),
        ).result.current;

        const { editableTerms, uneditableTerms, numberOfTerms } = extractFieldGlossaryTermsInfo(filledSchemaField);

        expect(editableTerms?.terms?.[0]?.term?.properties?.name === 'testTermName').toBeTruthy();
        expect(uneditableTerms?.terms?.[0]?.term?.properties?.name === 'testTermName').toBeTruthy();
        expect(numberOfTerms).toBe(2);
    });

    it('should not extract any terms when they were not provided', () => {
        mockedUseBaseEntity.mockReturnValue(emptyBaseEntity);
        const extractFieldGlossaryTermsInfo = renderHook(() =>
            useExtractFieldGlossaryTermsInfo(emptyEditableSchemaMetadata),
        ).result.current;

        const { editableTerms, uneditableTerms, numberOfTerms } = extractFieldGlossaryTermsInfo(emptySchemaField);

        expect(editableTerms).toBeUndefined();
        expect(uneditableTerms).toBeUndefined();
        expect(numberOfTerms).toBe(0);
    });
});
