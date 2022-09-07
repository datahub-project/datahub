import { glossaryTerm1, sampleTag } from '../../../../../../../Mocks';
import { SchemaField } from '../../../../../../../types.generated';
import { getTestEntityRegistry } from '../../../../../../../utils/test-utils/TestPageContainer';
import { filterSchemaRows } from '../utils/filterSchemaRows';

describe('filterSchemaRows', () => {
    const testEntityRegistry = getTestEntityRegistry();
    const rows = [{ fieldPath: 'customer' }, { fieldPath: 'testing' }, { fieldPath: 'shipment' }] as SchemaField[];

    it('should properly filter schema rows based on field name', () => {
        const filterText = 'test';
        const editableSchemaMetadata = { editableSchemaFieldInfo: [] };
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rows,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'testing' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter schema rows based on field name regardless of capitalization', () => {
        const editableSchemaMetadata = { editableSchemaFieldInfo: [] };
        const filterText = 'TeSt';
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rows,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'testing' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter schema rows based on tags', () => {
        const editableSchemaMetadata = {
            editableSchemaFieldInfo: [
                { fieldPath: 'customer', globalTags: { tags: [{ tag: sampleTag }] }, glossaryTerms: null },
            ],
        };
        const filterText = sampleTag.properties.name;
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rows,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'customer' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter schema rows based on glossary terms', () => {
        const editableSchemaMetadata = {
            editableSchemaFieldInfo: [
                { fieldPath: 'shipment', globalTags: null, glossaryTerms: { terms: [{ term: glossaryTerm1 }] } },
            ],
        };
        const filterText = glossaryTerm1.properties?.name as string;
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rows,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'shipment' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter and find children fields', () => {
        const rowsWithChildren = [
            { fieldPath: 'customer' },
            { fieldPath: 'testing' },
            { fieldPath: 'customer.child1' },
            { fieldPath: 'customer.child2' },
        ] as SchemaField[];
        const editableSchemaMetadata = { editableSchemaFieldInfo: [] };
        const filterText = 'child';
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rowsWithChildren,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([
            { fieldPath: 'customer' },
            { fieldPath: 'customer.child1' },
            { fieldPath: 'customer.child2' },
        ]);
        expect(expandedRowsFromFilter).toMatchObject(new Set(['customer']));
    });

    it('should properly filter and find children fields multiple levels down', () => {
        const rowsWithChildren = [
            { fieldPath: 'customer' },
            { fieldPath: 'testing' },
            { fieldPath: 'customer.child1' },
            { fieldPath: 'customer.child1.findMe' },
            { fieldPath: 'customer.child2' },
        ] as SchemaField[];
        const editableSchemaMetadata = { editableSchemaFieldInfo: [] };
        const filterText = 'find';
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rowsWithChildren,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([
            { fieldPath: 'customer' },
            { fieldPath: 'customer.child1' },
            { fieldPath: 'customer.child1.findMe' },
        ]);
        expect(expandedRowsFromFilter).toMatchObject(new Set(['customer', 'customer.child1']));
    });
});
