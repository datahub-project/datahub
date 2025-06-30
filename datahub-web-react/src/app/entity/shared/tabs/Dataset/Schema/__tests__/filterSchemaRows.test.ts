import { filterSchemaRows } from '@app/entity/shared/tabs/Dataset/Schema/utils/filterSchemaRows';
import { glossaryTerm1, sampleTag } from '@src/Mocks';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';

import { SchemaField } from '@types';

describe('filterSchemaRows', () => {
    const testEntityRegistry = getTestEntityRegistry();
    const rows = [
        { fieldPath: 'customer', description: 'customer description' },
        { fieldPath: 'testing', description: 'testing description' },
        { fieldPath: 'shipment', description: 'shipment description' },
    ] as SchemaField[];

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

    it('should properly filter schema rows based on description', () => {
        const filterText = 'testing description';
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

    it('should properly filter schema rows based on description regardless of capitalization', () => {
        const editableSchemaMetadata = { editableSchemaFieldInfo: [] };
        const filterText = 'TeSting DesCriptioN';
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rows,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'testing' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter schema rows based on editable description', () => {
        const editableSchemaMetadata = {
            editableSchemaFieldInfo: [
                {
                    fieldPath: 'customer',
                    description: 'editable customer description',
                    globalTags: null,
                    glossaryTerms: null,
                },
            ],
        };
        const filterText = 'editable customer description';
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rows,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'customer' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter schema rows based on editable description regardless of capitalization', () => {
        const editableSchemaMetadata = {
            editableSchemaFieldInfo: [
                {
                    fieldPath: 'customer',
                    description: 'editable customer description',
                    globalTags: null,
                    glossaryTerms: null,
                },
            ],
        };
        const filterText = 'EdiTable CuStoMer DesCriptioN';
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rows,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'customer' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter schema rows based on editable tags', () => {
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

    it('should properly filter schema rows based on editable glossary terms', () => {
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

    it('should properly filter schema rows based on non-editable tags', () => {
        const rowsWithTags = [
            { fieldPath: 'customer' },
            { fieldPath: 'testing', globalTags: { tags: [{ tag: sampleTag }] } },
            { fieldPath: 'shipment' },
        ] as SchemaField[];
        const editableSchemaMetadata = { editableSchemaFieldInfo: [] };
        const filterText = sampleTag.properties.name;
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rowsWithTags,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'testing' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter schema rows based on non-editable glossary terms', () => {
        const rowsWithTerms = [
            { fieldPath: 'customer' },
            { fieldPath: 'testing' },
            { fieldPath: 'shipment', glossaryTerms: { terms: [{ term: glossaryTerm1 }] } },
        ] as SchemaField[];
        const editableSchemaMetadata = { editableSchemaFieldInfo: [] };
        const filterText = glossaryTerm1.properties?.name as string;
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rowsWithTerms,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'shipment' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter schema rows based on business attribute properties description', () => {
        const rowsWithSchemaFieldEntity = [
            {
                fieldPath: 'customer',
                schemaFieldEntity: {
                    businessAttributes: {
                        businessAttribute: {
                            businessAttribute: { properties: { description: 'customer description' } },
                        },
                    },
                },
            },
            {
                fieldPath: 'testing',
                schemaFieldEntity: {
                    businessAttributes: {
                        businessAttribute: {
                            businessAttribute: { properties: { description: 'testing description' } },
                        },
                    },
                },
            },
            {
                fieldPath: 'shipment',
                schemaFieldEntity: {
                    businessAttributes: {
                        businessAttribute: {
                            businessAttribute: { properties: { description: 'shipment description' } },
                        },
                    },
                },
            },
        ] as SchemaField[];
        const filterText = 'testing description';
        const editableSchemaMetadata = { editableSchemaFieldInfo: [] };
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rowsWithSchemaFieldEntity,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'testing' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });

    it('should properly filter schema rows based on business attribute properties tags', () => {
        const rowsWithSchemaFieldEntity = [
            {
                fieldPath: 'customer',
                schemaFieldEntity: {
                    businessAttributes: {
                        businessAttribute: {
                            businessAttribute: { properties: { tags: { tags: [{ tag: sampleTag }] } } },
                        },
                    },
                },
            },
            {
                fieldPath: 'testing',
                schemaFieldEntity: {
                    businessAttributes: {
                        businessAttribute: {
                            businessAttribute: {
                                properties: { tags: { tags: [{ tag: { properties: { name: 'otherTag' } } }] } },
                            },
                        },
                    },
                },
            },
            {
                fieldPath: 'shipment',
                schemaFieldEntity: {
                    businessAttributes: {
                        businessAttribute: {
                            businessAttribute: {
                                properties: { tags: { tags: [{ tag: { properties: { name: 'anotherTag' } } }] } },
                            },
                        },
                    },
                },
            },
        ] as SchemaField[];
        const filterText = sampleTag.properties.name;
        const editableSchemaMetadata = { editableSchemaFieldInfo: [] };
        const { filteredRows, expandedRowsFromFilter } = filterSchemaRows(
            rowsWithSchemaFieldEntity,
            editableSchemaMetadata,
            filterText,
            testEntityRegistry,
        );

        expect(filteredRows).toMatchObject([{ fieldPath: 'customer' }]);
        expect(expandedRowsFromFilter).toMatchObject(new Set());
    });
});
