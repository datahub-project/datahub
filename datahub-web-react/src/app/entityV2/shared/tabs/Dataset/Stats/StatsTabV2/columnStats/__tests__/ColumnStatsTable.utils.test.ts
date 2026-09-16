import {
    createStatsOnlyField,
    filterColumnStatsByQuery,
    flattenFields,
    handleRowScrollIntoView,
    inferIsFieldNullable,
    mapToSchemaField,
    mapToSchemaFields,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/ColumnStatsTable.utils';

// Local type definitions for testing
interface DatasetFieldProfile {
    fieldPath: string;
    nullCount?: number | null;
    nullProportion?: number | null;
    uniqueCount?: number | null;
    min?: string | null;
    max?: string | null;
}

interface ExtendedSchemaFields {
    fieldPath: string;
    type: any;
    children?: ExtendedSchemaFields[];
    depth?: number;
    nullable: boolean;
    recursive: boolean;
}

describe('ColumnStatsTable Utils', () => {
    describe('inferIsFieldNullable', () => {
        it('should return true when nullCount is greater than 0', () => {
            const stat: DatasetFieldProfile = {
                fieldPath: 'test_field',
                nullCount: 5,
            };
            expect(inferIsFieldNullable(stat)).toBe(true);
        });

        it('should return false when nullCount is 0', () => {
            const stat: DatasetFieldProfile = {
                fieldPath: 'test_field',
                nullCount: 0,
            };
            expect(inferIsFieldNullable(stat)).toBe(false);
        });

        it('should return true when nullProportion is greater than 0', () => {
            const stat: DatasetFieldProfile = {
                fieldPath: 'test_field',
                nullProportion: 0.1,
            };
            expect(inferIsFieldNullable(stat)).toBe(true);
        });

        it('should return false when nullProportion is 0', () => {
            const stat: DatasetFieldProfile = {
                fieldPath: 'test_field',
                nullProportion: 0.0,
            };
            expect(inferIsFieldNullable(stat)).toBe(false);
        });

        it('should default to true when no null data is available', () => {
            const stat: DatasetFieldProfile = {
                fieldPath: 'test_field',
            };
            expect(inferIsFieldNullable(stat)).toBe(true);
        });

        it('should prioritize nullCount over nullProportion', () => {
            const stat: DatasetFieldProfile = {
                fieldPath: 'test_field',
                nullCount: 0,
                nullProportion: 0.1,
            };
            expect(inferIsFieldNullable(stat)).toBe(false);
        });
    });

    describe('mapToSchemaField', () => {
        it('should map valid field data correctly', () => {
            const fieldData = {
                fieldPath: 'test_field',
                type: { type: 'STRING' },
                nativeDataType: 'VARCHAR',
                nullable: true,
                recursive: false,
                description: 'Test field',
            };

            const result = mapToSchemaField(fieldData);

            expect(result).toEqual({
                fieldPath: 'test_field',
                type: { type: 'STRING' },
                nativeDataType: 'VARCHAR',
                schemaFieldEntity: null,
                nullable: true,
                recursive: false,
                description: 'Test field',
                children: undefined,
            });
        });

        it('should handle missing optional properties', () => {
            const fieldData = {
                fieldPath: 'test_field',
            };

            const result = mapToSchemaField(fieldData);

            expect(result).toEqual({
                fieldPath: 'test_field',
                type: null,
                nativeDataType: null,
                schemaFieldEntity: null,
                nullable: false,
                recursive: false,
                description: null,
                children: undefined,
            });
        });

        it('should throw error for invalid input', () => {
            expect(() => mapToSchemaField(null)).toThrow('Invalid field data provided to mapToSchemaField');
            expect(() => mapToSchemaField(undefined)).toThrow('Invalid field data provided to mapToSchemaField');
            expect(() => mapToSchemaField('string')).toThrow('Invalid field data provided to mapToSchemaField');
        });
    });

    describe('mapToSchemaFields', () => {
        it('should map array of field data correctly', () => {
            const fieldsData = [
                { fieldPath: 'field1', type: 'STRING' },
                { fieldPath: 'field2', type: 'NUMBER' },
            ];

            const result = mapToSchemaFields(fieldsData);

            expect(result).toHaveLength(2);
            expect(result[0].fieldPath).toBe('field1');
            expect(result[1].fieldPath).toBe('field2');
        });

        it('should return empty array for non-array input', () => {
            expect(mapToSchemaFields(null as any)).toEqual([]);
            expect(mapToSchemaFields(undefined as any)).toEqual([]);
            expect(mapToSchemaFields('string' as any)).toEqual([]);
        });
    });

    describe('createStatsOnlyField', () => {
        it('should create field with inferred nullability', () => {
            const stat: DatasetFieldProfile = {
                fieldPath: 'stats_only_field',
                nullCount: 5,
            };

            const result = createStatsOnlyField(stat);

            expect(result).toEqual({
                fieldPath: 'stats_only_field',
                type: null,
                nativeDataType: null,
                schemaFieldEntity: null,
                nullable: true, // inferred from nullCount > 0
                recursive: false,
                description: null,
            });
        });
    });

    describe('flattenFields', () => {
        it('should flatten nested field hierarchies', () => {
            const fields: ExtendedSchemaFields[] = [
                {
                    fieldPath: 'parent',
                    type: null,
                    nullable: false,
                    recursive: false,
                    children: [
                        {
                            fieldPath: 'parent.child1',
                            type: null,
                            nullable: false,
                            recursive: false,
                            children: [
                                {
                                    fieldPath: 'parent.child1.grandchild',
                                    type: null,
                                    nullable: false,
                                    recursive: false,
                                },
                            ],
                        },
                        { fieldPath: 'parent.child2', type: null, nullable: false, recursive: false },
                    ],
                },
                { fieldPath: 'sibling', type: null, nullable: false, recursive: false },
            ];

            const result = flattenFields(fields);

            expect(result).toHaveLength(5);
            expect(result.map((f) => f.fieldPath)).toEqual([
                'parent',
                'parent.child1',
                'parent.child1.grandchild',
                'parent.child2',
                'sibling',
            ]);
        });

        it('should handle fields without children', () => {
            const fields: ExtendedSchemaFields[] = [
                { fieldPath: 'field1', type: null, nullable: false, recursive: false },
                { fieldPath: 'field2', type: null, nullable: false, recursive: false },
            ];

            const result = flattenFields(fields);

            expect(result).toHaveLength(2);
            expect(result.map((f) => f.fieldPath)).toEqual(['field1', 'field2']);
        });
    });

    describe('filterColumnStatsByQuery', () => {
        const testData = [
            { column: 'customer_id' },
            { column: 'customer_name' },
            { column: 'order_total' },
            { column: 'product_category' },
        ];

        it('should filter data based on query', () => {
            const result = filterColumnStatsByQuery(testData, 'customer');
            expect(result).toHaveLength(2);
            expect(result.map((item) => item.column)).toEqual(['customer_id', 'customer_name']);
        });

        it('should be case insensitive', () => {
            const result = filterColumnStatsByQuery(testData, 'CUSTOMER');
            expect(result).toHaveLength(2);
        });

        it('should return all data for empty query', () => {
            const result = filterColumnStatsByQuery(testData, '');
            expect(result).toHaveLength(4);
        });

        it('should return all data for whitespace-only query', () => {
            const result = filterColumnStatsByQuery(testData, '   ');
            expect(result).toHaveLength(4);
        });

        it('should return empty array for no matches', () => {
            const result = filterColumnStatsByQuery(testData, 'nonexistent');
            expect(result).toHaveLength(0);
        });
    });

    describe('handleRowScrollIntoView', () => {
        let mockRow: HTMLTableRowElement;
        let mockHeader: HTMLTableSectionElement;
        let mockScrollContainer: HTMLElement;

        beforeEach(() => {
            // Create mock DOM elements
            mockScrollContainer = document.createElement('div');
            mockScrollContainer.scrollTop = 100;

            const mockTable = document.createElement('table');
            Object.defineProperty(mockTable, 'parentElement', {
                value: mockScrollContainer,
                writable: false,
            });

            mockRow = document.createElement('tr');
            mockRow.closest = vi.fn().mockReturnValue(mockTable);
            mockRow.getBoundingClientRect = vi.fn().mockReturnValue({
                top: 50,
                bottom: 100,
            });

            mockHeader = document.createElement('thead');
            mockHeader.getBoundingClientRect = vi.fn().mockReturnValue({
                top: 0,
                bottom: 80,
            });
        });

        it('should adjust scroll when row is above header', () => {
            handleRowScrollIntoView(mockRow, mockHeader);
            expect(mockScrollContainer.scrollTop).toBe(70); // 100 - (80 - 50)
        });

        it('should not adjust scroll when row is below header', () => {
            mockRow.getBoundingClientRect = vi.fn().mockReturnValue({
                top: 100,
                bottom: 150,
            });

            handleRowScrollIntoView(mockRow, mockHeader);
            expect(mockScrollContainer.scrollTop).toBe(100); // unchanged
        });

        it('should handle missing row or header gracefully', () => {
            expect(() => handleRowScrollIntoView(undefined, mockHeader)).not.toThrow();
            expect(() => handleRowScrollIntoView(mockRow, null)).not.toThrow();
            expect(() => handleRowScrollIntoView(undefined, null)).not.toThrow();
        });
    });
});
