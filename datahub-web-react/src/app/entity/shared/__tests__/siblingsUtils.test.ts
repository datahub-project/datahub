import {
    cleanHelper,
    combineEntityDataWithSiblings,
    mergeOwners,
    shouldEntityBeTreatedAsPrimary,
} from '@app/entity/shared/siblingUtils';
import { dataset3WithLineage, dataset3WithSchema, dataset4WithLineage } from '@src/Mocks';

import { EntityType, SchemaFieldDataType } from '@types';

const usageStats = {
    buckets: [
        {
            bucket: 1650412800000,
            duration: 'DAY',
            resource: 'urn:li:dataset:4',
            metrics: {
                uniqueUserCount: 1,
                totalSqlQueries: 37,
                topSqlQueries: ['some sql query'],
                __typename: 'UsageAggregationMetrics',
            },
            __typename: 'UsageAggregation',
        },
    ],
    aggregations: {
        uniqueUserCount: 2,
        totalSqlQueries: 39,
        users: [
            {
                user: {
                    urn: 'urn:li:corpuser:user',
                    username: 'user',
                    __typename: 'CorpUser',
                },
                count: 2,
                userEmail: 'user@datahubproject.io',
                __typename: 'UserUsageCounts',
            },
        ],
        fields: [
            {
                fieldName: 'field',
                count: 7,
                __typename: 'FieldUsageCounts',
            },
        ],
        __typename: 'UsageQueryResultAggregations',
    },
    __typename: 'UsageQueryResult',
};

const datasetPrimary = {
    ...dataset3WithLineage,
    ...dataset3WithSchema.dataset,
    properties: {
        ...dataset3WithLineage.properties,
        description: 'primary description',
    },
    editableProperties: {
        description: null,
    },
    globalTags: {
        tags: [
            {
                tag: {
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:primary-tag',
                    name: 'primary-tag',
                    description: 'primary tag',
                    properties: {
                        name: 'primary-tag',
                        description: 'primary tag',
                        colorHex: 'primary tag color',
                    },
                },
            },
        ],
    },
    siblings: {
        isPrimary: true,
    },
};

const datasetUnprimary = {
    ...dataset4WithLineage,
    usageStats,
    properties: {
        ...dataset4WithLineage.properties,
        description: 'unprimary description',
    },
    editableProperties: {
        description: 'secondary description',
    },
    globalTags: {
        tags: [
            {
                tag: {
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:unprimary-tag',
                    name: 'unprimary-tag',
                    description: 'unprimary tag',
                    properties: {
                        name: 'unprimary-tag',
                        description: 'unprimary tag',
                        colorHex: 'unprimary tag color',
                    },
                },
            },
        ],
    },
    schemaMetadata: {
        ...dataset4WithLineage.schemaMetadata,
        fields: [
            {
                __typename: 'SchemaField',
                nullable: false,
                recursive: false,
                fieldPath: 'new_one',
                description: 'Test to make sure fields merge works',
                type: SchemaFieldDataType.String,
                nativeDataType: 'varchar(100)',
                isPartOfKey: false,
                jsonPath: null,
                globalTags: null,
                glossaryTerms: null,
                label: 'hi',
            },
            ...(dataset4WithLineage.schemaMetadata?.fields || []),
            {
                __typename: 'SchemaField',
                nullable: false,
                recursive: false,
                fieldPath: 'duplicate_field',
                description: 'Test to make sure fields merge works case insensitive',
                type: SchemaFieldDataType.String,
                nativeDataType: 'varchar(100)',
                isPartOfKey: false,
                jsonPath: null,
                globalTags: null,
                glossaryTerms: null,
                label: 'hi',
            },
        ],
    },
    siblings: {
        isPrimary: false,
    },
};

const datasetPrimaryWithSiblings = {
    ...datasetPrimary,
    schemaMetadata: {
        ...datasetPrimary.schemaMetadata,
        fields: [
            ...(datasetPrimary.schemaMetadata?.fields || []),
            {
                __typename: 'SchemaField',
                nullable: false,
                recursive: false,
                fieldPath: 'DUPLICATE_FIELD',
                description: 'Test to make sure fields merge works case insensitive',
                type: SchemaFieldDataType.String,
                nativeDataType: 'varchar(100)',
                isPartOfKey: false,
                jsonPath: null,
                globalTags: null,
                glossaryTerms: null,
                label: 'hi',
            },
        ],
    },

    siblings: {
        isPrimary: true,
        siblings: [datasetUnprimary],
    },
    siblingsSearch: {
        count: 1,
        total: 1,
        searchResults: [{ entity: datasetUnprimary, matchedFields: [] }],
    },
};

const datasetUnprimaryWithPrimarySiblings = {
    ...datasetUnprimary,
    siblings: {
        isPrimary: false,
        siblings: [datasetPrimary],
    },
    siblingsSearch: {
        count: 1,
        total: 1,
        searchResults: [{ entity: datasetPrimary, matchedFields: [] }],
    },
};

const datasetUnprimaryWithNoPrimarySiblings = {
    ...datasetUnprimary,
    siblings: {
        isPrimary: false,
        siblings: [datasetUnprimary],
    },
    siblingsSearch: {
        count: 1,
        total: 1,
        searchResults: [{ entity: datasetUnprimary, matchedFields: [] }],
    },
};

describe('siblingUtils', () => {
    describe('combineEntityDataWithSiblings', () => {
        it('combines my metadata with my siblings as primary', () => {
            const baseEntity = { dataset: datasetPrimaryWithSiblings };
            expect(baseEntity.dataset.usageStats).toBeNull();
            const combinedData = combineEntityDataWithSiblings(baseEntity);
            // will merge properties only one entity has
            expect(combinedData.dataset.usageStats).toEqual(usageStats);

            // will merge arrays
            expect(combinedData.dataset.globalTags.tags).toHaveLength(2);
            expect(combinedData.dataset.globalTags.tags[0].tag.urn).toEqual('urn:li:tag:unprimary-tag');
            expect(combinedData.dataset.globalTags.tags[1].tag.urn).toEqual('urn:li:tag:primary-tag');

            // merges schema metadata properly  by fieldPath
            expect(combinedData.dataset.schemaMetadata?.fields).toHaveLength(4);
            expect(combinedData.dataset.schemaMetadata?.fields[0]?.fieldPath).toEqual('new_one');
            expect(combinedData.dataset.schemaMetadata?.fields[1]?.fieldPath).toEqual('DUPLICATE_FIELD');
            expect(combinedData.dataset.schemaMetadata?.fields[2]?.fieldPath).toEqual('user_id');
            expect(combinedData.dataset.schemaMetadata?.fields[3]?.fieldPath).toEqual('user_name');

            // will overwrite string properties w/ primary
            expect(combinedData.dataset.editableProperties.description).toEqual('secondary description');

            // will take secondary string properties in the case of empty string
            expect(combinedData.dataset.properties.description).toEqual('primary description');

            // will stay primary
            expect(combinedData.dataset.siblings.isPrimary).toBeTruthy();
        });

        it('combines my metadata with my siblings as secondary', () => {
            const baseEntity = { dataset: datasetUnprimaryWithPrimarySiblings };
            const combinedData = combineEntityDataWithSiblings(baseEntity);

            // will stay secondary
            expect(combinedData.dataset.siblings.isPrimary).toBeFalsy();
        });
    });

    describe('shouldEntityBeTreatedAsPrimary', () => {
        it('will say a primary entity is primary', () => {
            expect(shouldEntityBeTreatedAsPrimary(datasetPrimaryWithSiblings)).toBeTruthy();
        });

        it('will say a un-primary entity is not primary', () => {
            expect(shouldEntityBeTreatedAsPrimary(datasetUnprimaryWithPrimarySiblings)).toBeFalsy();
        });

        it('will say a un-primary entity is primary if it has no primary sibling', () => {
            expect(shouldEntityBeTreatedAsPrimary(datasetUnprimaryWithNoPrimarySiblings)).toBeTruthy();
        });
    });

    describe('cleanHelper', () => {
        let visited: Set<any>;

        beforeEach(() => {
            visited = new Set();
        });

        it('should return the object immediately if it has already been visited', () => {
            const obj = { test: 'value' };
            visited.add(obj);
            const result = cleanHelper(obj, visited);
            expect(result).toBe(obj);
        });

        it('should remove null properties from objects', () => {
            const obj = {
                validProperty: 'test',
                nullProperty: null,
                anotherValid: 42,
            };
            const result = cleanHelper(obj, visited);
            expect(result).toEqual({
                validProperty: 'test',
                anotherValid: 42,
            });
            expect(result).not.toHaveProperty('nullProperty');
        });

        it('should remove undefined properties from objects', () => {
            const obj = {
                validProperty: 'test',
                undefinedProperty: undefined,
                anotherValid: 42,
            };
            const result = cleanHelper(obj, visited);
            expect(result).toEqual({
                validProperty: 'test',
                anotherValid: 42,
            });
            expect(result).not.toHaveProperty('undefinedProperty');
        });

        it('should remove empty object properties', () => {
            const obj = {
                validProperty: 'test',
                emptyObject: {},
                anotherValid: 42,
            };
            const result = cleanHelper(obj, visited);
            expect(result).toEqual({
                validProperty: 'test',
                anotherValid: 42,
            });
            expect(result).not.toHaveProperty('emptyObject');
        });

        it('should recursively clean nested objects', () => {
            const obj = {
                level1: {
                    validProp: 'test',
                    nullProp: null,
                    level2: {
                        nestedValid: 'nested',
                        nestedNull: null,
                        emptyNested: {},
                    },
                },
                topLevel: 'value',
            };
            const result = cleanHelper(obj, visited);
            expect(result).toEqual({
                level1: {
                    validProp: 'test',
                    level2: {
                        nestedValid: 'nested',
                    },
                },
                topLevel: 'value',
            });
        });

        it('should not modify arrays when they contain null, undefined, or empty objects', () => {
            const obj = {
                arrayProp: ['valid', null, undefined, {}, { validNested: 'test' }],
                regularProp: 'test',
            };
            const result = cleanHelper(obj, visited);
            expect(result.arrayProp).toHaveLength(5);
            expect(result.arrayProp[0]).toBe('valid');
            expect(result.arrayProp[1]).toBe(null);
            expect(result.arrayProp[2]).toBe(undefined);
            expect(result.arrayProp[3]).toEqual({});
            expect(result.arrayProp[4]).toEqual({ validNested: 'test' });
        });

        it('should still clean objects within arrays recursively', () => {
            const obj = {
                arrayProp: [
                    {
                        validProp: 'test',
                        nullProp: null,
                        nestedObj: {
                            valid: 'nested',
                            invalid: null,
                        },
                    },
                ],
            };
            const result = cleanHelper(obj, visited);
            expect(result.arrayProp[0]).toEqual({
                validProp: 'test',
                nestedObj: {
                    valid: 'nested',
                },
            });
            expect(result.arrayProp[0]).not.toHaveProperty('nullProp');
        });

        it('should handle circular references without infinite recursion', () => {
            const obj: any = {
                validProp: 'test',
                nullProp: null,
            };
            obj.circular = obj;

            const result = cleanHelper(obj, visited);
            expect(result.validProp).toBe('test');
            expect(result).not.toHaveProperty('nullProp');
            expect(result.circular).toBe(result);
        });

        it('should handle objects with non-configurable properties gracefully', () => {
            const obj = {
                validProp: 'test',
                nullProp: null,
            };

            // Make a property non-configurable
            Object.defineProperty(obj, 'nonConfigurable', {
                value: null,
                configurable: false,
                enumerable: true,
                writable: true,
            });

            const result = cleanHelper(obj, visited);
            expect(result.validProp).toBe('test');
            expect(result).not.toHaveProperty('nullProp');
            // Non-configurable null property should remain
            expect(result).toHaveProperty('nonConfigurable');
            expect(result.nonConfigurable).toBe(null);
        });

        it('should preserve valid nested structures', () => {
            const obj = {
                user: {
                    name: 'John',
                    age: 30,
                    preferences: {
                        theme: 'dark',
                        notifications: true,
                    },
                },
                metadata: {
                    created: '2023-01-01',
                    tags: ['important', 'user'],
                },
            };

            const result = cleanHelper(obj, visited);
            expect(result).toEqual(obj);
        });

        it('should handle complex mixed structures', () => {
            const obj = {
                validString: 'test',
                validNumber: 42,
                validBoolean: true,
                validArray: [1, 2, 3],
                nullValue: null,
                undefinedValue: undefined,
                emptyObject: {},
                nestedValid: {
                    innerValid: 'nested',
                    innerNull: null,
                    innerArray: ['a', null, 'b'],
                    innerEmpty: {},
                },
                mixedArray: [{ valid: 'item', invalid: null }, null, 'string', 42],
            };

            const result = cleanHelper(obj, visited);
            expect(result).toEqual({
                validString: 'test',
                validNumber: 42,
                validBoolean: true,
                validArray: [1, 2, 3],
                nestedValid: {
                    innerValid: 'nested',
                    innerArray: ['a', null, 'b'],
                },
                mixedArray: [{ valid: 'item' }, null, 'string', 42],
            });
        });

        it('should handle edge case of empty input object', () => {
            const obj = {};
            const result = cleanHelper(obj, visited);
            expect(result).toEqual({});
        });

        it('should handle edge case where object becomes empty after cleaning', () => {
            const obj = {
                nullProp: null,
                undefinedProp: undefined,
                emptyProp: {},
            };
            const result = cleanHelper(obj, visited);
            expect(result).toEqual({});
        });
    });

    describe('mergeOwners', () => {
        const createOwner = (ownerUrn: string, ownershipTypeUrn?: string, associatedUrn?: string) => ({
            associatedUrn:
                associatedUrn ||
                'urn:li:dataset(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)',
            owner: { urn: ownerUrn },
            ownershipType: ownershipTypeUrn ? { urn: ownershipTypeUrn } : undefined,
        });

        it('should merge two arrays with no duplicates', () => {
            const destinationArray = [
                createOwner('urn:li:corpGroup:bfoo', 'urn:li:ownershipType:__system__technical_owner'),
            ];
            const sourceArray = [createOwner('urn:li:corpGroup:bar', 'urn:li:ownershipType:__system__business_owner')];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(2);
            expect(result).toEqual([
                createOwner('urn:li:corpGroup:bfoo', 'urn:li:ownershipType:__system__technical_owner'),
                createOwner('urn:li:corpGroup:bar', 'urn:li:ownershipType:__system__business_owner'),
            ]);
        });

        it('should deduplicate owners with same owner.urn and ownershipType.urn', () => {
            const duplicateOwner = createOwner(
                'urn:li:corpGroup:bfoo',
                'urn:li:ownershipType:__system__technical_owner',
            );
            const destinationArray = [duplicateOwner];
            const sourceArray = [duplicateOwner];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual(duplicateOwner);
        });

        it('should keep owners with same owner.urn but different ownershipType.urn as separate entries', () => {
            const destinationArray = [
                createOwner('urn:li:corpGroup:bfoo', 'urn:li:ownershipType:__system__technical_owner'),
            ];
            const sourceArray = [createOwner('urn:li:corpGroup:bfoo', 'urn:li:ownershipType:__system__business_owner')];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(2);
            expect(result[0].owner.urn).toBe('urn:li:corpGroup:bfoo');
            expect(result[0].ownershipType?.urn).toBe('urn:li:ownershipType:__system__technical_owner');
            expect(result[1].owner.urn).toBe('urn:li:corpGroup:bfoo');
            expect(result[1].ownershipType?.urn).toBe('urn:li:ownershipType:__system__business_owner');
        });

        it('should keep owners with different owner.urn but same ownershipType.urn as separate entries', () => {
            const destinationArray = [
                createOwner('urn:li:corpGroup:bfoo', 'urn:li:ownershipType:__system__technical_owner'),
            ];
            const sourceArray = [createOwner('urn:li:corpGroup:bar', 'urn:li:ownershipType:__system__technical_owner')];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(2);
            expect(result[0].owner.urn).toBe('urn:li:corpGroup:bfoo');
            expect(result[1].owner.urn).toBe('urn:li:corpGroup:bar');
            expect(result[0].ownershipType?.urn).toBe('urn:li:ownershipType:__system__technical_owner');
            expect(result[1].ownershipType?.urn).toBe('urn:li:ownershipType:__system__technical_owner');
        });

        it('should handle empty destination array', () => {
            const destinationArray: any[] = [];
            const sourceArray = [
                createOwner('urn:li:corpGroup:bfoo', 'urn:li:ownershipType:__system__technical_owner'),
            ];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual(sourceArray[0]);
        });

        it('should handle empty source array', () => {
            const destinationArray = [
                createOwner('urn:li:corpGroup:bfoo', 'urn:li:ownershipType:__system__technical_owner'),
            ];
            const sourceArray: any[] = [];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual(destinationArray[0]);
        });

        it('should handle both arrays being empty', () => {
            const destinationArray: any[] = [];
            const sourceArray: any[] = [];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(0);
            expect(result).toEqual([]);
        });

        it('should handle owners with missing ownershipType', () => {
            const destinationArray = [
                createOwner('urn:li:corpGroup:bfoo'), // no ownershipType
            ];
            const sourceArray = [
                createOwner('urn:li:corpGroup:bfoo', 'urn:li:ownershipType:__system__technical_owner'),
            ];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(2);
            expect(result[0].ownershipType).toBeUndefined();
            expect(result[1].ownershipType?.urn).toBe('urn:li:ownershipType:__system__technical_owner');
        });

        it('should deduplicate owners that both have undefined ownershipType', () => {
            const ownerWithoutType = createOwner('urn:li:corpGroup:bfoo');
            const destinationArray = [ownerWithoutType];
            const sourceArray = [createOwner('urn:li:corpGroup:bfoo')]; // same owner, no ownershipType

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(1);
            expect(result[0].owner.urn).toBe('urn:li:corpGroup:bfoo');
            expect(result[0].ownershipType).toBeUndefined();
        });

        it('should preserve order with destination array items first', () => {
            const destinationArray = [
                createOwner('urn:li:corpGroup:dest1', 'urn:li:ownershipType:__system__technical_owner'),
                createOwner('urn:li:corpGroup:dest2', 'urn:li:ownershipType:__system__business_owner'),
            ];
            const sourceArray = [
                createOwner('urn:li:corpGroup:src1', 'urn:li:ownershipType:__system__data_owner'),
                createOwner('urn:li:corpGroup:src2', 'urn:li:ownershipType:__system__steward'),
            ];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(4);
            expect(result[0].owner.urn).toBe('urn:li:corpGroup:dest1');
            expect(result[1].owner.urn).toBe('urn:li:corpGroup:dest2');
            expect(result[2].owner.urn).toBe('urn:li:corpGroup:src1');
            expect(result[3].owner.urn).toBe('urn:li:corpGroup:src2');
        });

        it('should handle complex scenario with multiple duplicates and unique entries', () => {
            const destinationArray = [
                createOwner('urn:li:corpGroup:common', 'urn:li:ownershipType:__system__technical_owner'),
                createOwner('urn:li:corpGroup:dest_only', 'urn:li:ownershipType:__system__business_owner'),
                createOwner('urn:li:corpGroup:common', 'urn:li:ownershipType:__system__data_owner'),
            ];
            const sourceArray = [
                createOwner('urn:li:corpGroup:common', 'urn:li:ownershipType:__system__technical_owner'), // duplicate
                createOwner('urn:li:corpGroup:src_only', 'urn:li:ownershipType:__system__steward'),
                createOwner('urn:li:corpGroup:common', 'urn:li:ownershipType:__system__business_owner'), // different type
            ];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(5);

            // Should contain all unique combinations
            const ownerCombinations = result.map((owner) => ({
                ownerUrn: owner.owner.urn,
                ownershipTypeUrn: owner.ownershipType?.urn,
            }));

            expect(ownerCombinations).toContainEqual({
                ownerUrn: 'urn:li:corpGroup:common',
                ownershipTypeUrn: 'urn:li:ownershipType:__system__technical_owner',
            });
            expect(ownerCombinations).toContainEqual({
                ownerUrn: 'urn:li:corpGroup:dest_only',
                ownershipTypeUrn: 'urn:li:ownershipType:__system__business_owner',
            });
            expect(ownerCombinations).toContainEqual({
                ownerUrn: 'urn:li:corpGroup:common',
                ownershipTypeUrn: 'urn:li:ownershipType:__system__data_owner',
            });
            expect(ownerCombinations).toContainEqual({
                ownerUrn: 'urn:li:corpGroup:src_only',
                ownershipTypeUrn: 'urn:li:ownershipType:__system__steward',
            });
            expect(ownerCombinations).toContainEqual({
                ownerUrn: 'urn:li:corpGroup:common',
                ownershipTypeUrn: 'urn:li:ownershipType:__system__business_owner',
            });
        });

        it('should maintain all properties of owner objects beyond just owner.urn and ownershipType.urn', () => {
            const destinationArray = [
                {
                    associatedUrn: 'urn:li:dataset:123',
                    owner: { urn: 'urn:li:corpGroup:bfoo', name: 'Test Group' },
                    ownershipType: { urn: 'urn:li:ownershipType:__system__technical_owner', name: 'Technical Owner' },
                    extraProperty: 'destination_value',
                },
            ];
            const sourceArray = [
                {
                    associatedUrn: 'urn:li:dataset:456',
                    owner: { urn: 'urn:li:corpGroup:bar', displayName: 'Another Group' },
                    ownershipType: {
                        urn: 'urn:li:ownershipType:__system__business_owner',
                        description: 'Business Owner',
                    },
                    extraProperty: 'source_value',
                },
            ];

            const result = mergeOwners(destinationArray, sourceArray, {});

            expect(result).toHaveLength(2);
            expect(result[0]).toEqual(destinationArray[0]);
            expect(result[1]).toEqual(sourceArray[0]);
        });
    });
});
