import { dataset3WithLineage, dataset3WithSchema, dataset4WithLineage } from '../../../../Mocks';
import { EntityType, SchemaFieldDataType } from '../../../../types.generated';
import { combineEntityDataWithSiblings, shouldEntityBeTreatedAsPrimary } from '../siblingUtils';

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
        description: '',
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
};

const datasetUnprimaryWithPrimarySiblings = {
    ...datasetUnprimary,
    siblings: {
        isPrimary: false,
        siblings: [datasetPrimary],
    },
};

const datasetUnprimaryWithNoPrimarySiblings = {
    ...datasetUnprimary,
    siblings: {
        isPrimary: false,
        siblings: [datasetUnprimary],
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
});
