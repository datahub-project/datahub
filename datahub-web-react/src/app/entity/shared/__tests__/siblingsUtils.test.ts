import { dataset3WithLineage, dataset4WithLineage } from '../../../../Mocks';
import { EntityType } from '../../../../types.generated';
import {
    combineEntityDataWithSiblings,
    combineSiblingsInSearchResults,
    shouldEntityBeTreatedAsPrimary,
} from '../siblingUtils';

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
    siblings: {
        isPrimary: false,
    },
};

const datasetPrimaryWithSiblings = {
    ...datasetPrimary,
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

const searchResultWithSiblings = [
    {
        entity: {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.raw_orders,PROD)',
            type: 'DATASET',
            name: 'cypress_project.jaffle_shop.raw_orders',
            origin: 'PROD',
            uri: null,
            platform: {
                urn: 'urn:li:dataPlatform:bigquery',
                type: 'DATA_PLATFORM',
                name: 'bigquery',
                properties: {
                    type: 'RELATIONAL_DB',
                    displayName: 'BigQuery',
                    datasetNameDelimiter: '.',
                    logoUrl: '/assets/platforms/bigquerylogo.png',
                    __typename: 'DataPlatformProperties',
                },
                displayName: null,
                info: null,
                __typename: 'DataPlatform',
            },
            dataPlatformInstance: null,
            editableProperties: null,
            platformNativeType: null,
            properties: {
                name: 'raw_orders',
                description: null,
                qualifiedName: null,
                customProperties: [],
                __typename: 'DatasetProperties',
            },
            ownership: null,
            globalTags: null,
            glossaryTerms: null,
            subTypes: {
                typeNames: ['table'],
                __typename: 'SubTypes',
            },
            domain: null,
            container: {
                urn: 'urn:li:container:348c96555971d3f5c1ffd7dd2e7446cb',
                platform: {
                    urn: 'urn:li:dataPlatform:bigquery',
                    type: 'DATA_PLATFORM',
                    name: 'bigquery',
                    properties: {
                        type: 'RELATIONAL_DB',
                        displayName: 'BigQuery',
                        datasetNameDelimiter: '.',
                        logoUrl: '/assets/platforms/bigquerylogo.png',
                        __typename: 'DataPlatformProperties',
                    },
                    displayName: null,
                    info: null,
                    __typename: 'DataPlatform',
                },
                properties: {
                    name: 'jaffle_shop',
                    __typename: 'ContainerProperties',
                },
                subTypes: {
                    typeNames: ['Dataset'],
                    __typename: 'SubTypes',
                },
                deprecation: null,
                __typename: 'Container',
            },
            parentContainers: {
                count: 2,
                containers: [
                    {
                        urn: 'urn:li:container:348c96555971d3f5c1ffd7dd2e7446cb',
                        platform: {
                            urn: 'urn:li:dataPlatform:bigquery',
                            type: 'DATA_PLATFORM',
                            name: 'bigquery',
                            properties: {
                                type: 'RELATIONAL_DB',
                                displayName: 'BigQuery',
                                datasetNameDelimiter: '.',
                                logoUrl: '/assets/platforms/bigquerylogo.png',
                                __typename: 'DataPlatformProperties',
                            },
                            displayName: null,
                            info: null,
                            __typename: 'DataPlatform',
                        },
                        properties: {
                            name: 'jaffle_shop',
                            __typename: 'ContainerProperties',
                        },
                        subTypes: {
                            typeNames: ['Dataset'],
                            __typename: 'SubTypes',
                        },
                        deprecation: null,
                        __typename: 'Container',
                    },
                    {
                        urn: 'urn:li:container:b5e95fce839e7d78151ed7e0a7420d84',
                        platform: {
                            urn: 'urn:li:dataPlatform:bigquery',
                            type: 'DATA_PLATFORM',
                            name: 'bigquery',
                            properties: {
                                type: 'RELATIONAL_DB',
                                displayName: 'BigQuery',
                                datasetNameDelimiter: '.',
                                logoUrl: '/assets/platforms/bigquerylogo.png',
                                __typename: 'DataPlatformProperties',
                            },
                            displayName: null,
                            info: null,
                            __typename: 'DataPlatform',
                        },
                        properties: {
                            name: 'cypress_project',
                            __typename: 'ContainerProperties',
                        },
                        subTypes: {
                            typeNames: ['Project'],
                            __typename: 'SubTypes',
                        },
                        deprecation: null,
                        __typename: 'Container',
                    },
                ],
                __typename: 'ParentContainersResult',
            },
            deprecation: null,
            siblings: {
                isPrimary: false,
                siblings: [
                    {
                        urn: 'urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.raw_orders,PROD)',
                        type: 'DATASET',
                        platform: {
                            urn: 'urn:li:dataPlatform:dbt',
                            type: 'DATA_PLATFORM',
                            name: 'dbt',
                            properties: {
                                type: 'OTHERS',
                                displayName: 'dbt',
                                datasetNameDelimiter: '.',
                                logoUrl: '/assets/platforms/dbtlogo.png',
                                __typename: 'DataPlatformProperties',
                            },
                            displayName: null,
                            info: null,
                            __typename: 'DataPlatform',
                        },
                        name: 'cypress_project.jaffle_shop.raw_orders',
                        properties: {
                            name: 'raw_orders',
                            description: '',
                            qualifiedName: null,
                            __typename: 'DatasetProperties',
                        },
                        __typename: 'Dataset',
                    },
                ],
                __typename: 'SiblingProperties',
            },
            __typename: 'Dataset',
        },
        matchedFields: [
            {
                name: 'name',
                value: 'raw_orders',
                __typename: 'MatchedField',
            },
            {
                name: 'id',
                value: 'cypress_project.jaffle_shop.raw_orders',
                __typename: 'MatchedField',
            },
        ],
        insights: [],
        __typename: 'SearchResult',
    },
    {
        entity: {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.raw_orders,PROD)',
            type: 'DATASET',
            name: 'cypress_project.jaffle_shop.raw_orders',
            origin: 'PROD',
            uri: null,
            platform: {
                urn: 'urn:li:dataPlatform:dbt',
                type: 'DATA_PLATFORM',
                name: 'dbt',
                properties: {
                    type: 'OTHERS',
                    displayName: 'dbt',
                    datasetNameDelimiter: '.',
                    logoUrl: '/assets/platforms/dbtlogo.png',
                    __typename: 'DataPlatformProperties',
                },
                displayName: null,
                info: null,
                __typename: 'DataPlatform',
            },
            dataPlatformInstance: null,
            editableProperties: null,
            platformNativeType: null,
            properties: {
                name: 'raw_orders',
                description: '',
                qualifiedName: null,
                customProperties: [
                    {
                        key: 'catalog_version',
                        value: '1.0.4',
                        __typename: 'StringMapEntry',
                    },
                    {
                        key: 'node_type',
                        value: 'seed',
                        __typename: 'StringMapEntry',
                    },
                    {
                        key: 'materialization',
                        value: 'seed',
                        __typename: 'StringMapEntry',
                    },
                    {
                        key: 'dbt_file_path',
                        value: 'data/raw_orders.csv',
                        __typename: 'StringMapEntry',
                    },
                    {
                        key: 'catalog_schema',
                        value: 'https://schemas.getdbt.com/dbt/catalog/v1.json',
                        __typename: 'StringMapEntry',
                    },
                    {
                        key: 'catalog_type',
                        value: 'table',
                        __typename: 'StringMapEntry',
                    },
                    {
                        key: 'manifest_version',
                        value: '1.0.4',
                        __typename: 'StringMapEntry',
                    },
                    {
                        key: 'manifest_schema',
                        value: 'https://schemas.getdbt.com/dbt/manifest/v4.json',
                        __typename: 'StringMapEntry',
                    },
                ],
                __typename: 'DatasetProperties',
            },
            ownership: null,
            globalTags: null,
            glossaryTerms: null,
            subTypes: {
                typeNames: ['seed'],
                __typename: 'SubTypes',
            },
            domain: null,
            container: null,
            parentContainers: {
                count: 0,
                containers: [],
                __typename: 'ParentContainersResult',
            },
            deprecation: null,
            siblings: {
                isPrimary: true,
                siblings: [
                    {
                        urn: 'urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.raw_orders,PROD)',
                        type: 'DATASET',
                        platform: {
                            urn: 'urn:li:dataPlatform:bigquery',
                            type: 'DATA_PLATFORM',
                            name: 'bigquery',
                            properties: {
                                type: 'RELATIONAL_DB',
                                displayName: 'BigQuery',
                                datasetNameDelimiter: '.',
                                logoUrl: '/assets/platforms/bigquerylogo.png',
                                __typename: 'DataPlatformProperties',
                            },
                            displayName: null,
                            info: null,
                            __typename: 'DataPlatform',
                        },
                        name: 'cypress_project.jaffle_shop.raw_orders',
                        properties: {
                            name: 'raw_orders',
                            description: null,
                            qualifiedName: null,
                            __typename: 'DatasetProperties',
                        },
                        __typename: 'Dataset',
                    },
                ],
                __typename: 'SiblingProperties',
            },
            __typename: 'Dataset',
        },
        matchedFields: [
            {
                name: 'name',
                value: 'raw_orders',
                __typename: 'MatchedField',
            },
            {
                name: 'id',
                value: 'cypress_project.jaffle_shop.raw_orders',
                __typename: 'MatchedField',
            },
        ],
        insights: [],
        __typename: 'SearchResult',
    },
];

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

    describe('combineSiblingsInSearchResults', () => {
        it('combines search results to deduplicate siblings', () => {
            const result = combineSiblingsInSearchResults(searchResultWithSiblings as any);

            expect(result).toHaveLength(1);
            expect(result?.[0]?.matchedEntities?.[0]?.urn).toEqual(
                'urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.raw_orders,PROD)',
            );
            expect(result?.[0]?.matchedEntities?.[1]?.urn).toEqual(
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.raw_orders,PROD)',
            );
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
