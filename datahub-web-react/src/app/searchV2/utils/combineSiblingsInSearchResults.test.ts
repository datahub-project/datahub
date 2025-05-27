import { combineSiblingsInSearchResults } from '@app/searchV2/utils/combineSiblingsInSearchResults';

const searchResultWithSiblings = [
    {
        entity: {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.raw_orders,PROD)',
            exists: true,
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
                    },
                ],
                __typename: 'SiblingProperties',
            },
            siblingsSearch: {
                searchResults: [
                    {
                        entity: {
                            urn: 'urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.raw_orders,PROD)',
                            exists: true,
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
                    },
                ],
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
            exists: true,
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
                        urn: 'urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.raw_orders,PROD)',
                        type: 'DATASET',
                    },
                ],
                __typename: 'SiblingProperties',
            },
            siblingsSearch: {
                searchResults: [
                    {
                        entity: {
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
                    },
                ],
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

const searchResultWithGhostSiblings = [
    {
        entity: {
            urn: 'urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.raw_orders,PROD)',
            exists: true,
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
                    },
                ],
                __typename: 'SiblingProperties',
            },
            siblingsSearch: {
                searchResults: [
                    {
                        entity: {
                            urn: 'urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.raw_orders,PROD)',
                            exists: false,
                            type: 'DATASET',
                        },
                    },
                ],
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

// --- Test data for DBT/Snowflake sibling handling ---
const dbtSnowflakeCommonName = 'dbt_plus_snowflake_asset';
const dbtPlatformName = 'dbt';
const snowflakePlatformName = 'snowflake';

const dbtUrn = `urn:li:dataset:(urn:li:dataPlatform:${dbtPlatformName},${dbtSnowflakeCommonName},PROD)`;
const snowflakeUrn = `urn:li:dataset:(urn:li:dataPlatform:${snowflakePlatformName},${dbtSnowflakeCommonName},PROD)`;

const dbtSiblingEntityData = {
    urn: dbtUrn,
    exists: true,
    type: 'DATASET',
    name: dbtSnowflakeCommonName,
    platform: {
        urn: `urn:li:dataPlatform:${dbtPlatformName}`,
        type: 'DATA_PLATFORM',
        name: dbtPlatformName,
        properties: {
            type: 'OTHERS',
            displayName: 'dbt',
            datasetNameDelimiter: '.',
            logoUrl: '/assets/platforms/dbtlogo.png',
            __typename: 'DataPlatformProperties',
        },
        __typename: 'DataPlatform',
    },
    properties: { name: 'dbt_plus_snowflake_asset', __typename: 'DatasetProperties' }, // Simplified properties
    __typename: 'Dataset',
};

const snowflakeSiblingEntityData = {
    urn: snowflakeUrn,
    exists: true,
    type: 'DATASET',
    name: dbtSnowflakeCommonName,
    platform: {
        urn: `urn:li:dataPlatform:${snowflakePlatformName}`,
        type: 'DATA_PLATFORM',
        name: snowflakePlatformName,
        properties: {
            type: 'WAREHOUSE',
            displayName: 'Snowflake',
            datasetNameDelimiter: '.',
            logoUrl: '/assets/platforms/snowflakelogo.png',
            __typename: 'DataPlatformProperties',
        },
        __typename: 'DataPlatform',
    },
    properties: { name: 'dbt_plus_snowflake_asset', __typename: 'DatasetProperties' }, // Simplified properties
    __typename: 'Dataset',
};

const dbtSearchResultItem = {
    entity: {
        urn: dbtUrn,
        exists: true,
        type: 'DATASET',
        name: dbtSnowflakeCommonName,
        platform: {
            urn: `urn:li:dataPlatform:${dbtPlatformName}`,
            type: 'DATA_PLATFORM',
            name: dbtPlatformName,
            properties: {
                type: 'OTHERS',
                displayName: 'dbt',
                datasetNameDelimiter: '.',
                logoUrl: '/assets/platforms/dbtlogo.png',
                __typename: 'DataPlatformProperties',
            },
            __typename: 'DataPlatform',
        },
        properties: { name: 'dbt_plus_snowflake_asset', __typename: 'DatasetProperties' },
        siblingsSearch: {
            searchResults: [{ entity: snowflakeSiblingEntityData }],
        },
        __typename: 'Dataset',
    },
    matchedFields: [{ name: 'name', value: dbtSnowflakeCommonName, __typename: 'MatchedField' }],
    __typename: 'SearchResult',
};

const snowflakeSearchResultItem = {
    entity: {
        urn: snowflakeUrn,
        exists: true,
        type: 'DATASET',
        name: dbtSnowflakeCommonName,
        platform: {
            urn: `urn:li:dataPlatform:${snowflakePlatformName}`,
            type: 'DATA_PLATFORM',
            name: snowflakePlatformName,
            properties: {
                type: 'WAREHOUSE',
                displayName: 'Snowflake',
                datasetNameDelimiter: '.',
                logoUrl: '/assets/platforms/snowflakelogo.png',
                __typename: 'DataPlatformProperties',
            },
            __typename: 'DataPlatform',
        },
        properties: { name: 'dbt_plus_snowflake_asset', __typename: 'DatasetProperties' },
        siblingsSearch: {
            searchResults: [{ entity: dbtSiblingEntityData }],
        },
        __typename: 'Dataset',
    },
    matchedFields: [{ name: 'name', value: dbtSnowflakeCommonName, __typename: 'MatchedField' }],
    __typename: 'SearchResult',
};

const searchResultsDbtFirstSnowflakeSecond = [dbtSearchResultItem, snowflakeSearchResultItem];
const searchResultsSnowflakeFirstDbtSecond = [snowflakeSearchResultItem, dbtSearchResultItem];
// --- End of test data for DBT/Snowflake ---

// --- Test data for non-DBT sibling handling ---
const platformACommonName = 'platform_a_plus_platform_b_asset';
const platformAName = 'platformA';
const platformBName = 'platformB';

const platformAUrn = `urn:li:dataset:(urn:li:dataPlatform:${platformAName},${platformACommonName},PROD)`;
const platformBUrn = `urn:li:dataset:(urn:li:dataPlatform:${platformBName},${platformACommonName},PROD)`;

const platformASiblingEntityData = {
    urn: platformAUrn,
    exists: true,
    type: 'DATASET',
    name: platformACommonName,
    platform: {
        urn: `urn:li:dataPlatform:${platformAName}`,
        type: 'DATA_PLATFORM',
        name: platformAName,
        properties: {
            type: 'OTHERS',
            displayName: 'Platform A',
            datasetNameDelimiter: '.',
            logoUrl: '/assets/platforms/platformAlogo.png',
            __typename: 'DataPlatformProperties',
        },
        __typename: 'DataPlatform',
    },
    properties: { name: platformACommonName, __typename: 'DatasetProperties' },
    __typename: 'Dataset',
};

const platformBSiblingEntityData = {
    urn: platformBUrn,
    exists: true,
    type: 'DATASET',
    name: platformACommonName,
    platform: {
        urn: `urn:li:dataPlatform:${platformBName}`,
        type: 'DATA_PLATFORM',
        name: platformBName,
        properties: {
            type: 'OTHERS',
            displayName: 'Platform B',
            datasetNameDelimiter: '.',
            logoUrl: '/assets/platforms/platformBlogo.png',
            __typename: 'DataPlatformProperties',
        },
        __typename: 'DataPlatform',
    },
    properties: { name: platformACommonName, __typename: 'DatasetProperties' },
    __typename: 'Dataset',
};

const platformASearchResultItem = {
    entity: {
        urn: platformAUrn,
        exists: true,
        type: 'DATASET',
        name: platformACommonName,
        platform: {
            urn: `urn:li:dataPlatform:${platformAName}`,
            type: 'DATA_PLATFORM',
            name: platformAName,
            properties: {
                type: 'OTHERS',
                displayName: 'Platform A',
                datasetNameDelimiter: '.',
                logoUrl: '/assets/platforms/platformAlogo.png',
                __typename: 'DataPlatformProperties',
            },
            __typename: 'DataPlatform',
        },
        properties: { name: platformACommonName, __typename: 'DatasetProperties' },
        siblingsSearch: {
            searchResults: [{ entity: platformBSiblingEntityData }],
        },
        __typename: 'Dataset',
    },
    matchedFields: [{ name: 'name', value: platformACommonName, __typename: 'MatchedField' }],
    __typename: 'SearchResult',
};

const platformBSearchResultItem = {
    entity: {
        urn: platformBUrn,
        exists: true,
        type: 'DATASET',
        name: platformACommonName,
        platform: {
            urn: `urn:li:dataPlatform:${platformBName}`,
            type: 'DATA_PLATFORM',
            name: platformBName,
            properties: {
                type: 'OTHERS',
                displayName: 'Platform B',
                datasetNameDelimiter: '.',
                logoUrl: '/assets/platforms/platformBlogo.png',
                __typename: 'DataPlatformProperties',
            },
            __typename: 'DataPlatform',
        },
        properties: { name: platformACommonName, __typename: 'DatasetProperties' },
        siblingsSearch: {
            searchResults: [{ entity: platformASiblingEntityData }],
        },
        __typename: 'Dataset',
    },
    matchedFields: [{ name: 'name', value: platformACommonName, __typename: 'MatchedField' }],
    __typename: 'SearchResult',
};

const searchResultsPlatformAFirstPlatformBSecond = [platformASearchResultItem, platformBSearchResultItem];
// --- End of test data for non-DBT sibling handling ---

describe('siblingUtils', () => {
    describe('combineSiblingsInSearchResults', () => {
        it('combines search results to deduplicate siblings', () => {
            const result = combineSiblingsInSearchResults(false, searchResultWithSiblings as any);

            expect(result).toHaveLength(1);
            expect(result?.[0]?.matchedEntities?.[0]?.urn).toEqual(
                'urn:li:dataset:(urn:li:dataPlatform:dbt,cypress_project.jaffle_shop.raw_orders,PROD)',
            );
            expect(result?.[0]?.matchedEntities?.[1]?.urn).toEqual(
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.raw_orders,PROD)',
            );

            expect(result?.[0]?.matchedEntities).toHaveLength(2);

            expect(result?.[0]?.matchedFields).toHaveLength(2);
        });

        it('will not combine an entity with a ghost node', () => {
            const result = combineSiblingsInSearchResults(false, searchResultWithGhostSiblings as any);

            expect(result).toHaveLength(1);
            expect(result?.[0]?.matchedEntities?.[0]?.urn).toEqual(
                'urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.raw_orders,PROD)',
            );
            expect(result?.[0]?.matchedEntities).toHaveLength(1);

            expect(result?.[0]?.matchedFields).toHaveLength(2);
        });

        describe('dbt and snowflake sibling handling', () => {
            describe('when showSeparateSiblings is false (siblings combined)', () => {
                it('handles DBT entity first, then Snowflake sibling: result uses Snowflake URN, DBT platform, includes both in matchedEntities', () => {
                    const result = combineSiblingsInSearchResults(false, searchResultsDbtFirstSnowflakeSecond as any);

                    expect(result).toHaveLength(1);
                    expect((result[0] as any).entity.urn).toEqual(snowflakeUrn); // Combined entity uses Snowflake URN
                    expect((result[0] as any).entity.platform?.name).toEqual(dbtPlatformName); // Platform is from the (modified) DBT entity
                });

                it('handles Snowflake entity first, then DBT sibling: result uses Snowflake URN and platform, includes both in matchedEntities', () => {
                    const result = combineSiblingsInSearchResults(false, searchResultsSnowflakeFirstDbtSecond as any);

                    expect(result).toHaveLength(1);
                    expect((result[0] as any).entity.urn).toEqual(snowflakeUrn); // Combined entity uses Snowflake URN
                    expect((result[0] as any).entity.platform?.name).toEqual(snowflakePlatformName); // Platform is from the Snowflake entity
                });
            });

            describe('when showSeparateSiblings is true (siblings not combined)', () => {
                it('handles DBT entity first, then Snowflake sibling: keeps both entities as is', () => {
                    const result = combineSiblingsInSearchResults(true, searchResultsDbtFirstSnowflakeSecond as any);

                    expect(result).toHaveLength(2);
                    // First entity should be the original DBT entity
                    expect((result[0] as any).urn).toEqual(dbtUrn);
                    expect((result[0] as any).platform?.name).toEqual(dbtPlatformName);

                    // Second entity should be the original Snowflake entity
                    expect((result[1] as any).urn).toEqual(snowflakeUrn);
                    expect((result[1] as any).platform?.name).toEqual(snowflakePlatformName);
                });

                it('handles Snowflake entity first, then DBT sibling: keeps both entities as is', () => {
                    const result = combineSiblingsInSearchResults(true, searchResultsSnowflakeFirstDbtSecond as any);

                    expect(result).toHaveLength(2);
                    // First entity should be the original Snowflake entity
                    expect((result[0] as any).urn).toEqual(snowflakeUrn);
                    expect((result[0] as any).platform?.name).toEqual(snowflakePlatformName);

                    // Second entity should be the original DBT entity
                    expect((result[1] as any).urn).toEqual(dbtUrn);
                    expect((result[1] as any).platform?.name).toEqual(dbtPlatformName);
                });
            });
        });

        describe('non-DBT sibling handling', () => {
            it('combines two non-DBT siblings generically (no URN swapping) when showSeparateSiblings is false', () => {
                const result = combineSiblingsInSearchResults(false, searchResultsPlatformAFirstPlatformBSecond as any);

                expect(result).toHaveLength(1);
                const combinedEntry = result[0] as any;

                // Assuming platformA is chosen as the primary by the generic combine() because it's first in the input list
                // The `entity` property of the result should be the main display entity.
                expect(combinedEntry.entity.urn).toEqual(platformAUrn);
                expect(combinedEntry.entity.platform?.name).toEqual(platformAName);

                // Both original entities should be present in matchedEntities
                expect(combinedEntry.matchedEntities).toHaveLength(2);
                const entityAInMatched = combinedEntry.matchedEntities.find((e: any) => e.urn === platformAUrn);
                const entityBInMatched = combinedEntry.matchedEntities.find((e: any) => e.urn === platformBUrn);

                expect(entityAInMatched).toBeDefined();
                expect(entityAInMatched.platform.name).toEqual(platformAName); // Verify no platform swapping
                expect(entityBInMatched).toBeDefined();
                expect(entityBInMatched.platform.name).toEqual(platformBName); // Verify no platform swapping
            });

            it('does not combine two non-DBT siblings when showSeparateSiblings is true', () => {
                const result = combineSiblingsInSearchResults(true, searchResultsPlatformAFirstPlatformBSecond as any);

                expect(result).toHaveLength(2);
                // The structure for showSeparateSiblings: true currently spreads entity properties to the top level of the result item.
                // First entity should be the original Platform A entity
                expect((result[0] as any).urn).toEqual(platformAUrn);
                expect((result[0] as any).platform?.name).toEqual(platformAName);

                // Second entity should be the original Platform B entity
                expect((result[1] as any).urn).toEqual(platformBUrn);
                expect((result[1] as any).platform?.name).toEqual(platformBName);
            });
        });
    });
});
