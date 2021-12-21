import { GetDatasetDocument } from './graphql/dataset.generated';
import { Dataset, EntityType, PlatformType, SchemaFieldDataType } from './types.generated';
import { GetMeOnlyDocument } from './graphql/me.generated';
// I had to make a new mock file cos the original mock file, ownership of dataset and the mock identity of the person visiting the schema pages is not the same
// hence cannot test the edit schema functionality.

const user1 = {
    username: '1',
    urn: 'urn:li:corpuser:1',
    type: EntityType.CorpUser,
    info: {
        email: 'sdas@domain.com',
        active: true,
        displayName: 'sdas',
        title: 'Software Engineer',
        firstName: 'Shirshanka',
        lastName: 'Das',
        fullName: 'Shirshanka Das',
    },
    editableInfo: {
        pictureLink: 'https://crunchconf.com/img/2019/speakers/1559291783-ShirshankaDas.png',
    },
    globalTags: {
        tags: [
            {
                tag: {
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:abc-sample-tag',
                    name: 'abc-sample-tag',
                    description: 'sample tag',
                },
            },
        ],
    },
};

const user2 = {
    username: 'john',
    urn: 'urn:li:corpuser:3',
    type: EntityType.CorpUser,
    info: {
        email: 'john@domain.com',
        active: true,
        displayName: 'john',
        title: 'Eng',
        firstName: 'John',
        lastName: 'Joyce',
        fullName: 'John Joyce',
    },
    editableInfo: {
        pictureLink: null,
    },
    globalTags: {
        tags: [
            {
                tag: {
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:abc-sample-tag',
                    name: 'abc-sample-tag',
                    description: 'sample tag',
                },
            },
        ],
    },
};

export const dataset3 = {
    __typename: 'Dataset',
    urn: 'urn:li:dataset:3',
    type: EntityType.Dataset,
    platform: {
        urn: 'urn:li:dataPlatform:kafka',
        name: 'Kafka',
        info: {
            displayName: 'Kafka',
            type: PlatformType.MessageBroker,
            datasetNameDelimiter: '.',
            logoUrl: '',
        },
        type: EntityType.DataPlatform,
    },
    platformNativeType: 'STREAM',
    name: 'Yet Another Dataset2',
    origin: 'PROD',
    uri: 'www.google.comx',
    properties: {
        description: 'This and here we have yet another Dataset (YAN). Are there more?',
        origin: 'PROD',
        customProperties: [{ key: 'propertyAKey', value: 'propertyAValue' }],
        externalUrl: 'https://data.hub',
    },
    editableProperties: null,
    created: {
        time: 0,
    },
    lastModified: {
        time: 0,
    },
    ownership: {
        owners: [
            {
                owner: {
                    ...user1,
                    __typename: 'CorpUser',
                },
                type: 'DATAOWNER',
                __typename: 'Owner',
            },
            {
                owner: {
                    ...user2,
                    __typename: 'CorpUser',
                },
                type: 'DELEGATE',
                __typename: 'Owner',
            },
        ],
        lastModified: {
            time: 0,
        },
        __typename: 'Ownership',
    },
    globalTags: {
        __typename: 'GlobalTags',
        tags: [
            {
                tag: {
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:abc-sample-tag',
                    name: 'abc-sample-tag',
                    description: 'sample tag',
                },
            },
        ],
    },
    glossaryTerms: {
        terms: [
            {
                term: {
                    type: EntityType.GlossaryTerm,
                    urn: 'urn:li:glossaryTerm:sample-glossary-term',
                    name: 'sample-glossary-term',
                    hierarchicalName: 'example.sample-glossary-term',
                    glossaryTermInfo: {
                        definition: 'sample definition',
                        termSource: 'sample term source',
                    },
                },
            },
        ],
    },
    incoming: null,
    outgoing: null,
    institutionalMemory: {
        elements: [
            {
                url: 'https://www.google.com',
                author: { urn: 'urn:li:corpuser:datahub', username: 'datahub', type: EntityType.CorpUser },
                description: 'This only points to Google',
                label: 'This only points to Google',
                created: {
                    actor: 'urn:li:corpuser:1',
                    time: 1612396473001,
                },
            },
        ],
    },
    schemaMetadata: {
        __typename: 'SchemaMetadata',
        aspectVersion: 0,
        createdAt: 0,
        fields: [
            {
                nullable: false,
                recursive: false,
                fieldPath: 'user_id',
                description: 'Id of the user created',
                type: SchemaFieldDataType.String,
                nativeDataType: 'varchar(100)',
                isPartOfKey: false,
                jsonPath: null,
                globalTags: null,
                glossaryTerms: null,
            },
            {
                nullable: false,
                recursive: false,
                fieldPath: 'user_name',
                description: 'Name of the user who signed up',
                type: SchemaFieldDataType.String,
                nativeDataType: 'boolean',
                isPartOfKey: false,
                jsonPath: null,
                globalTags: null,
                glossaryTerms: null,
            },
        ],
        hash: '',
        platformSchema: null,
        platformUrn: 'urn:li:dataPlatform:hive',
        created: {
            actor: 'urn:li:corpuser:jdoe',
            time: 1581407189000,
        },
        cluster: '',
        name: 'SampleHiveSchema',
        version: 0,
        lastModified: {
            actor: 'urn:li:corpuser:jdoe',
            time: 1581407189000,
        },
        datasetUrn: 'urn:li:dataset:3',
        primaryKeys: [],
        foreignKeys: [],
    },
    previousSchemaMetadata: null,
    editableSchemaMetadata: null,
    deprecation: null,
    usageStats: null,
    datasetProfiles: [
        {
            rowCount: 10,
            columnCount: 5,
            timestampMillis: 0,
            fieldProfiles: [
                {
                    fieldPath: 'testColumn',
                    uniqueCount: 1,
                    uniqueProportion: 0.129,
                    nullCount: 2,
                    nullProportion: 0.56,
                    min: '3',
                    max: '4',
                    mean: '5',
                    median: '6',
                    stdev: '1.2',
                    sampleValues: ['value1', 'value2', 'value3'],
                },
            ],
        },
    ],
    subTypes: null,
    viewProperties: null,
    autoRenderAspects: [
        {
            aspectName: 'autoRenderAspect',
            payload: '{ "values": [{ "autoField1": "autoValue1", "autoField2": "autoValue2" }] }',
            renderSpec: {
                displayType: 'tabular',
                displayName: 'Auto Render Aspect Custom Tab Name',
                key: 'values',
            },
        },
    ],
    status: {
        removed: false,
    },
} as Dataset;

/*
    Define mock data to be returned by Apollo MockProvider. 
*/
export const mocks2 = [
    {
        request: {
            query: GetDatasetDocument,
            variables: {
                urn: 'urn:li:dataset:3',
            },
        },
        result: {
            data: {
                dataset: {
                    ...dataset3,
                },
            },
        },
    },
    // {
    //     request: {
    //         query: GetMeDocument,
    //         variables: {},
    //     },
    //     result: {
    //         data: {
    //             __typename: 'Query',
    //             me: {
    //                 __typename: 'AuthenticatedUser',
    //                 corpUser: { ...user2 },
    //                 platformPrivileges: {
    //                     viewAnalytics: true,
    //                     managePolicies: true,
    //                     manageIdentities: true,
    //                 },
    //             },
    //         },
    //     },
    // },

    {
        // this mock can be shifted elsewhere in the doc. need to create new mock instead of recycling cos it needs to be specific to query else it doesnt work
        request: {
            query: GetMeOnlyDocument,
            variables: {},
        },
        result: {
            data: {
                __typename: 'Query',
                me: {
                    corpUser: {
                        username: '1',
                        urn: 'urn:li:corpuser:1',
                    },
                },
            },
        },
    },
];
