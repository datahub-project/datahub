import { GetDatasetDocument } from './graphql/dataset.generated';
import { Dataset, EntityType, PlatformType, SchemaFieldDataType } from './types.generated';
import { GetMeDocument } from './graphql/me.generated';

const user3 = {
    username: 'sdas',
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
    editableProperties: {
        displayName: 'ShirshankaDas',
        title: 'https://crunchconf.com/img/2019/speakers/1559291783-ShirshankaDas.png',
        pictureLink: 'https://crunchconf.com/img/2019/speakers/1559291783-ShirshankaDas.png',
        teams: 'https://crunchconf.com/img/2019/speakers/1559291783-ShirshankaDas.png',
        skills: 'https://crunchconf.com/img/2019/speakers/1559291783-ShirshankaDas.png',
    },
};

const user1 = {
    username: 'sdas',
    urn: 'urn:li:corpuser:1',
    type: 'CORP_USER',
    info: {
        __typename: 'CorpUserInfo',
        email: 'sdas@domain.com',
        active: true,
        displayName: 'sdas',
        title: 'Software Engineer',
        firstName: 'Shirshanka',
        lastName: 'Das',
        fullName: 'Shirshanka Das',
    },
    properties: {
        __typename: 'CorpUserProperties',
        email: 'sdas@domain.com',
        active: true,
        displayName: 'sdas',
        title: 'Software Engineer',
        firstName: 'Shirshanka',
        lastName: 'Das',
        fullName: 'Shirshanka Das',
    },
    editableProperties: {
        __typename: 'CorpUserEditableProperties',
        displayName: 'sdas',
        title: 'Software Engineer',
        pictureLink: 'something',
        email: 'sdas@domain.com',
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
    name: 'Yet Another Dataset',
    origin: 'PROD',
    uri: 'www.google.com',
    properties: {
        name: 'Yet Another Dataset',
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
        __typename: 'Ownership',
        owners: [
            {
                owner: {
                    __typename: 'CorpUser',
                    ...user1,
                },
                type: 'TECHNICAL_OWNER',
                source: null,
            },
        ],
        lastModified: {
            time: 0,
        },
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
                    properties: {
                        name: 'abc-sample-tag',
                        description: 'sample tag',
                        colorHex: 'sample tag color',
                    },
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
                    properties: {
                        name: 'sample-glossary-term',
                        description: 'sample definition',
                        definition: 'sample definition',
                        termSource: 'sample term source',
                    },
                },
            },
        ],
    },
    incoming: null,
    outgoing: null,
    upstream: null,
    downstream: null,
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
    operations: null,
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
    domain: null,
    container: null,
    lineage: null,
    relationships: null,
    health: null,
    assertions: null,
    status: null,
    readRuns: null,
    writeRuns: null,
} as Dataset;

/*
    Define mock data to be returned by Apollo MockProvider.
*/
export const editMocks = [
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
    {
        request: {
            query: GetMeDocument,
            variables: {},
        },
        result: {
            data: {
                __typename: 'Query',
                me: {
                    __typename: 'AuthenticatedUser',
                    corpUser: { ...user3 },
                    platformPrivileges: {
                        viewAnalytics: true,
                        managePolicies: true,
                        manageIdentities: true,
                        generatePersonalAccessTokens: true,
                        manageIngestion: true,
                        manageSecrets: true,
                        manageDomains: true,
                    },
                },
            },
        },
    },
];
