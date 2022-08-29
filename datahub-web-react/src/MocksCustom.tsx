import { GetDatasetDocument } from './graphql/dataset.generated';
import { Container, Dataset, EntityType, PlatformType, SchemaFieldDataType } from './types.generated';
import { GetMeDocument } from './graphql/me.generated';
import { GetContainerDocument } from './graphql/container.generated';
import { GetSearchResultsDocument, GetSearchResultsQuery } from './graphql/search.generated';

const user2 = {
    username: 'sdas',
    urn: 'urn:li:corpuser:2',
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

const customContainer = {
    __typename: 'Container',
    urn: 'urn:li:container:customContainer',
    type: EntityType.Container,
    platform: {
        urn: 'urn:li:dataPlatform:kafka',
        name: 'Kafka',
        info: {
            displayName: 'Kafka',
            type: PlatformType.MessageBroker,
            datasetNameDelimiter: '.',
            logoUrl: '',
        },
        displayName: 'kafka',
        properties: null,
        type: EntityType.DataPlatform,
        __typename: 'DataPlatform',
    },
    properties: {
        __typename: 'ContainerProperties',
        name: 'newContainer',
        description: null,
        customProperties: null,
    },
    subTypes: {
        __typename: 'SubTypes',
        typeNames: ['any'],
    },
    deprecation: {
        __typename: 'Deprecation',
        deprecated: false,
        decommissionTime: 0,
        note: '',
        actor: 'urn:li:corpuser:blah',
    },
} as Container;

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
    dataPlatformInstance: null,
    platformNativeType: 'STREAM',
    name: 'Yet Another Dataset',
    origin: 'PROD',
    uri: 'www.google.com',
    properties: {
        name: 'Yet Another Dataset',
        description: 'This and here we have yet another Dataset (YAN). Are there more?',
        origin: 'PROD',
        customProperties: [{ key: 'propertyAKey', value: 'propertyAValue', associatedUrn: 'urn:li:dataset:3' }],
        externalUrl: 'https://data.hub',
    },
    parentContainers: {
        count: 0,
        containers: [],
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
                    urn: 'urn:li:corpuser:2',
                    type: 'CORP_USER',
                    username: 'jdoe',
                    info: {
                        __typename: 'CorpUserInfo',
                        active: true,
                        displayName: 'John Doe',
                        title: 'Software Engineer',
                        email: 'jdoe@linkedin.com',
                        firstName: null,
                        lastName: null,
                        fullName: 'John Doe',
                    },
                    properties: {
                        __typename: 'CorpUserProperties',
                        active: true,
                        displayName: 'John Doe',
                        title: 'Software Engineer',
                        email: 'jdoe@linkedin.com',
                        firstName: null,
                        lastName: null,
                        fullName: 'John Doe',
                    },
                    editableProperties: {
                        __typename: 'CorpUserEditableProperties',
                        displayName: 'sdas',
                        title: 'Software Engineer',
                        pictureLink: 'something',
                        email: 'sdas@domain.com',
                    },
                },
                type: 'DATAOWNER',
                associatedUrn: 'urn:li:dataset:3',
            },
        ],
        lastModified: {
            __typename: 'AuditStamp',
            time: 1581407189000,
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
                associatedUrn: 'urn:li:dataset:3',
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
                associatedUrn: 'urn:li:dataset:3',
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
    container: {
        ...customContainer,
    },
    lineage: null,
    relationships: null,
    health: [],
    assertions: null,
    status: null,
    readRuns: null,
    writeRuns: null,
    testResults: null,
    siblings: null,
    statsSummary: null,
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
            query: GetContainerDocument,
            variables: {
                urn: 'urn:li:container:customContainer',
            },
        },
        result: {
            data: {
                container: {
                    ...customContainer,
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
                    corpUser: { ...user2 },
                    platformPrivileges: {
                        viewAnalytics: true,
                        managePolicies: true,
                        manageIdentities: true,
                    },
                },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    types: 'Container',
                    query: '*',
                    start: 0,
                    count: 10,
                    filters: [
                        {
                            field: 'platform',
                            value: 'urn:li:dataPlatform:kafka',
                        },
                    ],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                search: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [
                        {
                            entity: {
                                __typename: 'Container',
                                ...customContainer,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [],
                },
            } as GetSearchResultsQuery,
        },
    },
];
