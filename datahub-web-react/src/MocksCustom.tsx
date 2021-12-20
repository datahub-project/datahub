import { GetDatasetDocument, GetDatasetOwnersGqlDocument, UpdateDatasetDocument } from './graphql/dataset.generated';
import { GetDataFlowDocument } from './graphql/dataFlow.generated';
import { GetDataJobDocument } from './graphql/dataJob.generated';
import { GetBrowsePathsDocument, GetBrowseResultsDocument } from './graphql/browse.generated';
import {
    GetAutoCompleteResultsDocument,
    GetAutoCompleteMultipleResultsDocument,
    GetSearchResultsDocument,
    GetSearchResultsQuery,
    GetSearchResultsForMultipleDocument,
    GetSearchResultsForMultipleQuery,
} from './graphql/search.generated';
import { GetUserDocument } from './graphql/user.generated';
import {
    Dataset,
    DataFlow,
    DataJob,
    GlossaryTerm,
    EntityType,
    PlatformType,
    MlModel,
    MlModelGroup,
    SchemaFieldDataType,
    ScenarioType,
    RecommendationRenderType,
    RelationshipDirection,
} from './types.generated';
import { GetTagDocument } from './graphql/tag.generated';
import { GetMlModelDocument } from './graphql/mlModel.generated';
import { GetMlModelGroupDocument } from './graphql/mlModelGroup.generated';
import { GetGlossaryTermDocument, GetGlossaryTermQuery } from './graphql/glossaryTerm.generated';
import { GetEntityCountsDocument } from './graphql/app.generated';
import { GetMeDocument, GetMeOnlyDocument } from './graphql/me.generated';
import { ListRecommendationsDocument } from './graphql/recommendations.generated';

const user1 = {
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

const dataset1 = {
    urn: 'urn:li:dataset:1',
    type: EntityType.Dataset,
    platform: {
        urn: 'urn:li:dataPlatform:hdfs',
        name: 'HDFS',
        type: EntityType.DataPlatform,
        info: {
            displayName: 'HDFS',
            type: PlatformType.FileSystem,
            datasetNameDelimiter: '.',
            logoUrl: '',
        },
    },
    platformNativeType: 'TABLE',
    name: 'The Great Test Dataset',
    origin: 'PROD',
    tags: ['Private', 'PII'],
    uri: 'www.google.com',
    properties: {
        description: 'This is the greatest dataset in the world, youre gonna love it!',
        customProperties: [
            {
                key: 'TestProperty',
                value: 'My property value.',
            },
            {
                key: 'AnotherTestProperty',
                value: 'My other property value.',
            },
        ],
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
                },
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
            },
        ],
        lastModified: {
            time: 0,
        },
    },
    institutionalMemory: {
        elements: [
            {
                url: 'https://www.google.com',
                description: 'This only points to Google',
                created: {
                    actor: 'urn:li:corpuser:1',
                    time: 1612396473001,
                },
            },
        ],
    },
    usageStats: null,
    datasetProfiles: [
        {
            timestampMillis: 0,
            rowCount: 10,
            columnCount: 5,
            fieldProfiles: [
                {
                    fieldPath: 'testColumn',
                },
            ],
        },
    ],
};

const dataset2 = {
    urn: 'urn:li:dataset:2',
    type: EntityType.Dataset,
    platform: {
        urn: 'urn:li:dataPlatform:mysql',
        name: 'MySQL',
        info: {
            displayName: 'MySQL',
            type: PlatformType.RelationalDb,
            datasetNameDelimiter: '.',
            logoUrl: '',
        },
        type: EntityType.DataPlatform,
    },
    platformNativeType: 'TABLE',
    name: 'Some Other Dataset',
    origin: 'PROD',
    tags: ['Outdated'],
    uri: 'www.google.com',
    properties: {
        description: 'This is some other dataset, so who cares!',
        customProperties: [],
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
                },
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
            },
        ],
        lastModified: {
            time: 0,
        },
    },
    usageStats: null,
    datasetProfiles: [
        {
            timestampMillis: 0,
            rowCount: 10,
            columnCount: 5,
            fieldProfiles: [
                {
                    fieldPath: 'testColumn',
                    min: '3',
                    max: '4',
                    median: '6',
                    stdev: '1.2',
                    nullProportion: 0.56,
                    sampleValues: ['value1', 'value2', 'value3'],
                },
            ],
        },
    ],
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
                },
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
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

export const dataset4 = {
    ...dataset3,
    name: 'Fourth Test Dataset',
    urn: 'urn:li:dataset:4',
};

export const dataset5 = {
    ...dataset3,
    name: 'Fifth Test Dataset',
    urn: 'urn:li:dataset:5',
};

export const dataset6 = {
    ...dataset3,
    name: 'Sixth Test Dataset',
    urn: 'urn:li:dataset:6',
};

export const dataset7 = {
    ...dataset3,
    name: 'Seventh Test Dataset',
    urn: 'urn:li:dataset:7',
};

export const dataset3WithLineage = {
    ...dataset3,
    outgoing: {
        start: 0,
        count: 2,
        total: 2,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataset7,
            },
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataset4,
            },
        ],
    },
    incoming: null,
};

export const dataset4WithLineage = {
    ...dataset4,
    outgoing: {
        start: 0,
        count: 2,
        total: 2,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataset6,
            },
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataset5,
            },
        ],
    },
    incoming: {
        start: 0,
        count: 1,
        total: 1,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Incoming,
                entity: dataset3,
            },
        ],
    },
};

export const dataset5WithCyclicalLineage = {
    ...dataset5,
    outgoing: {
        start: 0,
        count: 1,
        total: 1,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataset3,
            },
        ],
    },
    incoming: {
        start: 0,
        count: 1,
        total: 1,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Incoming,
                entity: dataset7,
            },
        ],
    },
};

export const dataset5WithLineage = {
    ...dataset5,
    outgoing: null,
    incoming: {
        start: 0,
        count: 3,
        total: 3,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Incoming,
                entity: dataset7,
            },
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Incoming,
                entity: dataset6,
            },
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Incoming,
                entity: dataset4,
            },
        ],
    },
};

export const dataset6WithLineage = {
    ...dataset6,
    outgoing: {
        start: 0,
        count: 1,
        total: 1,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataset5,
            },
        ],
    },
    incoming: {
        start: 0,
        count: 1,
        total: 1,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Incoming,
                entity: dataset4,
            },
        ],
    },
};

export const dataset7WithLineage = {
    ...dataset7,
    outgoing: {
        start: 0,
        count: 1,
        total: 1,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataset5,
            },
        ],
    },
    incoming: {
        start: 0,
        count: 1,
        total: 1,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Incoming,
                entity: dataset3,
            },
        ],
    },
};

export const dataset7WithSelfReferentialLineage = {
    ...dataset7,
    outgoing: {
        start: 0,
        count: 2,
        total: 2,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataset5,
            },
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataset7,
            },
        ],
    },
    incoming: {
        start: 0,
        count: 2,
        total: 2,
        relationships: [
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Incoming,
                entity: dataset3,
            },
            {
                type: 'DownstreamOf',
                direction: RelationshipDirection.Incoming,
                entity: dataset7,
            },
        ],
    },
};
const glossaryTerm1 = {
    urn: 'urn:li:glossaryTerm:1',
    type: EntityType.GlossaryTerm,
    name: 'Another glossary term',
    hierarchicalName: 'example.AnotherGlossaryTerm',
    ownership: {
        owners: [
            {
                owner: {
                    ...user1,
                },
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
            },
        ],
        lastModified: {
            time: 0,
        },
    },
    glossaryTermInfo: {
        definition: 'New glossary term',
        termSource: 'termSource',
        sourceRef: 'sourceRef',
        sourceURI: 'sourceURI',
    },
} as GlossaryTerm;

const glossaryTerm2 = {
    urn: 'urn:li:glossaryTerm:example.glossaryterm1',
    type: 'GLOSSARY_TERM',
    name: 'glossaryterm1',
    hierarchicalName: 'example.glossaryterm1',
    ownership: null,
    glossaryTermInfo: {
        definition: 'is A relation glossary term 1',
        termSource: 'INTERNAL',
        sourceRef: 'TERM_SOURCE_SAXO',
        sourceUrl: '',
        rawSchema: 'sample proto schema',
        customProperties: [
            {
                key: 'keyProperty',
                value: 'valueProperty',
                __typename: 'StringMapEntry',
            },
        ],
        __typename: 'GlossaryTermInfo',
    },
    isRealtedTerms: {
        start: 0,
        count: 0,
        total: 0,
        relationships: [
            {
                entity: {
                    urn: 'urn:li:glossaryTerm:schema.Field16Schema_v1',
                    __typename: 'GlossaryTerm',
                },
            },
        ],
        __typename: 'EntityRelationshipsResult',
    },
    hasRelatedTerms: {
        start: 0,
        count: 0,
        total: 0,
        relationships: [
            {
                entity: {
                    urn: 'urn:li:glossaryTerm:example.glossaryterm2',
                    __typename: 'GlossaryTerm',
                },
            },
        ],
        __typename: 'EntityRelationshipsResult',
    },
    __typename: 'GlossaryTerm',
};

const glossaryTerm3 = {
    urn: 'urn:li:glossaryTerm:example.glossaryterm2',
    type: 'GLOSSARY_TERM',
    name: 'glossaryterm2',
    hierarchicalName: 'example.glossaryterm2',
    ownership: null,
    glossaryTermInfo: {
        definition: 'has A relation glossary term 2',
        termSource: 'INTERNAL',
        sourceRef: 'TERM_SOURCE_SAXO',
        sourceUrl: '',
        rawSchema: 'sample proto schema',
        customProperties: [
            {
                key: 'keyProperty',
                value: 'valueProperty',
                __typename: 'StringMapEntry',
            },
        ],
        __typename: 'GlossaryTermInfo',
    },
    glossaryRelatedTerms: {
        isRelatedTerms: null,
        hasRelatedTerms: [
            {
                urn: 'urn:li:glossaryTerm:example.glossaryterm3',
                name: 'glossaryterm3',
                __typename: 'GlossaryTerm',
            },
            {
                urn: 'urn:li:glossaryTerm:example.glossaryterm4',
                name: 'glossaryterm4',
                __typename: 'GlossaryTerm',
            },
        ],
        __typename: 'GlossaryRelatedTerms',
    },
    __typename: 'GlossaryTerm',
} as GlossaryTerm;

/*
    Define mock data to be returned by Apollo MockProvider. 
*/
export const mocks2 = [
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
                        username: 'demo',
                        urn: 'any',
                    },
                },
            },
        },
    },
    {
        // this mock can be shifted elsewhere in the doc. need to create new mock instead of recycling cos it needs to be specific to query else it doesnt work
        request: {
            query: GetDatasetOwnersGqlDocument,
            variables: {
                urn: 'urn:li:dataset:3',
            },
        },
        result: {
            data: {
                dataset: {
                    urn: 'urn:li:dataset:3',
                    name: 'Yet Another Dataset again',
                    ownership: {
                        owners: [
                            {
                                owner: {
                                    urn: 'any',
                                    username: 'anyuser',
                                    __type: 'CorpUser',
                                },
                                type: 'DATAOWNER',
                            },
                        ],
                    },
                },
            },
        },
    },
];
