import { GetDatasetDocument, UpdateDatasetDocument } from './graphql/dataset.generated';
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
import { GetMeDocument } from './graphql/me.generated';
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
                    properties: {
                        name: 'abc-sample-tag',
                        description: 'sample tag',
                        colorHex: 'sample tag color',
                    },
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
                    properties: {
                        name: 'abc-sample-tag',
                        description: 'sample tag',
                        colorHex: 'sample tag color',
                    },
                },
            },
        ],
    },
};

const dataPlatform = {
    urn: 'urn:li:dataPlatform:hdfs',
    name: 'HDFS',
    type: EntityType.DataPlatform,
    properties: {
        displayName: 'HDFS',
        type: PlatformType.FileSystem,
        datasetNameDelimiter: '.',
        logoUrl: '',
    },
};

const dataset1 = {
    urn: 'urn:li:dataset:1',
    type: EntityType.Dataset,
    platform: {
        urn: 'urn:li:dataPlatform:hdfs',
        name: 'HDFS',
        type: EntityType.DataPlatform,
        properties: {
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
        name: 'The Great Test Dataset',
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
    domain: null,
    container: null,
    upstream: null,
    downstream: null,
    health: null,
    assertions: null,
    deprecation: null,
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
        name: 'Some Other Dataset',
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
    domain: null,
    container: null,
    upstream: null,
    downstream: null,
    health: null,
    assertions: null,
    status: null,
    deprecation: null,
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

export const dataset4 = {
    ...dataset3,
    name: 'Fourth Test Dataset',
    urn: 'urn:li:dataset:4',
    properties: {
        name: 'Fourth Test Dataset',
        description: 'This and here we have yet another Dataset (YAN). Are there more?',
        origin: 'PROD',
        customProperties: [{ key: 'propertyAKey', value: 'propertyAValue' }],
        externalUrl: 'https://data.hub',
    },
};

export const dataset5 = {
    ...dataset3,
    name: 'Fifth Test Dataset',
    urn: 'urn:li:dataset:5',
    properties: {
        name: 'Fifth Test Dataset',
        description: 'This and here we have yet another Dataset (YAN). Are there more?',
        origin: 'PROD',
        customProperties: [{ key: 'propertyAKey', value: 'propertyAValue' }],
        externalUrl: 'https://data.hub',
    },
};

export const dataset6 = {
    ...dataset3,
    name: 'Sixth Test Dataset',
    urn: 'urn:li:dataset:6',
    properties: {
        name: 'Display Name of Sixth',
        qualifiedName: 'Fully Qualified Name of Sixth Test Dataset',
        description: 'This and here we have yet another Dataset (YAN). Are there more?',
        origin: 'PROD',
        customProperties: [{ key: 'propertyAKey', value: 'propertyAValue' }],
        externalUrl: 'https://data.hub',
    },
};

export const dataset7 = {
    ...dataset3,
    name: 'Seventh Test Dataset',
    urn: 'urn:li:dataset:7',
    properties: {
        name: 'Seventh Test Dataset',
        description: 'This and here we have yet another Dataset (YAN). Are there more?',
        origin: 'PROD',
        customProperties: [{ key: 'propertyAKey', value: 'propertyAValue' }],
        externalUrl: 'https://data.hub',
    },
};

export const dataset3WithLineage = {
    ...dataset3,
    upstream: {
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
    downstream: {
        start: 0,
        count: 0,
        total: 0,
        relationships: [],
    },
};

export const dataset4WithLineage = {
    ...dataset4,
    upstream: {
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
    downstream: {
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
    upstream: {
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
    downstream: {
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
    upstream: null,
    downstream: {
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
    upstream: {
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
    downstream: {
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
    upstream: {
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
    downstream: {
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
    upstream: {
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
    downstream: {
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
        name: 'Another glossary term',
        description: 'New glossary term',
        definition: 'New glossary term',
        termSource: 'termSource',
        sourceRef: 'sourceRef',
        sourceURI: 'sourceURI',
    },
    properties: {
        name: 'Another glossary term',
        description: 'New glossary term',
        definition: 'New glossary term',
        termSource: 'termSource',
        sourceRef: 'sourceRef',
        sourceURI: 'sourceURI',
    },
    deprecation: null,
} as GlossaryTerm;

const glossaryTerm2 = {
    urn: 'urn:li:glossaryTerm:example.glossaryterm1',
    type: 'GLOSSARY_TERM',
    name: 'glossaryterm1',
    hierarchicalName: 'example.glossaryterm1',
    ownership: null,
    glossaryTermInfo: {
        name: 'glossaryterm1',
        description: 'is A relation glossary term 1',
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
    properties: {
        name: 'glossaryterm1',
        description: 'is A relation glossary term 1',
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
        __typename: 'GlossaryTermProperties',
    },
    deprecation: null,
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
        name: 'glossaryterm2',
        description: 'has A relation glossary term 2',
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
    properties: {
        name: 'glossaryterm2',
        description: 'has A relation glossary term 2',
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
        __typename: 'GlossaryTermProperties',
    },
    glossaryRelatedTerms: {
        isRelatedTerms: null,
        hasRelatedTerms: [
            {
                urn: 'urn:li:glossaryTerm:example.glossaryterm3',
                properties: {
                    name: 'glossaryterm3',
                },
                __typename: 'GlossaryTerm',
            },
            {
                urn: 'urn:li:glossaryTerm:example.glossaryterm4',
                properties: {
                    name: 'glossaryterm4',
                },
                __typename: 'GlossaryTerm',
            },
        ],
        __typename: 'GlossaryRelatedTerms',
    },
    deprecation: null,
    __typename: 'GlossaryTerm',
} as GlossaryTerm;

const sampleTag = {
    urn: 'urn:li:tag:abc-sample-tag',
    name: 'abc-sample-tag',
    description: 'sample tag description',
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
    properties: {
        name: 'abc-sample-tag',
        description: 'sample tag description',
        colorHex: 'sample tag color',
    },
};

export const dataFlow1 = {
    __typename: 'DataFlow',
    urn: 'urn:li:dataFlow:1',
    type: EntityType.DataFlow,
    orchestrator: 'Airflow',
    flowId: 'flowId1',
    cluster: 'cluster1',
    properties: {
        name: 'DataFlowInfoName',
        description: 'DataFlowInfo1 Description',
        project: 'DataFlowInfo1 project',
        externalUrl: null,
        customProperties: [],
    },
    editableProperties: null,
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
    platform: {
        ...dataPlatform,
    },
    domain: null,
    deprecation: null,
} as DataFlow;

export const dataJob1 = {
    __typename: 'DataJob',
    urn: 'urn:li:dataJob:1',
    type: EntityType.DataJob,
    dataFlow: dataFlow1,
    jobId: 'jobId1',
    ownership: {
        __typename: 'Ownership',
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
    properties: {
        name: 'DataJobInfoName',
        description: 'DataJobInfo1 Description',
        externalUrl: null,
        customProperties: [],
    },
    editableProperties: null,
    inputOutput: {
        __typename: 'DataJobInputOutput',
        inputDatasets: [dataset3],
        outputDatasets: [dataset3],
        inputDatajobs: [],
    },
    globalTags: {
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
    incoming: null,
    outgoing: null,
    upstream: null,
    downstream: null,
    parentFlow: {
        start: 0,
        count: 1,
        total: 1,
        relationships: [
            {
                type: 'IsPartOf',
                direction: RelationshipDirection.Outgoing,
                entity: dataFlow1,
            },
        ],
    },
    domain: null,
    status: null,
    deprecation: null,
} as DataJob;

export const dataJob2 = {
    __typename: 'DataJob',
    urn: 'urn:li:dataJob:2',
    type: EntityType.DataJob,
    dataFlow: dataFlow1,
    jobId: 'jobId2',
    ownership: {
        __typename: 'Ownership',
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
    properties: {
        name: 'DataJobInfoName2',
        description: 'DataJobInfo2 Description',
        externalUrl: null,
        customProperties: [],
    },
    editableProperties: null,
    inputOutput: {
        __typename: 'DataJobInputOutput',
        inputDatasets: [dataset3],
        outputDatasets: [dataset3],
        inputDatajobs: [dataJob1],
    },
    globalTags: {
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
    domain: null,
    upstream: null,
    downstream: null,
    deprecation: null,
} as DataJob;

export const dataJob3 = {
    __typename: 'DataJob',
    urn: 'urn:li:dataJob:3',
    type: EntityType.DataJob,
    dataFlow: dataFlow1,
    jobId: 'jobId3',
    ownership: {
        __typename: 'Ownership',
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
    properties: {
        name: 'DataJobInfoName3',
        description: 'DataJobInfo3 Description',
        externalUrl: null,
        customProperties: [],
    },
    editableProperties: null,
    inputOutput: {
        __typename: 'DataJobInputOutput',
        inputDatasets: [dataset3],
        outputDatasets: [dataset3],
        inputDatajobs: [dataJob2],
    },
    globalTags: {
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
    domain: null,
    upstream: null,
    downstream: null,
    status: null,
    deprecation: null,
} as DataJob;

export const mlModel = {
    __typename: 'MLModel',
    urn: 'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)',
    type: EntityType.Mlmodel,
    name: 'trust model',
    description: 'a ml trust model',
    origin: 'PROD',
    platform: {
        urn: 'urn:li:dataPlatform:kafka',
        name: 'Kafka',
        info: {
            type: PlatformType.MessageBroker,
            datasetNameDelimiter: '.',
            logoUrl: '',
        },
        type: EntityType.DataPlatform,
    },
    tags: [],
    properties: {
        description: 'a ml trust model',
        date: null,
        version: '1',
        type: 'model type',
        trainingMetrics: null,
        hyperParams: null,
        mlFeatures: null,
        groups: null,
        customProperties: null,
    },
    ownership: {
        __typename: 'Ownership',
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
    upstreamLineage: [],
    downstreamLineage: [],
    globalTags: {
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
    incoming: null,
    outgoing: null,
    upstream: null,
    downstream: null,
    status: null,
    deprecation: null,
} as MlModel;

export const mlModelGroup = {
    __typename: 'MLModelGroup',
    urn: 'urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,another-group,PROD)',
    type: EntityType.MlmodelGroup,
    name: 'trust model group',
    description: 'a ml trust model group',
    origin: 'PROD',
    platform: {
        urn: 'urn:li:dataPlatform:kafka',
        name: 'Kafka',
        info: {
            type: PlatformType.MessageBroker,
            datasetNameDelimiter: '.',
            logoUrl: '',
        },
        type: EntityType.DataPlatform,
    },
    ownership: {
        __typename: 'Ownership',
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
    upstreamLineage: null,
    downstreamLineage: null,
    globalTags: {
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
    incoming: null,
    outgoing: null,
    upstream: null,
    downstream: null,
    status: null,
    deprecation: null,
} as MlModelGroup;

export const recommendationModules = [
    {
        title: 'Most Popular',
        moduleId: 'MostPopular',
        renderType: RecommendationRenderType.EntityNameList,
        content: [
            {
                entity: {
                    ...dataset2,
                },
            },
        ],
    },
    {
        title: 'Top Platforms',
        moduleId: 'TopPlatforms',
        renderType: RecommendationRenderType.PlatformSearchList,
        content: [
            {
                entity: {
                    urn: 'urn:li:dataPlatform:snowflake',
                    type: EntityType.DataPlatform,
                    name: 'snowflake',
                    properties: {
                        displayName: 'Snowflake',
                        datasetNameDelimiter: '.',
                        logoUrl: null,
                    },
                    displayName: null,
                    info: null,
                },
                params: {
                    contentParams: {
                        count: 1,
                    },
                },
            },
        ],
    },
    {
        title: 'Popular Tags',
        moduleId: 'PopularTags',
        renderType: RecommendationRenderType.TagSearchList,
        content: [
            {
                entity: {
                    urn: 'urn:li:tag:TestTag',
                    name: 'TestTag',
                },
            },
        ],
    },
];

/*
    Define mock data to be returned by Apollo MockProvider. 
*/
export const mocks = [
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
            query: GetUserDocument,
            variables: {
                urn: 'urn:li:corpuser:1',
            },
        },
        result: {
            data: {
                corpUser: {
                    ...user1,
                },
            },
        },
    },
    {
        request: {
            query: GetUserDocument,
            variables: {
                urn: 'urn:li:corpuser:2',
            },
        },
        result: {
            data: {
                corpUser: {
                    ...user1,
                },
            },
        },
    },
    {
        request: {
            query: GetUserDocument,
            variables: {
                urn: 'urn:li:corpuser:datahub',
            },
        },
        result: {
            data: {
                corpUser: {
                    ...user1,
                },
            },
        },
    },
    {
        request: {
            query: GetBrowsePathsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    urn: 'urn:li:dataset:1',
                },
            },
        },
        result: {
            data: {
                browsePaths: [['prod', 'hdfs', 'datasets']],
            },
        },
    },
    {
        request: {
            query: GetBrowseResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    path: [],
                    start: 0,
                    count: 20,
                    filters: null,
                },
            },
        },
        result: {
            data: {
                browse: {
                    entities: [],
                    start: 0,
                    count: 0,
                    total: 0,
                    metadata: {
                        path: [],
                        groups: [
                            {
                                name: 'prod',
                                count: 1,
                            },
                        ],
                        totalNumEntities: 1,
                    },
                },
            },
        },
    },
    {
        request: {
            query: GetBrowseResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    path: ['prod', 'hdfs'],
                    start: 0,
                    count: 20,
                    filters: null,
                },
            },
        },
        result: {
            data: {
                browse: {
                    entities: [
                        {
                            __typename: 'Dataset',
                            ...dataset1,
                        },
                    ],
                    start: 0,
                    count: 1,
                    total: 1,
                    metadata: {
                        path: ['prod', 'hdfs'],
                        groups: [],
                        totalNumEntities: 0,
                    },
                },
            },
        },
    },
    {
        request: {
            query: GetBrowseResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    path: ['prod'],
                    start: 0,
                    count: 20,
                    filters: null,
                },
            },
        },
        result: {
            data: {
                browse: {
                    entities: [],
                    start: 0,
                    count: 0,
                    total: 0,
                    metadata: {
                        path: ['prod'],
                        groups: [
                            {
                                name: 'hdfs',
                                count: 1,
                            },
                        ],
                        totalNumEntities: 1,
                    },
                },
            },
        },
    },
    {
        request: {
            query: GetAutoCompleteMultipleResultsDocument,
            variables: {
                input: {
                    query: 't',
                },
            },
        },
        result: {
            data: {
                autoCompleteForMultiple: {
                    query: 't',
                    suggestions: [
                        {
                            type: EntityType.Dataset,
                            suggestions: ['The Great Test Dataset', 'Some Other Dataset'],
                            entities: [dataset1, dataset2],
                        },
                    ],
                },
            },
        },
    },
    {
        request: {
            query: GetAutoCompleteMultipleResultsDocument,
            variables: {
                input: {
                    query: 't',
                    limit: 30,
                },
            },
        },
        result: {
            data: {
                autoCompleteForMultiple: {
                    query: 't',
                    suggestions: [
                        {
                            type: EntityType.Dataset,
                            suggestions: ['The Great Test Dataset', 'Some Other Dataset'],
                            entities: [dataset1, dataset2],
                        },
                    ],
                },
            },
        },
    },
    {
        request: {
            query: GetAutoCompleteResultsDocument,
            variables: {
                input: {
                    type: 'USER',
                    query: 'j',
                },
            },
        },
        result: {
            data: {
                autoComplete: {
                    query: 'j',
                    suggestions: ['jjoyce'],
                    entities: [user1],
                },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [],
                },
            },
        },
        result: {
            data: {
                search: {
                    start: 0,
                    count: 3,
                    total: 3,
                    searchResults: [
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset1,
                            },
                            matchedFields: [
                                {
                                    name: 'fieldName',
                                    value: 'fieldValue',
                                },
                            ],
                            insights: [],
                        },
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset2,
                            },
                        },
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [{ value: 'PROD', count: 3, entity: null }],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'HDFS', count: 1, entity: null },
                                { value: 'MySQL', count: 1, entity: null },
                                { value: 'Kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [
                        {
                            field: 'platform',
                            value: 'kafka',
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
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            __typename: 'FacetMetadata',
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            __typename: 'FacetMetadata',
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'GLOSSARY_TERM',
                    query: 'tags:"abc-sample-tag" OR fieldTags:"abc-sample-tag" OR editedFieldTags:"abc-sample-tag"',
                    start: 0,
                    count: 1,
                    filters: [],
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
                                __typename: 'GLOSSARY_TERM',
                                ...glossaryTerm1,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsQuery,
        },
    },
    {
        request: {
            query: GetGlossaryTermDocument,
            variables: {
                urn: 'urn:li:glossaryTerm:example.glossaryterm1',
            },
        },
        result: {
            data: {
                glossaryTerm: { ...glossaryTerm2 },
            } as GetGlossaryTermQuery,
        },
    },
    {
        request: {
            query: GetGlossaryTermDocument,
            variables: {
                urn: 'urn:li:glossaryTerm:example.glossaryterm2',
            },
        },
        result: {
            data: {
                glossaryTerm: { ...glossaryTerm3 },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [
                        {
                            field: 'platform',
                            value: 'kafka',
                        },
                        {
                            field: 'platform',
                            value: 'hdfs',
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
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'USER',
                    query: 'Test',
                    start: 0,
                    count: 10,
                },
            },
        },
        result: {
            data: {
                search: {
                    start: 0,
                    count: 2,
                    total: 2,
                    searchResult: [
                        {
                            entity: {
                                ...user1,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                },
            },
        },
    },
    {
        request: {
            query: UpdateDatasetDocument,
            variables: {
                input: {
                    urn: 'urn:li:dataset:1',
                    ownership: {
                        owners: [
                            {
                                owner: 'urn:li:corpuser:1',
                                type: 'DATAOWNER',
                            },
                        ],
                        lastModified: {
                            time: 0,
                        },
                    },
                },
            },
        },
        result: {
            data: {
                dataset: {
                    urn: 'urn:li:corpuser:1',
                    ownership: {
                        owners: [
                            {
                                owner: {
                                    ...user1,
                                },
                                type: 'DATAOWNER',
                            },
                        ],
                        lastModified: {
                            time: 0,
                        },
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
                    type: 'CORP_USER',
                    query: 'tags:"abc-sample-tag" OR fieldTags:"abc-sample-tag" OR editedFieldTags:"abc-sample-tag"',
                    start: 0,
                    count: 1,
                    filters: [],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                search: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 0,
                    total: 2,
                    searchResults: [],
                    facets: [],
                },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    query: 'tags:"abc-sample-tag" OR fieldTags:"abc-sample-tag" OR editedFieldTags:"abc-sample-tag"',
                    start: 0,
                    count: 1,
                    filters: [],
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
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'DATASET',
                    query: '*',
                    start: 0,
                    count: 20,
                    filters: [],
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
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset4,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'DATA_FLOW',
                    query: 'Sample',
                    start: 0,
                    count: 10,
                    filters: [],
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
                                __typename: 'DataFlow',
                                ...dataFlow1,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'platform',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                {
                                    value: 'hdfs',
                                    count: 1,
                                    entity: null,
                                },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsQuery,
        },
    },
    {
        request: {
            query: GetDataFlowDocument,
            variables: {
                urn: 'urn:li:dataFlow:1',
            },
        },
        result: {
            data: {
                dataFlow: {
                    ...dataFlow1,
                    dataJobs: {
                        entities: [
                            {
                                created: {
                                    time: 0,
                                },
                                entity: dataJob1,
                            },
                            {
                                created: {
                                    time: 0,
                                },
                                entity: dataJob2,
                            },
                            {
                                created: {
                                    time: 0,
                                },
                                entity: dataJob3,
                            },
                        ],
                    },
                },
            },
        },
    },
    {
        request: {
            query: GetDataJobDocument,
            variables: {
                urn: 'urn:li:dataJob:1',
            },
        },
        result: {
            data: {
                dataJob: {
                    ...dataJob1,
                },
            },
        },
    },
    {
        request: {
            query: GetMlModelDocument,
            variables: {
                urn: 'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)',
            },
        },
        result: {
            data: {
                mlModel: {
                    ...mlModel,
                },
            },
        },
    },
    {
        request: {
            query: GetMlModelGroupDocument,
            variables: {
                urn: mlModelGroup.urn,
            },
        },
        result: {
            data: {
                mlModelGroup: {
                    ...mlModelGroup,
                },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsDocument,
            variables: {
                input: {
                    type: 'DATA_JOB',
                    query: 'Sample',
                    start: 0,
                    count: 10,
                    filters: [],
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
                                __typename: 'DataJob',
                                ...dataJob1,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'platform',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                {
                                    value: 'hdfs',
                                    count: 1,
                                    entity: null,
                                },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsQuery,
        },
    },
    {
        request: {
            query: GetTagDocument,
            variables: {
                urn: 'urn:li:tag:abc-sample-tag',
            },
        },
        result: {
            data: {
                tag: { ...sampleTag },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: ['DATASET'],
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [
                        {
                            field: 'platform',
                            value: 'kafka',
                        },
                    ],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                searchAcrossEntities: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [
                        {
                            __typename: 'SearchResult',
                            entity: {
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            __typename: 'FacetMetadata',
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    __typename: 'AggregationMetadata',
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            __typename: 'FacetMetadata',
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                {
                                    __typename: 'AggregationMetadata',
                                    value: 'hdfs',
                                    count: 1,
                                    entity: null,
                                },
                                {
                                    __typename: 'AggregationMetadata',
                                    value: 'mysql',
                                    count: 1,
                                    entity: null,
                                },
                                {
                                    __typename: 'AggregationMetadata',
                                    value: 'kafka',
                                    count: 1,
                                    entity: null,
                                },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: ['DATASET'],
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [
                        {
                            field: 'platform',
                            value: 'kafka',
                        },
                    ],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                searchAcrossEntities: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: ['DATA_JOB'],
                    query: 'Sample',
                    start: 0,
                    count: 10,
                    filters: [],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                searchAcrossEntities: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [
                        {
                            entity: {
                                __typename: 'DataJob',
                                ...dataJob1,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'platform',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                {
                                    value: 'hdfs',
                                    count: 1,
                                    entity: null,
                                },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: ['DATASET'],
                    query: 'tags:"abc-sample-tag" OR fieldTags:"abc-sample-tag" OR editedFieldTags:"abc-sample-tag"',
                    start: 0,
                    count: 1,
                    filters: [],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                searchAcrossEntities: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: ['DATASET'],
                    query: '*',
                    start: 0,
                    count: 20,
                    filters: [],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                searchAcrossEntities: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset4,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: ['DATASET'],
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [
                        {
                            field: 'platform',
                            value: 'kafka',
                        },
                        {
                            field: 'platform',
                            value: 'hdfs',
                        },
                    ],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                searchAcrossEntities: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: ['DATASET'],
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [
                        {
                            field: 'platform',
                            value: 'kafka',
                        },
                        {
                            field: 'platform',
                            value: 'hdfs',
                        },
                    ],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                searchAcrossEntities: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
    {
        request: {
            query: GetEntityCountsDocument,
            variables: {
                input: {
                    types: [EntityType.Dataset],
                },
            },
        },
        result: {
            data: {
                getEntityCounts: {
                    counts: [
                        {
                            entityType: EntityType.Dataset,
                            count: 10,
                        },
                    ],
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
            query: ListRecommendationsDocument,
            variables: {
                input: {
                    userUrn: user2.urn,
                    requestContext: {
                        scenario: ScenarioType.Home,
                    },
                    limit: 10,
                },
            },
        },
        result: {
            data: {
                listRecommendations: {
                    modules: [...recommendationModules],
                },
            },
        },
    },
    {
        request: {
            query: ListRecommendationsDocument,
            variables: {
                input: {
                    userUrn: user2.urn,
                    requestContext: {
                        scenario: ScenarioType.EntityProfile,
                        entityRequestContext: {
                            urn: dataset3.urn,
                            type: EntityType.Dataset,
                        },
                    },
                    limit: 3,
                },
            },
        },
        result: {
            data: {
                listRecommendations: {
                    modules: [...recommendationModules],
                },
            },
        },
    },
    {
        request: {
            query: ListRecommendationsDocument,
            variables: {
                input: {
                    userUrn: user2.urn,
                    requestContext: {
                        scenario: ScenarioType.SearchResults,
                        searchRequestContext: {
                            query: 'noresults',
                            filters: [],
                        },
                    },
                    limit: 3,
                },
            },
        },
        result: {
            data: {
                listRecommendations: {
                    modules: [...recommendationModules],
                },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: [],
                    query: 'noresults',
                    start: 0,
                    count: 10,
                    filters: [],
                },
            },
        },
        result: {
            data: {
                search: {
                    start: 0,
                    count: 0,
                    total: 0,
                    searchResults: [],
                    facets: [],
                },
            },
        },
    },
    {
        request: {
            query: GetEntityCountsDocument,
            variables: {
                input: {
                    types: [
                        EntityType.Dataset,
                        EntityType.Chart,
                        EntityType.Dashboard,
                        EntityType.DataFlow,
                        EntityType.GlossaryTerm,
                        EntityType.MlfeatureTable,
                        EntityType.Mlmodel,
                        EntityType.MlmodelGroup,
                    ],
                },
            },
        },
        result: {
            data: {
                getEntityCounts: {
                    counts: [
                        {
                            entityType: EntityType.Dataset,
                            count: 670,
                        },
                    ],
                },
            },
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: [],
                    query: '*',
                    start: 0,
                    count: 20,
                    filters: [],
                },
            },
        },
        result: {
            data: {
                __typename: 'Query',
                searchAcrossEntities: {
                    __typename: 'SearchResults',
                    start: 0,
                    count: 1,
                    total: 1,
                    searchResults: [
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                        {
                            entity: {
                                __typename: 'Dataset',
                                ...dataset4,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    facets: [
                        {
                            field: 'origin',
                            displayName: 'origin',
                            aggregations: [
                                {
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                        },
                    ],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
];
