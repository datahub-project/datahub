import React from 'react';

import { Entity } from '@app/entity/Entity';
import { VIEW_ENTITY_PAGE } from '@app/entity/shared/constants';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { ViewBuilderState } from '@app/entity/view/types';
import { EntityCapabilityType } from '@app/entityV2/Entity';
import { FetchedEntity } from '@app/lineage/types';
import { DEFAULT_APP_CONFIG } from '@src/appConfigContext';

import { AppConfigDocument, GetEntityCountsDocument } from '@graphql/app.generated';
import { GetBrowsePathsDocument, GetBrowseResultsDocument } from '@graphql/browse.generated';
import { GetDataFlowDocument } from '@graphql/dataFlow.generated';
import { GetDataJobDocument } from '@graphql/dataJob.generated';
import { GetDatasetDocument, GetDatasetSchemaDocument, UpdateDatasetDocument } from '@graphql/dataset.generated';
import { GetGlossaryTermDocument, GetGlossaryTermQuery } from '@graphql/glossaryTerm.generated';
import { GetMeDocument } from '@graphql/me.generated';
import { GetMlModelDocument } from '@graphql/mlModel.generated';
import { GetMlModelGroupDocument } from '@graphql/mlModelGroup.generated';
import { GetGrantedPrivilegesDocument } from '@graphql/policy.generated';
import { GetQuickFiltersDocument } from '@graphql/quickFilters.generated';
import { ListRecommendationsDocument } from '@graphql/recommendations.generated';
import {
    GetAutoCompleteMultipleResultsDocument,
    GetAutoCompleteResultsDocument,
    GetSearchResultsDocument,
    GetSearchResultsForMultipleDocument,
    GetSearchResultsForMultipleQuery,
    GetSearchResultsQuery,
} from '@graphql/search.generated';
import { GetTagDocument } from '@graphql/tag.generated';
import { GetUserDocument } from '@graphql/user.generated';
import {
    AppConfig,
    BusinessAttribute,
    Container,
    DataFlow,
    DataHubView,
    DataHubViewFilter,
    DataHubViewType,
    DataJob,
    Dataset,
    EntityPrivileges,
    EntityRelationshipsResult,
    EntityType,
    FilterOperator,
    GlobalTags,
    GlossaryNode,
    GlossaryTerm,
    LogicalOperator,
    Maybe,
    MlModel,
    MlModelGroup,
    Owner,
    OwnershipType,
    PlatformPrivileges,
    PlatformType,
    RecommendationRenderType,
    RelationshipDirection,
    ScenarioType,
    SchemaFieldDataType,
    SearchResult,
} from '@types';

export const entityPrivileges: EntityPrivileges = {
    canEditLineage: true,
    canEditDomains: true,
    canEditDataProducts: true,
    canEditTags: true,
    canEditGlossaryTerms: true,
    canEditDescription: true,
    canEditLinks: true,
    canEditOwners: true,
    canEditAssertions: true,
    canEditIncidents: true,
    canEditDeprecation: true,
    canEditSchemaFieldTags: true,
    canEditSchemaFieldGlossaryTerms: true,
    canEditSchemaFieldDescription: true,
    canEditQueries: true,
    canEditEmbed: true,
    canManageEntity: true,
    canManageChildren: true,
    canEditProperties: true,
    canViewDatasetUsage: true,
    canViewDatasetProfile: true,
    canViewDatasetOperations: true,
    __typename: 'EntityPrivileges',
};

export const user1 = {
    __typename: 'CorpUser',
    username: 'sdas',
    urn: 'urn:li:corpuser:1',
    type: EntityType.CorpUser,
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
    globalTags: {
        __typename: 'GlobalTags',
        tags: [
            {
                __typename: 'TagAssociation',
                tag: {
                    __typename: 'Tag',
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:abc-sample-tag',
                    name: 'abc-sample-tag',
                    description: 'sample tag',
                    properties: {
                        __typename: 'TagProperties',
                        name: 'abc-sample-tag',
                        description: 'sample tag',
                        colorHex: 'sample tag color',
                    },
                },
                associatedUrn: 'urn:li:corpuser:1',
            },
        ],
    },
    settings: {
        __typename: 'CorpUserSettings',
        appearance: { __typename: 'CorpUserAppearanceSettings', showSimplifiedHomepage: false, showThemeV2: false },
        views: { __typename: 'CorpUserViewSettings', defaultView: null },
    },
    editableInfo: null,
    properties: null,
    editableProperties: null,
    autoRenderAspects: [],
};

const user2 = {
    __typename: 'CorpUser',
    username: 'john',
    urn: 'urn:li:corpuser:3',
    type: EntityType.CorpUser,
    properties: {
        __typename: 'CorpUserInfo',
        email: 'john@domain.com',
        active: true,
        displayName: 'john',
        title: 'Eng',
        firstName: 'John',
        lastName: 'Joyce',
        fullName: 'John Joyce',
    },
    editableProperties: {
        displayName: 'Test',
        title: 'test',
        pictureLink: null,
        teams: [],
        skills: [],
        __typename: 'CorpUserEditableProperties',
        email: 'john@domain.com',
        persona: null,
        platforms: null,
    },
    groups: {
        __typename: 'EntityRelationshipsResult',
        relationships: [
            {
                __typename: 'EntityRelationship',
                entity: {
                    __typename: 'CorpGroup',
                    urn: 'urn:li:corpgroup:group1',
                    name: 'group1',
                    properties: null,
                },
            },
        ],
    },
    globalTags: {
        __typename: 'GlobalTags',
        tags: [
            {
                __typename: 'TagAssociation',
                tag: {
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:abc-sample-tag',
                    name: 'abc-sample-tag',
                    description: 'sample tag',
                    properties: {
                        __typename: 'TagProperties',
                        name: 'abc-sample-tag',
                        description: 'sample tag',
                        colorHex: 'sample tag color',
                    },
                },
                associatedUrn: 'urn:li:corpuser:3',
            },
        ],
    },
    settings: {
        __typename: 'CorpUserSettings',
        appearance: { __typename: 'CorpUserAppearanceSettings', showSimplifiedHomepage: false, showThemeV2: false },
        views: { __typename: 'CorpUserViewSettings', defaultView: null },
    },
    editableInfo: null,
    info: null,
};

export const dataPlatform = {
    urn: 'urn:li:dataPlatform:hdfs',
    name: 'HDFS',
    type: EntityType.DataPlatform,
    properties: {
        displayName: 'HDFS',
        type: PlatformType.FileSystem,
        datasetNameDelimiter: '.',
        logoUrl:
            'https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/lookerlogo.png',
    },
};

export const dataPlatformInstance = {
    urn: 'urn:li:dataPlatformInstance:(urn:li:dataPlatform:clickhouse,clickhousetestserver)',
    type: EntityType.DataPlatformInstance,
    instanceId: 'clickhousetestserver',
    platform: {
        type: 'DATA_PLATFORM',
        urn: 'urn:li:dataPlatform:clickhouse',
        properties: {
            displayName: 'ClickHouse',
            logoUrl: '/assets/platforms/clickhouselogo.png',
        },
    },
};

export const dataset1 = {
    __typename: 'Dataset',
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
            logoUrl:
                'https://raw.githubusercontent.com/datahub-project/datahub/master/datahub-web-react/src/images/lookerlogo.png',
        },
    },
    lastIngested: null,
    exists: true,
    dataPlatformInstance: null,
    platformNativeType: 'TABLE',
    name: 'The Great Test Dataset',
    origin: 'PROD',
    tags: ['Private', 'PII'],
    uri: 'www.google.com',
    privileges: {
        ...entityPrivileges,
    },
    properties: {
        name: 'The Great Test Dataset',
        description: 'This is the greatest dataset in the world, youre gonna love it!',
        customProperties: [
            {
                key: 'TestProperty',
                associatedUrn: 'urn:li:dataset:1',
                value: 'My property value.',
            },
            {
                associatedUrn: 'urn:li:dataset:1',
                key: 'AnotherTestProperty',
                value: 'My other property value.',
            },
        ],
        externalUrl: null,
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
                associatedUrn: 'urn:li:dataset:1',
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                associatedUrn: 'urn:li:dataset:1',
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
                associatedUrn: 'urn:li:dataset:1',
            },
        ],
    },
    usageStats: null,
    datasetProfiles: [
        {
            timestampMillis: 0,
            rowCount: 10,
            columnCount: 5,
            sizeInBytes: 10,
            fieldProfiles: [
                {
                    fieldPath: 'testColumn',
                },
            ],
        },
    ],
    domain: null,
    application: null,
    container: null,
    health: [],
    assertions: null,
    deprecation: null,
    testResults: null,
    statsSummary: null,
    embed: null,
    browsePathV2: { path: [{ name: 'test', entity: null }], __typename: 'BrowsePathV2' },
    autoRenderAspects: [],
    structuredProperties: null,
    forms: null,
    activeIncidents: null,
};

export const dataset2 = {
    __typename: 'Dataset',
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
    privileges: {
        ...entityPrivileges,
    },
    lastIngested: null,
    exists: true,
    dataPlatformInstance: null,
    platformNativeType: 'TABLE',
    name: 'Some Other Dataset',
    origin: 'PROD',
    tags: ['Outdated'],
    uri: 'www.google.com',
    properties: {
        name: 'Some Other Dataset',
        description: 'This is some other dataset, so who cares!',
        customProperties: [],
        origin: 'PROD',
        externalUrl: null,
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
                associatedUrn: 'urn:li:dataset:2',
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
                associatedUrn: 'urn:li:dataset:2',
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
            sizeInBytes: 10000,
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
    application: null,
    container: null,
    health: [],
    assertions: null,
    status: null,
    deprecation: null,
    testResults: null,
    statsSummary: null,
    embed: null,
    browsePathV2: { path: [{ name: 'test', entity: null }], __typename: 'BrowsePathV2' },
    autoRenderAspects: [],
    structuredProperties: null,
    forms: null,
    activeIncidents: null,
};

export const dataset3 = {
    __typename: 'Dataset',
    urn: 'urn:li:dataset:3',
    type: EntityType.Dataset,
    platform: {
        __typename: 'DataPlatform',
        urn: 'urn:li:dataPlatform:kafka',
        name: 'Kafka',
        displayName: 'Kafka',
        info: {
            __typename: 'DataPlatformInfo',
            displayName: 'Kafka',
            type: PlatformType.MessageBroker,
            datasetNameDelimiter: '.',
            logoUrl: '',
        },
        type: EntityType.DataPlatform,
        lastIngested: null,
        properties: null,
    },
    privileges: {
        ...entityPrivileges,
    },
    exists: true,
    lastIngested: null,
    dataPlatformInstance: null,
    platformNativeType: 'STREAM',
    name: 'Yet Another Dataset',
    origin: 'PROD',
    uri: 'www.google.com',
    properties: {
        __typename: 'DatasetProperties',
        name: 'Yet Another Dataset',
        qualifiedName: 'Yet Another Dataset',
        description: 'This and here we have yet another Dataset (YAN). Are there more?',
        origin: 'PROD',
        customProperties: [
            {
                __typename: 'CustomPropertiesEntry',
                key: 'propertyAKey',
                value: 'propertyAValue',
                associatedUrn: 'urn:li:dataset:3',
            },
        ],
        externalUrl: 'https://data.hub',
        lastModified: {
            __typename: 'AuditStamp',
            time: 0,
            actor: null,
        },
    },
    parentContainers: {
        __typename: 'ParentContainersResult',
        count: 0,
        containers: [],
    },
    editableProperties: null,
    created: {
        __typename: 'AuditStamp',
        time: 0,
        actor: null,
    },
    lastModified: {
        __typename: 'AuditStamp',
        time: 0,
        actor: null,
    },
    ownership: {
        __typename: 'Ownership',
        owners: [
            {
                __typename: 'Owner',
                owner: {
                    ...user1,
                },
                type: 'DATAOWNER',
                associatedUrn: 'urn:li:dataset:3',
                ownershipType: null,
            },
            {
                __typename: 'Owner',
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
                associatedUrn: 'urn:li:dataset:3',
                ownershipType: null,
            },
        ],
        lastModified: {
            __typename: 'AuditStamp',
            time: 0,
            actor: null,
        },
    },
    globalTags: {
        __typename: 'GlobalTags',
        tags: [
            {
                __typename: 'TagAssociation',
                tag: {
                    __typename: 'Tag',
                    type: EntityType.Tag,
                    urn: 'urn:li:tag:abc-sample-tag',
                    name: 'abc-sample-tag',
                    description: 'sample tag',
                    properties: {
                        __typename: 'TagProperties',
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
        __typename: 'GlossaryTerms',
        terms: [
            {
                __typename: 'GlossaryTermAssociation',
                term: {
                    __typename: 'GlossaryTerm',
                    type: EntityType.GlossaryTerm,
                    urn: 'urn:li:glossaryTerm:sample-glossary-term',
                    name: 'sample-glossary-term',
                    hierarchicalName: 'example.sample-glossary-term',
                    properties: {
                        __typename: 'GlossaryTermProperties',
                        name: 'sample-glossary-term',
                        description: 'sample definition',
                        definition: 'sample definition',
                        termSource: 'sample term source',
                        customProperties: null,
                    },
                    ownership: null,
                    parentNodes: null,
                },
                associatedUrn: 'urn:li:dataset:3',
                actor: {
                    __typename: 'CorpUser',
                    urn: 'urn:li:corpuser:admin',
                    type: EntityType.CorpUser,
                    username: '',
                },
            },
        ],
    },
    incoming: null,
    outgoing: null,
    institutionalMemory: {
        __typename: 'InstitutionalMemory',
        elements: [
            {
                __typename: 'InstitutionalMemoryMetadata',
                url: 'https://www.google.com',
                author: {
                    __typename: 'CorpUser',
                    urn: 'urn:li:corpuser:datahub',
                    username: 'datahub',
                    type: EntityType.CorpUser,
                },
                actor: {
                    __typename: 'CorpUser',
                    urn: 'urn:li:corpuser:datahub',
                    username: 'datahub',
                    type: EntityType.CorpUser,
                },
                description: 'This only points to Google',
                label: 'This only points to Google',
                created: {
                    __typename: 'AuditStamp',
                    actor: 'urn:li:corpuser:1',
                    time: 1612396473001,
                },
                associatedUrn: 'urn:li:dataset:3',
            },
        ],
    },
    deprecation: null,
    usageStats: null,
    latestFullTableProfile: null,
    latestPartitionProfile: null,
    operations: null,
    datasetProfiles: [
        {
            __typename: 'DatasetProfile',
            rowCount: 10,
            columnCount: 5,
            sizeInBytes: 10000,
            timestampMillis: 0,
            fieldProfiles: [
                {
                    __typename: 'DatasetFieldProfile',
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
            __typename: 'RawAspect',
            aspectName: 'autoRenderAspect',
            payload: '{ "values": [{ "autoField1": "autoValue1", "autoField2": "autoValue2" }] }',
            renderSpec: {
                __typename: 'AutoRenderSpec',
                displayType: 'tabular',
                displayName: 'Auto Render Aspect Custom Tab Name',
                key: 'values',
            },
        },
    ],
    domain: null,
    application: null,
    container: null,
    lineage: null,
    relationships: null,
    health: [],
    assertions: null,
    status: null,
    runs: null,
    testResults: null,
    siblings: null,
    siblingsSearch: null,
    statsSummary: null,
    embed: null,
    browsePathV2: { __typename: 'BrowsePathV2', path: [{ name: 'test', entity: null, __typename: 'BrowsePathEntry' }] },
    access: null,
    dataProduct: null,
    lastProfile: null,
    lastOperation: null,
    structuredProperties: null,
    forms: null,
    notes: [],
    activeIncidents: null,
    upstream: null,
    downstream: null,
    versionProperties: null,
} as Dataset;

export const dataset3WithSchema = {
    dataset: {
        __typename: 'Dataset',
        schemaMetadata: {
            __typename: 'SchemaMetadata',
            aspectVersion: 0,
            createdAt: 0,
            fields: [
                {
                    __typename: 'SchemaField',
                    nullable: false,
                    recursive: false,
                    fieldPath: 'user_id',
                    description: 'Id of the user created',
                    type: SchemaFieldDataType.String,
                    nativeDataType: 'varchar(100)',
                    isPartOfKey: false,
                    isPartitioningKey: false,
                    jsonPath: null,
                    globalTags: null,
                    glossaryTerms: null,
                    label: 'hi',
                    schemaFieldEntity: null,
                },
                {
                    __typename: 'SchemaField',
                    nullable: false,
                    recursive: false,
                    fieldPath: 'user_name',
                    description: 'Name of the user who signed up',
                    type: SchemaFieldDataType.String,
                    nativeDataType: 'boolean',
                    isPartOfKey: false,
                    isPartitioningKey: false,
                    jsonPath: null,
                    globalTags: null,
                    glossaryTerms: null,
                    label: 'hi',
                    schemaFieldEntity: null,
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
        documentation: null,
        siblings: null,
        siblingsSearch: null,
    },
};

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
        customProperties: [{ key: 'propertyAKey', value: 'propertyAValue', associatedUrn: 'urn:li:dataset:5' }],
        externalUrl: 'https://data.hub',
        lastModified: dataset3.properties?.lastModified,
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
        customProperties: [{ key: 'propertyAKey', value: 'propertyAValue', associatedUrn: 'urn:li:dataset:6' }],
        externalUrl: 'https://data.hub',
        lastModified: dataset3.properties?.lastModified,
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

export const container1 = {
    urn: 'urn:li:container:DATABASE',
    type: EntityType.Container,
    platform: dataPlatform,
    lastIngested: null,
    exists: true,
    properties: {
        name: 'database1',
        externalUrl: null,
        __typename: 'ContainerProperties',
    },
    autoRenderAspects: [],
    __typename: 'Container',
} as Container;

export const container2 = {
    urn: 'urn:li:container:SCHEMA',
    type: EntityType.Container,
    platform: dataPlatform,
    lastIngested: null,
    exists: true,
    properties: {
        name: 'schema1',
        externalUrl: null,
        __typename: 'ContainerProperties',
    },
    autoRenderAspects: [],
    __typename: 'Container',
} as Container;

export const glossaryTerm1 = {
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
                associatedUrn: 'urn:li:glossaryTerm:1',
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                associatedUrn: 'urn:li:glossaryTerm:1',
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
    parentNodes: null,
    deprecation: null,
    autoRenderAspects: [],
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
                __typename: 'CustomPropertiesEntry',
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
                associatedUrn: 'urn:li:glossaryTerm:example.glossaryterm1',
                __typename: 'CustomPropertiesEntry',
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
    parentNodes: null,
    autoRenderAspects: [],
    __typename: 'GlossaryTerm',
};

export const glossaryTerm3 = {
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
                associatedUrn: 'urn:li:glossaryTerm:example.glossaryterm2',
                __typename: 'CustomPropertiesEntry',
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
                associatedUrn: 'urn:li:glossaryTerm:example.glossaryterm2',
                __typename: 'CustomPropertiesEntry',
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
    autoRenderAspects: [],
    __typename: 'GlossaryTerm',
} as GlossaryTerm;

export const glossaryNode1 = {
    urn: 'urn:li:glossaryNode:example.glossarynode1',
    type: 'GLOSSARY_NODE',
    properties: {
        name: 'Glossary Node 1',
    },
    parentNodes: {
        count: 0,
        nodes: [],
    },
    __typename: 'GlossaryNode',
} as GlossaryNode;

export const glossaryNode2 = {
    urn: 'urn:li:glossaryNode:example.glossarynode2',
    type: 'GLOSSARY_NODE',
    properties: {
        name: 'Glossary Node 2',
    },
    parentNodes: {
        count: 1,
        nodes: [glossaryNode1],
    },
    __typename: 'GlossaryNode',
} as GlossaryNode;

export const glossaryNode3 = {
    urn: 'urn:li:glossaryNode:example.glossarynode3',
    type: 'GLOSSARY_NODE',
    properties: {
        name: 'Glossary Node 3',
    },
    parentNodes: {
        count: 2,
        nodes: [glossaryNode2, glossaryNode1],
    },
    __typename: 'GlossaryNode',
} as GlossaryNode;

export const glossaryNode4 = {
    urn: 'urn:li:glossaryNode:example.glossarynode4',
    type: 'GLOSSARY_NODE',
    properties: {
        name: 'Glossary Node 4',
    },
    parentNodes: {
        count: 0,
        nodes: [],
    },
    __typename: 'GlossaryNode',
} as GlossaryNode;

export const glossaryNode5 = {
    urn: 'urn:li:glossaryNode:example.glossarynode5',
    type: 'GLOSSARY_NODE',
    properties: {
        name: 'Glossary Node 5',
    },
    parentNodes: {
        count: 1,
        nodes: [glossaryNode4],
    },
    __typename: 'GlossaryNode',
} as GlossaryNode;

export const sampleTag = {
    urn: 'urn:li:tag:abc-sample-tag',
    type: EntityType.Tag,
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
    autoRenderAspects: [],
};

export const dataFlow1 = {
    __typename: 'DataFlow',
    urn: 'urn:li:dataFlow:1',
    type: EntityType.DataFlow,
    orchestrator: 'Airflow',
    flowId: 'flowId1',
    cluster: 'cluster1',
    lastIngested: null,
    exists: true,
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
                associatedUrn: 'urn:li:dataFlow:1',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
                associatedUrn: 'urn:li:dataFlow:1',
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
                associatedUrn: 'urn:li:dataFlow:1',
            },
        ],
    },
    platform: {
        urn: 'urn:li:dataPlatform:airflow',
        name: 'Airflow',
        type: EntityType.DataPlatform,
        properties: {
            displayName: 'Airflow',
            type: PlatformType.FileSystem,
            datasetNameDelimiter: '.',
            logoUrl: '',
        },
    },
    domain: null,
    application: null,
    deprecation: null,
    autoRenderAspects: [],
    activeIncidents: null,
    health: [],
} as DataFlow;

export const dataJob1 = {
    __typename: 'DataJob',
    urn: 'urn:li:dataJob:1',
    type: EntityType.DataJob,
    dataFlow: dataFlow1,
    jobId: 'jobId1',
    lastIngested: null,
    exists: true,
    ownership: {
        __typename: 'Ownership',
        owners: [
            {
                owner: {
                    ...user1,
                },
                associatedUrn: 'urn:li:dataJob:1',
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                associatedUrn: 'urn:li:dataJob:1',
                type: 'DELEGATE',
            },
        ],
        lastModified: {
            time: 0,
        },
    },
    privileges: {
        ...entityPrivileges,
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
        inputDatasets: [dataset5],
        outputDatasets: [dataset6],
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
                associatedUrn: 'urn:li:dataJob:1',
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
    application: null,
    status: null,
    deprecation: null,
    autoRenderAspects: [],
    activeIncidents: null,
    health: [],
} as DataJob;

export const businessAttribute = {
    urn: 'urn:li:businessAttribute:ba1',
    type: EntityType.BusinessAttribute,
    __typename: 'BusinessAttribute',
    properties: {
        name: 'TestBusinessAtt-2',
        description: 'lorem upsum updated 12',
        created: {
            time: 1705857132786,
        },
        lastModified: {
            time: 1705857132786,
        },
        glossaryTerms: {
            terms: [
                {
                    term: {
                        urn: 'urn:li:glossaryTerm:1',
                        type: EntityType.GlossaryTerm,
                        hierarchicalName: 'SampleHierarchicalName',
                        name: 'SampleName',
                    },
                    associatedUrn: 'urn:li:businessAttribute:ba1',
                },
            ],
            __typename: 'GlossaryTerms',
        },
        tags: {
            __typename: 'GlobalTags',
            tags: [
                {
                    tag: {
                        urn: 'urn:li:tag:abc-sample-tag',
                        __typename: 'Tag',
                        type: EntityType.Tag,
                        name: 'abc-sample-tag',
                    },
                    __typename: 'TagAssociation',
                    associatedUrn: 'urn:li:businessAttribute:ba1',
                },
                {
                    tag: {
                        urn: 'urn:li:tag:TestTag',
                        __typename: 'Tag',
                        type: EntityType.Tag,
                        name: 'TestTag',
                    },
                    __typename: 'TagAssociation',
                    associatedUrn: 'urn:li:businessAttribute:ba1',
                },
            ],
        },
        customProperties: [
            {
                key: 'prop2',
                value: 'val2',
                associatedUrn: 'urn:li:businessAttribute:ba1',
                __typename: 'CustomPropertiesEntry',
            },
            {
                key: 'prop1',
                value: 'val1',
                associatedUrn: 'urn:li:businessAttribute:ba1',
                __typename: 'CustomPropertiesEntry',
            },
            {
                key: 'prop3',
                value: 'val3',
                associatedUrn: 'urn:li:businessAttribute:ba1',
                __typename: 'CustomPropertiesEntry',
            },
        ],
    },
    ownership: {
        owners: [
            {
                owner: {
                    ...user1,
                },
                associatedUrn: 'urn:li:businessAttribute:ba',
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                associatedUrn: 'urn:li:businessAttribute:ba',
                type: 'DELEGATE',
            },
        ],
        lastModified: {
            time: 0,
        },
    },
} as BusinessAttribute;

export const dataJob2 = {
    __typename: 'DataJob',
    urn: 'urn:li:dataJob:2',
    type: EntityType.DataJob,
    dataFlow: dataFlow1,
    jobId: 'jobId2',
    privileges: {
        ...entityPrivileges,
    },
    ownership: {
        __typename: 'Ownership',
        owners: [
            {
                owner: {
                    ...user1,
                },
                associatedUrn: 'urn:li:dataJob:2',
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                associatedUrn: 'urn:li:dataJob:2',
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
                associatedUrn: 'urn:li:dataJob:2',
            },
        ],
    },
    domain: null,
    application: null,
    upstream: null,
    downstream: null,
    deprecation: null,
    autoRenderAspects: [],
    activeIncidents: null,
    health: [],
} as DataJob;

export const dataJob3 = {
    __typename: 'DataJob',
    urn: 'urn:li:dataJob:3',
    type: EntityType.DataJob,
    dataFlow: dataFlow1,
    jobId: 'jobId3',
    lastIngested: null,
    exists: true,
    privileges: {
        ...entityPrivileges,
    },
    ownership: {
        __typename: 'Ownership',
        owners: [
            {
                owner: {
                    ...user1,
                },
                associatedUrn: 'urn:li:dataJob:3',
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                associatedUrn: 'urn:li:dataJob:3',
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
                associatedUrn: 'urn:li:dataJob:3',
            },
        ],
    },
    domain: null,
    application: null,
    upstream: null,
    downstream: null,
    status: null,
    deprecation: null,
    autoRenderAspects: [],
    activeIncidents: null,
    health: [],
} as DataJob;

export const mlModel = {
    __typename: 'MLModel',
    urn: 'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)',
    type: EntityType.Mlmodel,
    name: 'trust model',
    description: 'a ml trust model',
    origin: 'PROD',
    lastIngested: null,
    exists: true,
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
        name: 'trust model',
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
                associatedUrn: 'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)',
            },
            {
                owner: {
                    ...user2,
                },
                type: 'DELEGATE',
                associatedUrn: 'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)',
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
                associatedUrn: 'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,trustmodel,PROD)',
            },
        ],
    },
    incoming: null,
    outgoing: null,
    upstream: null,
    downstream: null,
    status: null,
    deprecation: null,
    autoRenderAspects: [],
} as MlModel;

export const dataset1FetchedEntity = {
    urn: dataset1.urn,
    name: dataset1.name,
    type: dataset1.type,
    upstreamChildren: [],
    downstreamChildren: [
        { type: EntityType.Dataset, entity: dataset2 },
        { type: EntityType.DataJob, entity: dataJob1 },
    ],
} as FetchedEntity;

export const dataset2FetchedEntity = {
    urn: dataset2.urn,
    name: 'test name',
    type: dataset2.type,
    upstreamChildren: [
        { type: EntityType.Dataset, entity: dataset1 },
        { type: EntityType.DataJob, entity: dataJob1 },
    ],
    downstreamChildren: [],
} as FetchedEntity;

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
                associatedUrn: 'urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,another-group,PROD)',
                type: 'DATAOWNER',
            },
            {
                owner: {
                    ...user2,
                },
                associatedUrn: 'urn:li:mlModelGroup:(urn:li:dataPlatform:sagemaker,another-group,PROD)',
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
        newData: () => ({
            data: {
                dataset: {
                    ...dataset3,
                },
            },
        }),
    },
    {
        request: {
            query: GetDatasetSchemaDocument,
            variables: {
                urn: 'urn:li:dataset:3',
            },
        },
        result: {
            data: {
                ...dataset3WithSchema,
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
                    filters: [],
                    orFilters: [],
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
                    filters: [],
                    orFilters: [],
                },
            },
        },
        result: {
            data: {
                browse: {
                    entities: [
                        {
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
                    filters: [],
                    orFilters: [],
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
                    limit: 10,
                    filters: [],
                    types: [],
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
                    limit: 10,
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
                    orFilters: [],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                            entity: null,
                        },
                        {
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'HDFS', count: 1, entity: null },
                                { value: 'MySQL', count: 1, entity: null },
                                { value: 'Kafka', count: 1, entity: null },
                            ],
                            entity: null,
                        },
                    ],
                    suggestions: [],
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
                    filters: [],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: 'platform',
                                    values: ['kafka'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                            ],
                        },
                    ],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                                    value: 'PROD',
                                    count: 3,
                                    entity: null,
                                },
                            ],
                            entity: null,
                        },
                        {
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
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
                            entity: null,
                        },
                    ],
                    suggestions: [],
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
                    orFilters: [],
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
                            entity: null,
                        },
                        {
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                            entity: null,
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
                    filters: [],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: 'platform',
                                    values: ['kafka', 'hdfs'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                            ],
                        },
                    ],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                            __typename: 'SearchResult',
                            entity: {
                                __typename: 'Dataset',
                                ...dataset3,
                            },
                            matchedFields: [],
                            insights: [],
                        },
                    ],
                    suggestions: [],
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
                            entity: null,
                        },
                        {
                            __typename: 'FacetMetadata',
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            __typename: 'FacetMetadata',
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null, __typename: 'AggregationMetadata' },
                                { value: 'mysql', count: 1, entity: null, __typename: 'AggregationMetadata' },
                                { value: 'kafka', count: 1, entity: null, __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
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
                    orFilters: [],
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
                    orFilters: [],
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
                            entity: null,
                        },
                        {
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                            entity: null,
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
                    orFilters: [],
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
                            entity: null,
                        },
                        {
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                            entity: null,
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
                    orFilters: [],
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
                            entity: null,
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
                            entity: null,
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
                    orFilters: [],
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
                            entity: null,
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
                            entity: null,
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
                    types: [],
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: '_entityType',
                                    values: ['DATASET'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                                {
                                    field: 'platform',
                                    values: ['kafka'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                            ],
                        },
                    ],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                    suggestions: [],
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
                            entity: null,
                        },
                        // {
                        //     displayName: 'Domain',
                        //     field: 'domains',
                        //     __typename: 'FacetMetadata',
                        //     aggregations: [
                        //         {
                        //             value: 'urn:li:domain:baedb9f9-98ef-4846-8a0c-2a88680f213e',
                        //             count: 1,
                        //             __typename: 'AggregationMetadata',
                        //         },
                        //     ],
                        // },
                        {
                            __typename: 'FacetMetadata',
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
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
                            entity: null,
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
                    types: [],
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: '_entityType',
                                    values: ['DATASET'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                                {
                                    field: 'platform',
                                    values: ['kafka'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                            ],
                        },
                    ],
                    searchFlags: { getSuggestions: true },
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
                    facets: [],
                    suggestions: [],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
    {
        request: {
            query: GetSearchResultsForMultipleDocument,
            variables: {
                input: {
                    types: [],
                    query: 'Sample',
                    start: 0,
                    count: 10,
                    filters: [],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: '_entityType',
                                    values: ['DATA_JOB'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                            ],
                        },
                    ],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                    suggestions: [],
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
                            entity: null,
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
                            entity: null,
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
                    types: [''],
                    query: 'tags:"abc-sample-tag" OR fieldTags:"abc-sample-tag" OR editedFieldTags:"abc-sample-tag"',
                    start: 0,
                    count: 1,
                    filters: [],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: '_entityType',
                                    values: ['DATASET'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                            ],
                        },
                    ],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                    suggestions: [],
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
                            entity: null,
                        },
                        {
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                            entity: null,
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
                    types: [''],
                    query: '*',
                    start: 0,
                    count: 20,
                    filters: [],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: '_entityType',
                                    values: ['DATASET'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                            ],
                        },
                    ],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                    suggestions: [],
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
                            entity: null,
                        },
                        {
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                            entity: null,
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
                    types: [],
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: '_entityType',
                                    values: ['DATASET'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                                {
                                    field: 'platform',
                                    values: ['kafka', 'hdfs'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                            ],
                        },
                    ],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                    suggestions: [],
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
                            entity: null,
                        },
                        {
                            __typename: 'FacetMetadata',
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            __typename: 'FacetMetadata',
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null, __typename: 'AggregationMetadata' },
                                { value: 'mysql', count: 1, entity: null, __typename: 'AggregationMetadata' },
                                { value: 'kafka', count: 1, entity: null, __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
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
                    types: [],
                    query: 'test',
                    start: 0,
                    count: 10,
                    filters: [],
                    orFilters: [
                        {
                            and: [
                                {
                                    field: '_entityType',
                                    values: ['DATASET'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                                {
                                    field: 'platform',
                                    values: ['kafka', 'hdfs'],
                                    negated: false,
                                    condition: FilterOperator.Equal,
                                },
                            ],
                        },
                    ],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                    suggestions: [],
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
                                    __typename: 'AggregationMetadata',
                                },
                            ],
                            entity: null,
                        },
                        {
                            __typename: 'FacetMetadata',
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            __typename: 'FacetMetadata',
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null, __typename: 'AggregationMetadata' },
                                { value: 'mysql', count: 1, entity: null, __typename: 'AggregationMetadata' },
                                { value: 'kafka', count: 1, entity: null, __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
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
                        __typename: 'PlatformPrivileges',
                        viewAnalytics: true,
                        managePolicies: true,
                        manageIdentities: true,
                        manageDomains: true,
                        manageTags: true,
                        viewManageTags: true,
                        createDomains: true,
                        createTags: true,
                        manageUserCredentials: true,
                        manageGlossaries: true,
                        viewTests: false,
                        manageTests: true,
                        manageTokens: true,
                        manageSecrets: true,
                        manageIngestion: true,
                        generatePersonalAccessTokens: true,
                        manageGlobalViews: true,
                        manageOwnershipTypes: true,
                        manageGlobalAnnouncements: true,
                        createBusinessAttributes: true,
                        manageBusinessAttributes: true,
                        manageStructuredProperties: true,
                        viewStructuredPropertiesPage: true,
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
                    orFilters: [],
                    searchFlags: { getSuggestions: true, includeStructuredPropertyFacets: true },
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
                    suggestions: [],
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
                        EntityType.DataProduct,
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
                    count: 6,
                    filters: [],
                    orFilters: [],
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
                            entity: null,
                        },
                        {
                            field: '_entityType',
                            displayName: 'Type',
                            aggregations: [
                                { count: 37, entity: null, value: 'DATASET', __typename: 'AggregationMetadata' },
                                { count: 7, entity: null, value: 'CHART', __typename: 'AggregationMetadata' },
                            ],
                            entity: null,
                        },
                        {
                            field: 'platform',
                            displayName: 'platform',
                            aggregations: [
                                { value: 'hdfs', count: 1, entity: null },
                                { value: 'mysql', count: 1, entity: null },
                                { value: 'kafka', count: 1, entity: null },
                            ],
                            entity: null,
                        },
                    ],
                },
            } as GetSearchResultsForMultipleQuery,
        },
    },
    {
        request: {
            query: GetQuickFiltersDocument,
            variables: {
                input: {},
            },
        },
        result: {
            data: [],
        },
    },
    {
        request: {
            query: GetGrantedPrivilegesDocument,
            variables: {
                input: {
                    actorUrn: 'urn:li:corpuser:3',
                    resourceSpec: { resourceType: EntityType.Dataset, resourceUrn: dataset3.urn },
                },
            },
        },
        result: {
            data: { getGrantedPrivileges: { privileges: [VIEW_ENTITY_PAGE] } },
        },
    },
];

export const mocksWithSearchFlagsOff = [
    ...mocks,
    {
        request: {
            query: AppConfigDocument,
        },
        result: {
            data: {
                appConfig: {
                    ...DEFAULT_APP_CONFIG,
                    featureFlags: {
                        ...DEFAULT_APP_CONFIG.featureFlags,
                        showSearchFiltersV2: false,
                    },
                } as AppConfig,
            },
        },
    },
];

export const platformPrivileges: PlatformPrivileges = {
    viewAnalytics: true,
    managePolicies: true,
    manageIdentities: true,
    generatePersonalAccessTokens: true,
    manageDomains: true,
    manageIngestion: true,
    manageSecrets: true,
    manageTokens: true,
    viewTests: false,
    manageTests: true,
    manageGlossaries: true,
    manageUserCredentials: true,
    manageTags: true,
    viewManageTags: true,
    createTags: true,
    createDomains: true,
    manageGlobalViews: true,
    manageOwnershipTypes: true,
    manageGlobalAnnouncements: true,
    createBusinessAttributes: true,
    manageBusinessAttributes: true,
    manageStructuredProperties: true,
    viewStructuredPropertiesPage: true,
};

export const DomainMock1 = {
    urn: 'urn:li:domain:afbdad41-c523-469f-9b62-de94f938f702',
    id: 'afbdad41-c523-469f-9b62-de94f938f702',
    type: 'DOMAIN',
    icon: () => <></>,
    isSearchEnabled: () => false,
    isBrowseEnabled: () => false,
    isLineageEnabled: () => false,
    getCollectionName: () => 'domain1_mock_1',
    getPathName: () => 'domain_path_1',
    getGraphName: () => 'domain_graph_1',
    displayName: () => 'MOCK_DOMAIN_1',
    parentDomains: {
        domains: [],
    },
    renderProfile: () => <></>,
    renderPreview: () => <></>,
    renderSearch: () => <></>,
    getGenericEntityProperties: () => {
        return {
            parentDomains: {
                count: 1,
                domains: [
                    {
                        urn: 'urn:li:domain:afbdad41-c523-469f-9b62-de94f938f702',
                        type: 'DOMAIN',
                        name: 'DOMAIN_1',
                    },
                ],
            },
        };
    },
    supportedCapabilities: () => new Set(),
} as Entity<any>;

export const DomainMock2 = {
    urn: 'urn:li:domain:bebdad41-c523-469f-9b62-de94f938f603',
    id: 'bebdad41-c523-469f-9b62-de94f938f603',
    type: 'DOMAIN',
    icon: () => <></>,
    isSearchEnabled: () => false,
    isBrowseEnabled: () => false,
    isLineageEnabled: () => false,
    getCollectionName: () => 'domain_mock_2',
    getPathName: () => 'domain_path_2',
    getGraphName: () => 'domain_graph_2',
    displayName: () => 'MOCK_DOMAIN_2',
    parentDomains: {
        domains: [],
    },
    renderProfile: () => <></>,
    renderPreview: () => <></>,
    renderSearch: () => <></>,
    getGenericEntityProperties: () => {
        return {
            parentDomains: {
                count: 1,
                domains: [
                    {
                        urn: 'urn:li:domain:afbdad41-c523-469f-9b62-de94f938f603',
                        type: 'DOMAIN',
                        name: 'DOMAIN_2',
                    },
                ],
            },
        };
    },
    supportedCapabilities: () => new Set(),
} as Entity<any>;

export const DomainMock3 = [DomainMock1, DomainMock2] as Array<Entity<any>>;

export const expectedResult = [
    {
        type: 'DOMAIN',
        urn: 'urn:li:domain:afbdad41-c523-469f-9b62-de94f938f702',
        name: 'DOMAIN_1',
    },
];

export const owners: Owner[] = [
    {
        __typename: 'Owner',
        owner: {
            __typename: 'CorpUser',
            username: 'john',
            urn: 'urn:li:corpuser:3',
            type: EntityType.CorpUser,
        },
        associatedUrn: 'urn:li:dataset:1',
        type: OwnershipType.Developer,
    },
    {
        owner: {
            __typename: 'CorpUser',
            username: 'john',
            urn: 'urn:li:corpuser:3',
            type: EntityType.CorpUser,
        },
        associatedUrn: 'urn:li:dataset:1',
        type: OwnershipType.Delegate,
    },
    {
        owner: {
            __typename: 'CorpUser',
            username: 'sdas',
            urn: 'urn:li:corpuser:1',
            type: EntityType.CorpUser,
        },
        associatedUrn: 'urn:li:dataset:2',
        type: OwnershipType.Dataowner,
    },
    {
        owner: {
            __typename: 'CorpUser',
            username: 'sdas',
            urn: 'urn:li:corpuser:1',
            type: EntityType.CorpUser,
        },
        type: OwnershipType.Delegate,
        associatedUrn: 'urn:li:dataset:2',
    },
];

export const globalTags: GlobalTags = {
    __typename: 'GlobalTags',
    tags: [
        {
            __typename: 'TagAssociation',
            tag: {
                __typename: 'Tag',
                type: EntityType.Tag,
                urn: 'urn:li:tag:abc-sample-tag',
                name: 'abc-sample-tag',
                description: 'sample tag',
                properties: {
                    __typename: 'TagProperties',
                    name: 'abc-sample-tag',
                    description: 'sample tag',
                    colorHex: 'sample tag color',
                },
            },
            associatedUrn: 'urn:li:corpuser:1',
        },
    ],
};

export const entityCapabilities: Set<EntityCapabilityType> = new Set([
    EntityCapabilityType.OWNERS,
    EntityCapabilityType.GLOSSARY_TERMS,
    EntityCapabilityType.TAGS,
    EntityCapabilityType.DOMAINS,
    EntityCapabilityType.DEPRECATION,
    EntityCapabilityType.SOFT_DELETE,
    EntityCapabilityType.TEST,
    EntityCapabilityType.ROLES,
    EntityCapabilityType.DATA_PRODUCTS,
    EntityCapabilityType.HEALTH,
    EntityCapabilityType.LINEAGE,
]);

const filters: DataHubViewFilter = {
    filters: [
        {
            condition: FilterOperator.Equal,
            field: 'mockField1',
            negated: false,
            values: ['value1', 'value2', 'value3'],
        },
        {
            condition: FilterOperator.Exists,
            field: 'mockField2',
            negated: true,
            values: ['value4', 'value5', 'value6'],
        },
    ],
    operator: LogicalOperator.And,
};

export const viewBuilderStateMock: ViewBuilderState = {
    viewType: DataHubViewType.Global,
    name: 'VIEW_BUILDER_TEST',
    description: 'A description for testing convertStateToUpdateInput',
    definition: {
        entityTypes: [EntityType.AccessToken, EntityType.Domain, EntityType.Container, EntityType.DataFlow],
        filter: filters,
    },
};

export const searchViewsMock: Array<DataHubView> = [
    {
        urn: 'test-urn1',
        type: EntityType.DatahubView,
        viewType: DataHubViewType.Global,
        name: 'VIEW_BUILDER_TEST',
        description: 'A description for testing convertStateToUpdateInput',
        definition: {
            entityTypes: [EntityType.AccessToken, EntityType.Domain, EntityType.Container, EntityType.DataFlow],
            filter: {
                operator: LogicalOperator.And,
                filters: [
                    {
                        field: 'mockField1',
                        condition: FilterOperator.Equal,
                        values: ['value1', 'value2', 'value3'],
                        negated: false,
                    },
                    {
                        field: 'mockField2',
                        condition: FilterOperator.Exists,
                        values: ['value4', 'value5', 'value6'],
                        negated: true,
                    },
                ],
            },
        },
    },
    {
        urn: 'test-urn2',
        type: EntityType.DatahubView,
        viewType: DataHubViewType.Global,
        name: 'MOCK_TEST_VIEW',
        description: 'Lorem ipsum dolor sit amet, consectetu',
        definition: {
            entityTypes: [EntityType.AccessToken, EntityType.Container, EntityType.DataFlow],
            filter: {
                operator: LogicalOperator.Or,
                filters: [
                    {
                        field: 'mockField1',
                        condition: FilterOperator.GreaterThan,
                        values: ['value1', 'value2', 'value3'],
                        negated: false,
                    },
                    {
                        field: 'mockField2',
                        condition: FilterOperator.In,
                        values: ['value4', 'value6'],
                        negated: false,
                    },
                ],
            },
        },
    },
];

export const mockEntityRelationShipResult: Maybe<EntityRelationshipsResult> = {
    start: 0,
    count: 0,
    total: 0,
    relationships: [
        {
            type: 'Test1',
            direction: RelationshipDirection.Outgoing,
            entity: {
                urn: 'urn:li:glossaryTerm:schema.Field16Schema_v1',
                type: EntityType.GlossaryTerm,
            },
        },
        {
            type: 'Test2',
            direction: RelationshipDirection.Incoming,
            entity: {
                urn: 'urn:li:glossaryTerm:schema.Field16Schema_v2',
                type: EntityType.Assertion,
            },
        },
    ],
    __typename: 'EntityRelationshipsResult',
};

export const mockSearchResult: SearchResult = {
    __typename: 'SearchResult',
    entity: {
        __typename: 'Dataset',
        ...dataset3,
    },
    matchedFields: [],
    insights: [],
    extraProperties: [
        { name: 'isOutputPort', value: 'true' },
        { name: 'test2_name', value: 'test2_value' },
    ],
};

export const mockRecord: Record<string, string | boolean> = {
    key1: 'value1',
    key2: true,
    key3: 'value2',
    key4: false,
    key5: 'value3',
    key6: true,
};

export const mockFineGrainedLineages1: GenericEntityProperties = {
    siblings: {
        isPrimary: true,
        siblings: [{ type: EntityType.Dataset, urn: 'test_urn' }],
    },
    siblingsSearch: {
        count: 1,
        total: 1,
        searchResults: [{ entity: { type: EntityType.Dataset, urn: 'test_urn' }, matchedFields: [] }],
    },
    fineGrainedLineages: [
        {
            upstreams: [
                {
                    urn: 'urn:li:glossaryTerm:example.glossaryterm1',
                    path: 'test_downstream1',
                },
            ],
            downstreams: [
                {
                    urn: 'urn:li:glossaryTerm:example.glossaryterm2',
                    path: 'test_downstream2',
                },
            ],
        },
    ],
};
