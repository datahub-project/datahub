/* eslint-disable */
import * as Types from '../types.generated';

import {
    NonRecursiveDatasetFieldsFragment,
    GlobalTagsFieldsFragment,
    GlossaryTermsFragment,
    EntityDomainFragment,
    ParentContainersFieldsFragment,
    SchemaMetadataFieldsFragment,
} from './fragments.generated';
import { TestFieldsFragment } from './test.generated';
import { AssertionDetailsFragment, AssertionRunEventDetailsFragment } from './assertion.generated';
import { RunResultsFragment } from './dataProcess.generated';
import { gql } from '@apollo/client';
import {
    NonRecursiveDatasetFieldsFragmentDoc,
    GlobalTagsFieldsFragmentDoc,
    GlossaryTermsFragmentDoc,
    EntityDomainFragmentDoc,
    ParentContainersFieldsFragmentDoc,
    SchemaMetadataFieldsFragmentDoc,
} from './fragments.generated';
import { TestFieldsFragmentDoc } from './test.generated';
import { AssertionDetailsFragmentDoc, AssertionRunEventDetailsFragmentDoc } from './assertion.generated';
import { RunResultsFragmentDoc } from './dataProcess.generated';
import * as Apollo from '@apollo/client';
export type GetDataProfilesQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    limit?: Types.Maybe<Types.Scalars['Int']>;
    startTime?: Types.Maybe<Types.Scalars['Long']>;
    endTime?: Types.Maybe<Types.Scalars['Long']>;
}>;

export type GetDataProfilesQuery = { __typename?: 'Query' } & {
    dataset?: Types.Maybe<
        { __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn'> & {
                datasetProfiles?: Types.Maybe<
                    Array<
                        { __typename?: 'DatasetProfile' } & Pick<
                            Types.DatasetProfile,
                            'rowCount' | 'columnCount' | 'sizeInBytes' | 'timestampMillis'
                        > & {
                                fieldProfiles?: Types.Maybe<
                                    Array<
                                        { __typename?: 'DatasetFieldProfile' } & Pick<
                                            Types.DatasetFieldProfile,
                                            | 'fieldPath'
                                            | 'uniqueCount'
                                            | 'uniqueProportion'
                                            | 'nullCount'
                                            | 'nullProportion'
                                            | 'min'
                                            | 'max'
                                            | 'mean'
                                            | 'median'
                                            | 'stdev'
                                            | 'sampleValues'
                                        >
                                    >
                                >;
                            }
                    >
                >;
            }
    >;
};

export type GetDatasetQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetDatasetQuery = { __typename?: 'Query' } & {
    dataset?: Types.Maybe<
        { __typename?: 'Dataset' } & Pick<Types.Dataset, 'exists'> & {
                siblings?: Types.Maybe<
                    { __typename?: 'SiblingProperties' } & Pick<Types.SiblingProperties, 'isPrimary'> & {
                            siblings?: Types.Maybe<
                                Array<
                                    Types.Maybe<
                                        | ({ __typename?: 'AccessTokenMetadata' } & Pick<
                                              Types.AccessTokenMetadata,
                                              'urn' | 'type'
                                          >)
                                        | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>)
                                        | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'>)
                                        | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
                                        | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
                                        | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
                                        | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'>)
                                        | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'>)
                                        | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
                                        | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
                                        | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
                                        | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'>)
                                        | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
                                        | ({ __typename?: 'DataPlatformInstance' } & Pick<
                                              Types.DataPlatformInstance,
                                              'urn' | 'type'
                                          >)
                                        | ({ __typename?: 'DataProcessInstance' } & Pick<
                                              Types.DataProcessInstance,
                                              'urn' | 'type'
                                          >)
                                        | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'> &
                                              NonSiblingDatasetFieldsFragment)
                                        | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
                                        | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
                                        | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'>)
                                        | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'>)
                                        | ({ __typename?: 'MLFeatureTable' } & Pick<
                                              Types.MlFeatureTable,
                                              'urn' | 'type'
                                          >)
                                        | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'>)
                                        | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'>)
                                        | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'>)
                                        | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
                                        | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
                                        | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
                                        | ({ __typename?: 'SchemaFieldEntity' } & Pick<
                                              Types.SchemaFieldEntity,
                                              'urn' | 'type'
                                          >)
                                        | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'>)
                                        | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
                                        | ({ __typename?: 'VersionedDataset' } & Pick<
                                              Types.VersionedDataset,
                                              'urn' | 'type'
                                          >)
                                    >
                                >
                            >;
                        }
                >;
            } & NonSiblingDatasetFieldsFragment
    >;
};

export type NonSiblingDatasetFieldsFragment = { __typename?: 'Dataset' } & {
    deprecation?: Types.Maybe<
        { __typename?: 'Deprecation' } & Pick<Types.Deprecation, 'actor' | 'deprecated' | 'note' | 'decommissionTime'>
    >;
    globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
    glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
    subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
    domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
    parentContainers?: Types.Maybe<{ __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment>;
    usageStats?: Types.Maybe<
        { __typename?: 'UsageQueryResult' } & {
            buckets?: Types.Maybe<
                Array<
                    Types.Maybe<
                        { __typename?: 'UsageAggregation' } & Pick<Types.UsageAggregation, 'bucket'> & {
                                metrics?: Types.Maybe<
                                    { __typename?: 'UsageAggregationMetrics' } & Pick<
                                        Types.UsageAggregationMetrics,
                                        'totalSqlQueries'
                                    >
                                >;
                            }
                    >
                >
            >;
            aggregations?: Types.Maybe<
                { __typename?: 'UsageQueryResultAggregations' } & Pick<
                    Types.UsageQueryResultAggregations,
                    'uniqueUserCount' | 'totalSqlQueries'
                > & {
                        fields?: Types.Maybe<
                            Array<
                                Types.Maybe<
                                    { __typename?: 'FieldUsageCounts' } & Pick<
                                        Types.FieldUsageCounts,
                                        'fieldName' | 'count'
                                    >
                                >
                            >
                        >;
                    }
            >;
        }
    >;
    datasetProfiles?: Types.Maybe<
        Array<
            { __typename?: 'DatasetProfile' } & Pick<
                Types.DatasetProfile,
                'rowCount' | 'columnCount' | 'sizeInBytes' | 'timestampMillis'
            > & {
                    fieldProfiles?: Types.Maybe<
                        Array<
                            { __typename?: 'DatasetFieldProfile' } & Pick<
                                Types.DatasetFieldProfile,
                                | 'fieldPath'
                                | 'uniqueCount'
                                | 'uniqueProportion'
                                | 'nullCount'
                                | 'nullProportion'
                                | 'min'
                                | 'max'
                                | 'mean'
                                | 'median'
                                | 'stdev'
                                | 'sampleValues'
                            >
                        >
                    >;
                }
        >
    >;
    health?: Types.Maybe<
        Array<{ __typename?: 'Health' } & Pick<Types.Health, 'type' | 'status' | 'message' | 'causes'>>
    >;
    assertions?: Types.Maybe<{ __typename?: 'EntityAssertionsResult' } & Pick<Types.EntityAssertionsResult, 'total'>>;
    operations?: Types.Maybe<
        Array<{ __typename?: 'Operation' } & Pick<Types.Operation, 'timestampMillis' | 'lastUpdatedTimestamp'>>
    >;
    autoRenderAspects?: Types.Maybe<
        Array<
            { __typename?: 'RawAspect' } & Pick<Types.RawAspect, 'aspectName' | 'payload'> & {
                    renderSpec?: Types.Maybe<
                        { __typename?: 'AspectRenderSpec' } & Pick<
                            Types.AspectRenderSpec,
                            'displayType' | 'displayName' | 'key'
                        >
                    >;
                }
        >
    >;
    status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
    readRuns?: Types.Maybe<
        { __typename?: 'DataProcessInstanceResult' } & Pick<
            Types.DataProcessInstanceResult,
            'count' | 'start' | 'total'
        >
    >;
    writeRuns?: Types.Maybe<
        { __typename?: 'DataProcessInstanceResult' } & Pick<
            Types.DataProcessInstanceResult,
            'count' | 'start' | 'total'
        >
    >;
    testResults?: Types.Maybe<
        { __typename?: 'TestResults' } & {
            passing: Array<
                { __typename?: 'TestResult' } & Pick<Types.TestResult, 'type'> & {
                        test?: Types.Maybe<{ __typename?: 'Test' } & TestFieldsFragment>;
                    }
            >;
            failing: Array<
                { __typename?: 'TestResult' } & Pick<Types.TestResult, 'type'> & {
                        test?: Types.Maybe<{ __typename?: 'Test' } & TestFieldsFragment>;
                    }
            >;
        }
    >;
    statsSummary?: Types.Maybe<
        { __typename?: 'DatasetStatsSummary' } & Pick<
            Types.DatasetStatsSummary,
            'queryCountLast30Days' | 'uniqueUserCountLast30Days'
        > & {
                topUsersLast30Days?: Types.Maybe<
                    Array<
                        { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type' | 'username'> & {
                                properties?: Types.Maybe<
                                    { __typename?: 'CorpUserProperties' } & Pick<
                                        Types.CorpUserProperties,
                                        'displayName' | 'firstName' | 'lastName' | 'fullName'
                                    >
                                >;
                                editableProperties?: Types.Maybe<
                                    { __typename?: 'CorpUserEditableProperties' } & Pick<
                                        Types.CorpUserEditableProperties,
                                        'displayName' | 'pictureLink'
                                    >
                                >;
                            }
                    >
                >;
            }
    >;
    siblings?: Types.Maybe<{ __typename?: 'SiblingProperties' } & Pick<Types.SiblingProperties, 'isPrimary'>>;
    privileges?: Types.Maybe<
        { __typename?: 'EntityPrivileges' } & Pick<
            Types.EntityPrivileges,
            'canEditLineage' | 'canEditEmbed' | 'canEditQueries'
        >
    >;
} & NonRecursiveDatasetFieldsFragment &
    ViewPropertiesFragment;

export type GetRecentQueriesQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetRecentQueriesQuery = { __typename?: 'Query' } & {
    dataset?: Types.Maybe<
        { __typename?: 'Dataset' } & {
            usageStats?: Types.Maybe<
                { __typename?: 'UsageQueryResult' } & {
                    buckets?: Types.Maybe<
                        Array<
                            Types.Maybe<
                                { __typename?: 'UsageAggregation' } & Pick<Types.UsageAggregation, 'bucket'> & {
                                        metrics?: Types.Maybe<
                                            { __typename?: 'UsageAggregationMetrics' } & Pick<
                                                Types.UsageAggregationMetrics,
                                                'topSqlQueries'
                                            >
                                        >;
                                    }
                            >
                        >
                    >;
                }
            >;
        }
    >;
};

export type GetLastMonthUsageAggregationsQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetLastMonthUsageAggregationsQuery = { __typename?: 'Query' } & {
    dataset?: Types.Maybe<
        { __typename?: 'Dataset' } & {
            usageStats?: Types.Maybe<
                { __typename?: 'UsageQueryResult' } & {
                    aggregations?: Types.Maybe<
                        { __typename?: 'UsageQueryResultAggregations' } & Pick<
                            Types.UsageQueryResultAggregations,
                            'uniqueUserCount' | 'totalSqlQueries'
                        > & {
                                users?: Types.Maybe<
                                    Array<
                                        Types.Maybe<
                                            { __typename?: 'UserUsageCounts' } & Pick<
                                                Types.UserUsageCounts,
                                                'count' | 'userEmail'
                                            > & {
                                                    user?: Types.Maybe<
                                                        { __typename?: 'CorpUser' } & Pick<
                                                            Types.CorpUser,
                                                            'urn' | 'type' | 'username'
                                                        > & {
                                                                properties?: Types.Maybe<
                                                                    { __typename?: 'CorpUserProperties' } & Pick<
                                                                        Types.CorpUserProperties,
                                                                        | 'displayName'
                                                                        | 'firstName'
                                                                        | 'lastName'
                                                                        | 'fullName'
                                                                    >
                                                                >;
                                                                editableProperties?: Types.Maybe<
                                                                    {
                                                                        __typename?: 'CorpUserEditableProperties';
                                                                    } & Pick<
                                                                        Types.CorpUserEditableProperties,
                                                                        'displayName' | 'pictureLink'
                                                                    >
                                                                >;
                                                            }
                                                    >;
                                                }
                                        >
                                    >
                                >;
                                fields?: Types.Maybe<
                                    Array<
                                        Types.Maybe<
                                            { __typename?: 'FieldUsageCounts' } & Pick<
                                                Types.FieldUsageCounts,
                                                'fieldName' | 'count'
                                            >
                                        >
                                    >
                                >;
                            }
                    >;
                }
            >;
        }
    >;
};

export type UpdateDatasetMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.DatasetUpdateInput;
}>;

export type UpdateDatasetMutation = { __typename?: 'Mutation' } & {
    updateDataset?: Types.Maybe<{ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn'>>;
};

export type ViewPropertiesFragment = { __typename?: 'Dataset' } & {
    viewProperties?: Types.Maybe<
        { __typename?: 'ViewProperties' } & Pick<Types.ViewProperties, 'materialized' | 'logic' | 'language'>
    >;
};

export type AssertionsQueryFragment = { __typename?: 'Dataset' } & {
    assertions?: Types.Maybe<
        { __typename?: 'EntityAssertionsResult' } & Pick<Types.EntityAssertionsResult, 'start' | 'count' | 'total'> & {
                assertions: Array<
                    { __typename?: 'Assertion' } & {
                        runEvents?: Types.Maybe<
                            { __typename?: 'AssertionRunEventsResult' } & Pick<
                                Types.AssertionRunEventsResult,
                                'total' | 'failed' | 'succeeded'
                            > & {
                                    runEvents: Array<
                                        { __typename?: 'AssertionRunEvent' } & AssertionRunEventDetailsFragment
                                    >;
                                }
                        >;
                    } & AssertionDetailsFragment
                >;
            }
    >;
};

export type GetDatasetAssertionsQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetDatasetAssertionsQuery = { __typename?: 'Query' } & {
    dataset?: Types.Maybe<
        { __typename?: 'Dataset' } & {
            siblings?: Types.Maybe<
                { __typename?: 'SiblingProperties' } & Pick<Types.SiblingProperties, 'isPrimary'> & {
                        siblings?: Types.Maybe<
                            Array<
                                Types.Maybe<
                                    | ({ __typename?: 'AccessTokenMetadata' } & Pick<
                                          Types.AccessTokenMetadata,
                                          'urn' | 'type'
                                      >)
                                    | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>)
                                    | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'>)
                                    | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
                                    | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
                                    | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
                                    | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataPlatformInstance' } & Pick<
                                          Types.DataPlatformInstance,
                                          'urn' | 'type'
                                      >)
                                    | ({ __typename?: 'DataProcessInstance' } & Pick<
                                          Types.DataProcessInstance,
                                          'urn' | 'type'
                                      >)
                                    | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'> &
                                          AssertionsQueryFragment)
                                    | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
                                    | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
                                    | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'>)
                                    | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
                                    | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
                                    | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
                                    | ({ __typename?: 'SchemaFieldEntity' } & Pick<
                                          Types.SchemaFieldEntity,
                                          'urn' | 'type'
                                      >)
                                    | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'>)
                                    | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
                                    | ({ __typename?: 'VersionedDataset' } & Pick<
                                          Types.VersionedDataset,
                                          'urn' | 'type'
                                      >)
                                >
                            >
                        >;
                    }
            >;
        } & AssertionsQueryFragment
    >;
};

export type GetDatasetRunsQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    start: Types.Scalars['Int'];
    count: Types.Scalars['Int'];
    direction: Types.RelationshipDirection;
}>;

export type GetDatasetRunsQuery = { __typename?: 'Query' } & {
    dataset?: Types.Maybe<
        { __typename?: 'Dataset' } & {
            runs?: Types.Maybe<{ __typename?: 'DataProcessInstanceResult' } & RunResultsFragment>;
        }
    >;
};

export type DatasetSchemaFragment = { __typename?: 'Dataset' } & {
    schemaMetadata?: Types.Maybe<{ __typename?: 'SchemaMetadata' } & SchemaMetadataFieldsFragment>;
    editableSchemaMetadata?: Types.Maybe<
        { __typename?: 'EditableSchemaMetadata' } & {
            editableSchemaFieldInfo: Array<
                { __typename?: 'EditableSchemaFieldInfo' } & Pick<
                    Types.EditableSchemaFieldInfo,
                    'fieldPath' | 'description'
                > & {
                        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
                    }
            >;
        }
    >;
};

export type GetDatasetSchemaQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetDatasetSchemaQuery = { __typename?: 'Query' } & {
    dataset?: Types.Maybe<
        { __typename?: 'Dataset' } & {
            siblings?: Types.Maybe<
                { __typename?: 'SiblingProperties' } & Pick<Types.SiblingProperties, 'isPrimary'> & {
                        siblings?: Types.Maybe<
                            Array<
                                Types.Maybe<
                                    | ({ __typename?: 'AccessTokenMetadata' } & Pick<
                                          Types.AccessTokenMetadata,
                                          'urn' | 'type'
                                      >)
                                    | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>)
                                    | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'>)
                                    | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
                                    | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
                                    | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
                                    | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
                                    | ({ __typename?: 'DataPlatformInstance' } & Pick<
                                          Types.DataPlatformInstance,
                                          'urn' | 'type'
                                      >)
                                    | ({ __typename?: 'DataProcessInstance' } & Pick<
                                          Types.DataProcessInstance,
                                          'urn' | 'type'
                                      >)
                                    | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'> &
                                          DatasetSchemaFragment)
                                    | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
                                    | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
                                    | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'>)
                                    | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'>)
                                    | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
                                    | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
                                    | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
                                    | ({ __typename?: 'SchemaFieldEntity' } & Pick<
                                          Types.SchemaFieldEntity,
                                          'urn' | 'type'
                                      >)
                                    | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'>)
                                    | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
                                    | ({ __typename?: 'VersionedDataset' } & Pick<
                                          Types.VersionedDataset,
                                          'urn' | 'type'
                                      >)
                                >
                            >
                        >;
                    }
            >;
        } & DatasetSchemaFragment
    >;
};

export const ViewPropertiesFragmentDoc = gql`
    fragment viewProperties on Dataset {
        viewProperties {
            materialized
            logic
            language
        }
    }
`;
export const NonSiblingDatasetFieldsFragmentDoc = gql`
    fragment nonSiblingDatasetFields on Dataset {
        ...nonRecursiveDatasetFields
        deprecation {
            actor
            deprecated
            note
            decommissionTime
        }
        globalTags {
            ...globalTagsFields
        }
        glossaryTerms {
            ...glossaryTerms
        }
        subTypes {
            typeNames
        }
        domain {
            ...entityDomain
        }
        parentContainers {
            ...parentContainersFields
        }
        usageStats(range: MONTH) {
            buckets {
                bucket
                metrics {
                    totalSqlQueries
                }
            }
            aggregations {
                uniqueUserCount
                totalSqlQueries
                fields {
                    fieldName
                    count
                }
            }
        }
        datasetProfiles(limit: 1) {
            rowCount
            columnCount
            sizeInBytes
            timestampMillis
            fieldProfiles {
                fieldPath
                uniqueCount
                uniqueProportion
                nullCount
                nullProportion
                min
                max
                mean
                median
                stdev
                sampleValues
            }
        }
        health {
            type
            status
            message
            causes
        }
        assertions(start: 0, count: 1) {
            total
        }
        operations(limit: 1) {
            timestampMillis
            lastUpdatedTimestamp
        }
        ...viewProperties
        autoRenderAspects: aspects(input: { autoRenderOnly: true }) {
            aspectName
            payload
            renderSpec {
                displayType
                displayName
                key
            }
        }
        status {
            removed
        }
        readRuns: runs(start: 0, count: 20, direction: INCOMING) {
            count
            start
            total
        }
        writeRuns: runs(start: 0, count: 20, direction: OUTGOING) {
            count
            start
            total
        }
        testResults {
            passing {
                test {
                    ...testFields
                }
                type
            }
            failing {
                test {
                    ...testFields
                }
                type
            }
        }
        statsSummary {
            queryCountLast30Days
            uniqueUserCountLast30Days
            topUsersLast30Days {
                urn
                type
                username
                properties {
                    displayName
                    firstName
                    lastName
                    fullName
                }
                editableProperties {
                    displayName
                    pictureLink
                }
            }
        }
        siblings {
            isPrimary
        }
        privileges {
            canEditLineage
            canEditEmbed
            canEditQueries
        }
    }
    ${NonRecursiveDatasetFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${ParentContainersFieldsFragmentDoc}
    ${ViewPropertiesFragmentDoc}
    ${TestFieldsFragmentDoc}
`;
export const AssertionsQueryFragmentDoc = gql`
    fragment assertionsQuery on Dataset {
        assertions(start: 0, count: 1000) {
            start
            count
            total
            assertions {
                ...assertionDetails
                runEvents(status: COMPLETE, limit: 1) {
                    total
                    failed
                    succeeded
                    runEvents {
                        ...assertionRunEventDetails
                    }
                }
            }
        }
    }
    ${AssertionDetailsFragmentDoc}
    ${AssertionRunEventDetailsFragmentDoc}
`;
export const DatasetSchemaFragmentDoc = gql`
    fragment datasetSchema on Dataset {
        schemaMetadata(version: 0) {
            ...schemaMetadataFields
        }
        editableSchemaMetadata {
            editableSchemaFieldInfo {
                fieldPath
                description
                globalTags {
                    ...globalTagsFields
                }
                glossaryTerms {
                    ...glossaryTerms
                }
            }
        }
    }
    ${SchemaMetadataFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
`;
export const GetDataProfilesDocument = gql`
    query getDataProfiles($urn: String!, $limit: Int, $startTime: Long, $endTime: Long) {
        dataset(urn: $urn) {
            urn
            datasetProfiles(limit: $limit, startTimeMillis: $startTime, endTimeMillis: $endTime) {
                rowCount
                columnCount
                sizeInBytes
                timestampMillis
                fieldProfiles {
                    fieldPath
                    uniqueCount
                    uniqueProportion
                    nullCount
                    nullProportion
                    min
                    max
                    mean
                    median
                    stdev
                    sampleValues
                }
            }
        }
    }
`;

/**
 * __useGetDataProfilesQuery__
 *
 * To run a query within a React component, call `useGetDataProfilesQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDataProfilesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDataProfilesQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      limit: // value for 'limit'
 *      startTime: // value for 'startTime'
 *      endTime: // value for 'endTime'
 *   },
 * });
 */
export function useGetDataProfilesQuery(
    baseOptions: Apollo.QueryHookOptions<GetDataProfilesQuery, GetDataProfilesQueryVariables>,
) {
    return Apollo.useQuery<GetDataProfilesQuery, GetDataProfilesQueryVariables>(GetDataProfilesDocument, baseOptions);
}
export function useGetDataProfilesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDataProfilesQuery, GetDataProfilesQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDataProfilesQuery, GetDataProfilesQueryVariables>(
        GetDataProfilesDocument,
        baseOptions,
    );
}
export type GetDataProfilesQueryHookResult = ReturnType<typeof useGetDataProfilesQuery>;
export type GetDataProfilesLazyQueryHookResult = ReturnType<typeof useGetDataProfilesLazyQuery>;
export type GetDataProfilesQueryResult = Apollo.QueryResult<GetDataProfilesQuery, GetDataProfilesQueryVariables>;
export const GetDatasetDocument = gql`
    query getDataset($urn: String!) {
        dataset(urn: $urn) {
            exists
            ...nonSiblingDatasetFields
            siblings {
                isPrimary
                siblings {
                    urn
                    type
                    ...nonSiblingDatasetFields
                }
            }
        }
    }
    ${NonSiblingDatasetFieldsFragmentDoc}
`;

/**
 * __useGetDatasetQuery__
 *
 * To run a query within a React component, call `useGetDatasetQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDatasetQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDatasetQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetDatasetQuery(baseOptions: Apollo.QueryHookOptions<GetDatasetQuery, GetDatasetQueryVariables>) {
    return Apollo.useQuery<GetDatasetQuery, GetDatasetQueryVariables>(GetDatasetDocument, baseOptions);
}
export function useGetDatasetLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDatasetQuery, GetDatasetQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDatasetQuery, GetDatasetQueryVariables>(GetDatasetDocument, baseOptions);
}
export type GetDatasetQueryHookResult = ReturnType<typeof useGetDatasetQuery>;
export type GetDatasetLazyQueryHookResult = ReturnType<typeof useGetDatasetLazyQuery>;
export type GetDatasetQueryResult = Apollo.QueryResult<GetDatasetQuery, GetDatasetQueryVariables>;
export const GetRecentQueriesDocument = gql`
    query getRecentQueries($urn: String!) {
        dataset(urn: $urn) {
            usageStats(range: MONTH) {
                buckets {
                    bucket
                    metrics {
                        topSqlQueries
                    }
                }
            }
        }
    }
`;

/**
 * __useGetRecentQueriesQuery__
 *
 * To run a query within a React component, call `useGetRecentQueriesQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetRecentQueriesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetRecentQueriesQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetRecentQueriesQuery(
    baseOptions: Apollo.QueryHookOptions<GetRecentQueriesQuery, GetRecentQueriesQueryVariables>,
) {
    return Apollo.useQuery<GetRecentQueriesQuery, GetRecentQueriesQueryVariables>(
        GetRecentQueriesDocument,
        baseOptions,
    );
}
export function useGetRecentQueriesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetRecentQueriesQuery, GetRecentQueriesQueryVariables>,
) {
    return Apollo.useLazyQuery<GetRecentQueriesQuery, GetRecentQueriesQueryVariables>(
        GetRecentQueriesDocument,
        baseOptions,
    );
}
export type GetRecentQueriesQueryHookResult = ReturnType<typeof useGetRecentQueriesQuery>;
export type GetRecentQueriesLazyQueryHookResult = ReturnType<typeof useGetRecentQueriesLazyQuery>;
export type GetRecentQueriesQueryResult = Apollo.QueryResult<GetRecentQueriesQuery, GetRecentQueriesQueryVariables>;
export const GetLastMonthUsageAggregationsDocument = gql`
    query getLastMonthUsageAggregations($urn: String!) {
        dataset(urn: $urn) {
            usageStats(range: MONTH) {
                aggregations {
                    uniqueUserCount
                    totalSqlQueries
                    users {
                        user {
                            urn
                            type
                            username
                            properties {
                                displayName
                                firstName
                                lastName
                                fullName
                            }
                            editableProperties {
                                displayName
                                pictureLink
                            }
                        }
                        count
                        userEmail
                    }
                    fields {
                        fieldName
                        count
                    }
                }
            }
        }
    }
`;

/**
 * __useGetLastMonthUsageAggregationsQuery__
 *
 * To run a query within a React component, call `useGetLastMonthUsageAggregationsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetLastMonthUsageAggregationsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetLastMonthUsageAggregationsQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetLastMonthUsageAggregationsQuery(
    baseOptions: Apollo.QueryHookOptions<
        GetLastMonthUsageAggregationsQuery,
        GetLastMonthUsageAggregationsQueryVariables
    >,
) {
    return Apollo.useQuery<GetLastMonthUsageAggregationsQuery, GetLastMonthUsageAggregationsQueryVariables>(
        GetLastMonthUsageAggregationsDocument,
        baseOptions,
    );
}
export function useGetLastMonthUsageAggregationsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<
        GetLastMonthUsageAggregationsQuery,
        GetLastMonthUsageAggregationsQueryVariables
    >,
) {
    return Apollo.useLazyQuery<GetLastMonthUsageAggregationsQuery, GetLastMonthUsageAggregationsQueryVariables>(
        GetLastMonthUsageAggregationsDocument,
        baseOptions,
    );
}
export type GetLastMonthUsageAggregationsQueryHookResult = ReturnType<typeof useGetLastMonthUsageAggregationsQuery>;
export type GetLastMonthUsageAggregationsLazyQueryHookResult = ReturnType<
    typeof useGetLastMonthUsageAggregationsLazyQuery
>;
export type GetLastMonthUsageAggregationsQueryResult = Apollo.QueryResult<
    GetLastMonthUsageAggregationsQuery,
    GetLastMonthUsageAggregationsQueryVariables
>;
export const UpdateDatasetDocument = gql`
    mutation updateDataset($urn: String!, $input: DatasetUpdateInput!) {
        updateDataset(urn: $urn, input: $input) {
            urn
        }
    }
`;
export type UpdateDatasetMutationFn = Apollo.MutationFunction<UpdateDatasetMutation, UpdateDatasetMutationVariables>;

/**
 * __useUpdateDatasetMutation__
 *
 * To run a mutation, you first call `useUpdateDatasetMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateDatasetMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateDatasetMutation, { data, loading, error }] = useUpdateDatasetMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateDatasetMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateDatasetMutation, UpdateDatasetMutationVariables>,
) {
    return Apollo.useMutation<UpdateDatasetMutation, UpdateDatasetMutationVariables>(
        UpdateDatasetDocument,
        baseOptions,
    );
}
export type UpdateDatasetMutationHookResult = ReturnType<typeof useUpdateDatasetMutation>;
export type UpdateDatasetMutationResult = Apollo.MutationResult<UpdateDatasetMutation>;
export type UpdateDatasetMutationOptions = Apollo.BaseMutationOptions<
    UpdateDatasetMutation,
    UpdateDatasetMutationVariables
>;
export const GetDatasetAssertionsDocument = gql`
    query getDatasetAssertions($urn: String!) {
        dataset(urn: $urn) {
            ...assertionsQuery
            siblings {
                isPrimary
                siblings {
                    urn
                    type
                    ...assertionsQuery
                }
            }
        }
    }
    ${AssertionsQueryFragmentDoc}
`;

/**
 * __useGetDatasetAssertionsQuery__
 *
 * To run a query within a React component, call `useGetDatasetAssertionsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDatasetAssertionsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDatasetAssertionsQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetDatasetAssertionsQuery(
    baseOptions: Apollo.QueryHookOptions<GetDatasetAssertionsQuery, GetDatasetAssertionsQueryVariables>,
) {
    return Apollo.useQuery<GetDatasetAssertionsQuery, GetDatasetAssertionsQueryVariables>(
        GetDatasetAssertionsDocument,
        baseOptions,
    );
}
export function useGetDatasetAssertionsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDatasetAssertionsQuery, GetDatasetAssertionsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDatasetAssertionsQuery, GetDatasetAssertionsQueryVariables>(
        GetDatasetAssertionsDocument,
        baseOptions,
    );
}
export type GetDatasetAssertionsQueryHookResult = ReturnType<typeof useGetDatasetAssertionsQuery>;
export type GetDatasetAssertionsLazyQueryHookResult = ReturnType<typeof useGetDatasetAssertionsLazyQuery>;
export type GetDatasetAssertionsQueryResult = Apollo.QueryResult<
    GetDatasetAssertionsQuery,
    GetDatasetAssertionsQueryVariables
>;
export const GetDatasetRunsDocument = gql`
    query getDatasetRuns($urn: String!, $start: Int!, $count: Int!, $direction: RelationshipDirection!) {
        dataset(urn: $urn) {
            runs(start: $start, count: $count, direction: $direction) {
                ...runResults
            }
        }
    }
    ${RunResultsFragmentDoc}
`;

/**
 * __useGetDatasetRunsQuery__
 *
 * To run a query within a React component, call `useGetDatasetRunsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDatasetRunsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDatasetRunsQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      start: // value for 'start'
 *      count: // value for 'count'
 *      direction: // value for 'direction'
 *   },
 * });
 */
export function useGetDatasetRunsQuery(
    baseOptions: Apollo.QueryHookOptions<GetDatasetRunsQuery, GetDatasetRunsQueryVariables>,
) {
    return Apollo.useQuery<GetDatasetRunsQuery, GetDatasetRunsQueryVariables>(GetDatasetRunsDocument, baseOptions);
}
export function useGetDatasetRunsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDatasetRunsQuery, GetDatasetRunsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDatasetRunsQuery, GetDatasetRunsQueryVariables>(GetDatasetRunsDocument, baseOptions);
}
export type GetDatasetRunsQueryHookResult = ReturnType<typeof useGetDatasetRunsQuery>;
export type GetDatasetRunsLazyQueryHookResult = ReturnType<typeof useGetDatasetRunsLazyQuery>;
export type GetDatasetRunsQueryResult = Apollo.QueryResult<GetDatasetRunsQuery, GetDatasetRunsQueryVariables>;
export const GetDatasetSchemaDocument = gql`
    query getDatasetSchema($urn: String!) {
        dataset(urn: $urn) {
            ...datasetSchema
            siblings {
                isPrimary
                siblings {
                    urn
                    type
                    ... on Dataset {
                        ...datasetSchema
                    }
                }
            }
        }
    }
    ${DatasetSchemaFragmentDoc}
`;

/**
 * __useGetDatasetSchemaQuery__
 *
 * To run a query within a React component, call `useGetDatasetSchemaQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDatasetSchemaQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDatasetSchemaQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetDatasetSchemaQuery(
    baseOptions: Apollo.QueryHookOptions<GetDatasetSchemaQuery, GetDatasetSchemaQueryVariables>,
) {
    return Apollo.useQuery<GetDatasetSchemaQuery, GetDatasetSchemaQueryVariables>(
        GetDatasetSchemaDocument,
        baseOptions,
    );
}
export function useGetDatasetSchemaLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDatasetSchemaQuery, GetDatasetSchemaQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDatasetSchemaQuery, GetDatasetSchemaQueryVariables>(
        GetDatasetSchemaDocument,
        baseOptions,
    );
}
export type GetDatasetSchemaQueryHookResult = ReturnType<typeof useGetDatasetSchemaQuery>;
export type GetDatasetSchemaLazyQueryHookResult = ReturnType<typeof useGetDatasetSchemaLazyQuery>;
export type GetDatasetSchemaQueryResult = Apollo.QueryResult<GetDatasetSchemaQuery, GetDatasetSchemaQueryVariables>;
