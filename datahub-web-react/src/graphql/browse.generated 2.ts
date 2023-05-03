/* eslint-disable */
import * as Types from '../types.generated';

import {
    PlatformFieldsFragment,
    OwnershipFieldsFragment,
    GlobalTagsFieldsFragment,
    GlossaryTermsFragment,
    EntityDomainFragment,
    NonRecursiveDataFlowFieldsFragment,
    InstitutionalMemoryFieldsFragment,
    DeprecationFieldsFragment,
    EmbedFieldsFragment,
    DataPlatformInstanceFieldsFragment,
    ParentContainersFieldsFragment,
    InputFieldsFieldsFragment,
    EntityContainerFragment,
    ParentNodesFieldsFragment,
    GlossaryNodeFragment,
    NonRecursiveMlFeatureTableFragment,
    NonRecursiveMlFeatureFragment,
    NonRecursiveMlPrimaryKeyFragment,
    SchemaMetadataFieldsFragment,
    NonConflictingPlatformFieldsFragment,
} from './fragments.generated';
import { gql } from '@apollo/client';
import {
    PlatformFieldsFragmentDoc,
    OwnershipFieldsFragmentDoc,
    GlobalTagsFieldsFragmentDoc,
    GlossaryTermsFragmentDoc,
    EntityDomainFragmentDoc,
    NonRecursiveDataFlowFieldsFragmentDoc,
    InstitutionalMemoryFieldsFragmentDoc,
    DeprecationFieldsFragmentDoc,
    EmbedFieldsFragmentDoc,
    DataPlatformInstanceFieldsFragmentDoc,
    ParentContainersFieldsFragmentDoc,
    InputFieldsFieldsFragmentDoc,
    EntityContainerFragmentDoc,
    ParentNodesFieldsFragmentDoc,
    GlossaryNodeFragmentDoc,
    NonRecursiveMlFeatureTableFragmentDoc,
    NonRecursiveMlFeatureFragmentDoc,
    NonRecursiveMlPrimaryKeyFragmentDoc,
    SchemaMetadataFieldsFragmentDoc,
    NonConflictingPlatformFieldsFragmentDoc,
} from './fragments.generated';
import * as Apollo from '@apollo/client';
export type GetBrowsePathsQueryVariables = Types.Exact<{
    input: Types.BrowsePathsInput;
}>;

export type GetBrowsePathsQuery = { __typename?: 'Query' } & {
    browsePaths?: Types.Maybe<Array<{ __typename?: 'BrowsePath' } & Pick<Types.BrowsePath, 'path'>>>;
};

export type GetBrowseResultsQueryVariables = Types.Exact<{
    input: Types.BrowseInput;
}>;

export type GetBrowseResultsQuery = { __typename?: 'Query' } & {
    browse?: Types.Maybe<
        { __typename?: 'BrowseResults' } & Pick<Types.BrowseResults, 'start' | 'count' | 'total'> & {
                entities: Array<
                    | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'>)
                    | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>)
                    | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type' | 'tool' | 'chartId'> & {
                              properties?: Types.Maybe<
                                  { __typename?: 'ChartProperties' } & Pick<
                                      Types.ChartProperties,
                                      'name' | 'description' | 'externalUrl' | 'type' | 'access'
                                  > & { lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'> }
                              >;
                              editableProperties?: Types.Maybe<
                                  { __typename?: 'ChartEditableProperties' } & Pick<
                                      Types.ChartEditableProperties,
                                      'description'
                                  >
                              >;
                              ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                              globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                              glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
                              platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                              domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
                          })
                    | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
                    | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
                    | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
                    | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type' | 'tool' | 'dashboardId'> & {
                              properties?: Types.Maybe<
                                  { __typename?: 'DashboardProperties' } & Pick<
                                      Types.DashboardProperties,
                                      'name' | 'description' | 'externalUrl' | 'access'
                                  > & { lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'> }
                              >;
                              editableProperties?: Types.Maybe<
                                  { __typename?: 'DashboardEditableProperties' } & Pick<
                                      Types.DashboardEditableProperties,
                                      'description'
                                  >
                              >;
                              ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                              globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                              glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
                              platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                              domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
                              subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
                          })
                    | ({ __typename?: 'DataFlow' } & Pick<
                          Types.DataFlow,
                          'urn' | 'type' | 'orchestrator' | 'flowId' | 'cluster'
                      > & {
                              properties?: Types.Maybe<
                                  { __typename?: 'DataFlowProperties' } & Pick<
                                      Types.DataFlowProperties,
                                      'name' | 'description' | 'project'
                                  >
                              >;
                              editableProperties?: Types.Maybe<
                                  { __typename?: 'DataFlowEditableProperties' } & Pick<
                                      Types.DataFlowEditableProperties,
                                      'description'
                                  >
                              >;
                              ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                              globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                              glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
                              platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                              domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
                          })
                    | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
                    | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
                    | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
                    | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type' | 'jobId'> & {
                              dataFlow?: Types.Maybe<{ __typename?: 'DataFlow' } & NonRecursiveDataFlowFieldsFragment>;
                              ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                              properties?: Types.Maybe<
                                  { __typename?: 'DataJobProperties' } & Pick<
                                      Types.DataJobProperties,
                                      'name' | 'description'
                                  >
                              >;
                              globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                              glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
                              editableProperties?: Types.Maybe<
                                  { __typename?: 'DataJobEditableProperties' } & Pick<
                                      Types.DataJobEditableProperties,
                                      'description'
                                  >
                              >;
                              domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
                          })
                    | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'>)
                    | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'>)
                    | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'>)
                    | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'name' | 'origin' | 'urn' | 'type'> & {
                              properties?: Types.Maybe<
                                  { __typename?: 'DatasetProperties' } & Pick<
                                      Types.DatasetProperties,
                                      'name' | 'description'
                                  >
                              >;
                              editableProperties?: Types.Maybe<
                                  { __typename?: 'DatasetEditableProperties' } & Pick<
                                      Types.DatasetEditableProperties,
                                      'description'
                                  >
                              >;
                              platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                              ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                              globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                              glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
                              subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
                              domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
                          })
                    | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
                    | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
                    | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'> & {
                              ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                              properties?: Types.Maybe<
                                  { __typename?: 'GlossaryTermProperties' } & Pick<
                                      Types.GlossaryTermProperties,
                                      'name' | 'description' | 'definition' | 'termSource' | 'sourceRef' | 'sourceUrl'
                                  > & {
                                          customProperties?: Types.Maybe<
                                              Array<
                                                  { __typename?: 'CustomPropertiesEntry' } & Pick<
                                                      Types.CustomPropertiesEntry,
                                                      'key' | 'value'
                                                  >
                                              >
                                          >;
                                      }
                              >;
                          })
                    | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'>)
                    | ({ __typename?: 'MLFeatureTable' } & Pick<
                          Types.MlFeatureTable,
                          'urn' | 'type' | 'name' | 'description'
                      > & {
                              featureTableProperties?: Types.Maybe<
                                  { __typename?: 'MLFeatureTableProperties' } & Pick<
                                      Types.MlFeatureTableProperties,
                                      'description'
                                  > & {
                                          mlFeatures?: Types.Maybe<
                                              Array<
                                                  Types.Maybe<
                                                      { __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn'>
                                                  >
                                              >
                                          >;
                                          mlPrimaryKeys?: Types.Maybe<
                                              Array<
                                                  Types.Maybe<
                                                      { __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn'>
                                                  >
                                              >
                                          >;
                                      }
                              >;
                              ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                              platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                          })
                    | ({ __typename?: 'MLModel' } & Pick<
                          Types.MlModel,
                          'name' | 'origin' | 'description' | 'urn' | 'type'
                      > & {
                              ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                              globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                              platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                          })
                    | ({ __typename?: 'MLModelGroup' } & Pick<
                          Types.MlModelGroup,
                          'name' | 'origin' | 'description' | 'urn' | 'type'
                      > & {
                              ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                              platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                          })
                    | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'>)
                    | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
                    | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
                    | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
                    | ({ __typename?: 'SchemaFieldEntity' } & Pick<Types.SchemaFieldEntity, 'urn' | 'type'>)
                    | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'>)
                    | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
                    | ({ __typename?: 'VersionedDataset' } & Pick<Types.VersionedDataset, 'urn' | 'type'>)
                >;
                groups: Array<{ __typename?: 'BrowseResultGroup' } & Pick<Types.BrowseResultGroup, 'name' | 'count'>>;
                metadata: { __typename?: 'BrowseResultMetadata' } & Pick<
                    Types.BrowseResultMetadata,
                    'path' | 'totalNumEntities'
                >;
            }
    >;
};

export const GetBrowsePathsDocument = gql`
    query getBrowsePaths($input: BrowsePathsInput!) {
        browsePaths(input: $input) {
            path
        }
    }
`;

/**
 * __useGetBrowsePathsQuery__
 *
 * To run a query within a React component, call `useGetBrowsePathsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetBrowsePathsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetBrowsePathsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetBrowsePathsQuery(
    baseOptions: Apollo.QueryHookOptions<GetBrowsePathsQuery, GetBrowsePathsQueryVariables>,
) {
    return Apollo.useQuery<GetBrowsePathsQuery, GetBrowsePathsQueryVariables>(GetBrowsePathsDocument, baseOptions);
}
export function useGetBrowsePathsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetBrowsePathsQuery, GetBrowsePathsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetBrowsePathsQuery, GetBrowsePathsQueryVariables>(GetBrowsePathsDocument, baseOptions);
}
export type GetBrowsePathsQueryHookResult = ReturnType<typeof useGetBrowsePathsQuery>;
export type GetBrowsePathsLazyQueryHookResult = ReturnType<typeof useGetBrowsePathsLazyQuery>;
export type GetBrowsePathsQueryResult = Apollo.QueryResult<GetBrowsePathsQuery, GetBrowsePathsQueryVariables>;
export const GetBrowseResultsDocument = gql`
    query getBrowseResults($input: BrowseInput!) {
        browse(input: $input) {
            entities {
                urn
                type
                ... on Dataset {
                    name
                    origin
                    properties {
                        name
                        description
                    }
                    editableProperties {
                        description
                    }
                    platform {
                        ...platformFields
                    }
                    ownership {
                        ...ownershipFields
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
                }
                ... on Dashboard {
                    urn
                    type
                    tool
                    dashboardId
                    properties {
                        name
                        description
                        externalUrl
                        access
                        lastModified {
                            time
                        }
                    }
                    editableProperties {
                        description
                    }
                    ownership {
                        ...ownershipFields
                    }
                    globalTags {
                        ...globalTagsFields
                    }
                    glossaryTerms {
                        ...glossaryTerms
                    }
                    platform {
                        ...platformFields
                    }
                    domain {
                        ...entityDomain
                    }
                    subTypes {
                        typeNames
                    }
                }
                ... on GlossaryTerm {
                    ownership {
                        ...ownershipFields
                    }
                    properties {
                        name
                        description
                        definition
                        termSource
                        sourceRef
                        sourceUrl
                        customProperties {
                            key
                            value
                        }
                    }
                }
                ... on Chart {
                    urn
                    type
                    tool
                    chartId
                    properties {
                        name
                        description
                        externalUrl
                        type
                        access
                        lastModified {
                            time
                        }
                    }
                    editableProperties {
                        description
                    }
                    ownership {
                        ...ownershipFields
                    }
                    globalTags {
                        ...globalTagsFields
                    }
                    glossaryTerms {
                        ...glossaryTerms
                    }
                    platform {
                        ...platformFields
                    }
                    domain {
                        ...entityDomain
                    }
                }
                ... on DataFlow {
                    urn
                    type
                    orchestrator
                    flowId
                    cluster
                    properties {
                        name
                        description
                        project
                    }
                    editableProperties {
                        description
                    }
                    ownership {
                        ...ownershipFields
                    }
                    globalTags {
                        ...globalTagsFields
                    }
                    glossaryTerms {
                        ...glossaryTerms
                    }
                    platform {
                        ...platformFields
                    }
                    domain {
                        ...entityDomain
                    }
                }
                ... on DataJob {
                    urn
                    type
                    dataFlow {
                        ...nonRecursiveDataFlowFields
                    }
                    jobId
                    ownership {
                        ...ownershipFields
                    }
                    properties {
                        name
                        description
                    }
                    globalTags {
                        ...globalTagsFields
                    }
                    glossaryTerms {
                        ...glossaryTerms
                    }
                    editableProperties {
                        description
                    }
                    domain {
                        ...entityDomain
                    }
                }
                ... on MLFeatureTable {
                    urn
                    type
                    name
                    description
                    featureTableProperties {
                        description
                        mlFeatures {
                            urn
                        }
                        mlPrimaryKeys {
                            urn
                        }
                    }
                    ownership {
                        ...ownershipFields
                    }
                    platform {
                        ...platformFields
                    }
                }
                ... on MLModel {
                    name
                    origin
                    description
                    ownership {
                        ...ownershipFields
                    }
                    globalTags {
                        ...globalTagsFields
                    }
                    platform {
                        ...platformFields
                    }
                }
                ... on MLModelGroup {
                    name
                    origin
                    description
                    ownership {
                        ...ownershipFields
                    }
                    platform {
                        ...platformFields
                    }
                }
            }
            groups {
                name
                count
            }
            start
            count
            total
            metadata {
                path
                totalNumEntities
            }
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${NonRecursiveDataFlowFieldsFragmentDoc}
`;

/**
 * __useGetBrowseResultsQuery__
 *
 * To run a query within a React component, call `useGetBrowseResultsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetBrowseResultsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetBrowseResultsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetBrowseResultsQuery(
    baseOptions: Apollo.QueryHookOptions<GetBrowseResultsQuery, GetBrowseResultsQueryVariables>,
) {
    return Apollo.useQuery<GetBrowseResultsQuery, GetBrowseResultsQueryVariables>(
        GetBrowseResultsDocument,
        baseOptions,
    );
}
export function useGetBrowseResultsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetBrowseResultsQuery, GetBrowseResultsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetBrowseResultsQuery, GetBrowseResultsQueryVariables>(
        GetBrowseResultsDocument,
        baseOptions,
    );
}
export type GetBrowseResultsQueryHookResult = ReturnType<typeof useGetBrowseResultsQuery>;
export type GetBrowseResultsLazyQueryHookResult = ReturnType<typeof useGetBrowseResultsLazyQuery>;
export type GetBrowseResultsQueryResult = Apollo.QueryResult<GetBrowseResultsQuery, GetBrowseResultsQueryVariables>;
