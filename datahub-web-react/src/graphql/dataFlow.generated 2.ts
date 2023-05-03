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
import { PartialLineageResultsFragment } from './lineage.generated';
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
import { PartialLineageResultsFragmentDoc } from './lineage.generated';
import * as Apollo from '@apollo/client';
export type DataFlowFieldsFragment = { __typename?: 'DataFlow' } & Pick<
    Types.DataFlow,
    'urn' | 'type' | 'exists' | 'lastIngested' | 'orchestrator' | 'flowId' | 'cluster'
> & {
        platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
        properties?: Types.Maybe<
            { __typename?: 'DataFlowProperties' } & Pick<
                Types.DataFlowProperties,
                'name' | 'description' | 'project' | 'externalUrl'
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
        editableProperties?: Types.Maybe<
            { __typename?: 'DataFlowEditableProperties' } & Pick<Types.DataFlowEditableProperties, 'description'>
        >;
        ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
        globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
        institutionalMemory?: Types.Maybe<{ __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment>;
        glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
        domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
        status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
        deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
    };

export type GetDataFlowQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetDataFlowQuery = { __typename?: 'Query' } & {
    dataFlow?: Types.Maybe<
        { __typename?: 'DataFlow' } & {
            upstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & PartialLineageResultsFragment>;
            downstream?: Types.Maybe<{ __typename?: 'EntityLineageResult' } & PartialLineageResultsFragment>;
            childJobs?: Types.Maybe<
                { __typename?: 'EntityRelationshipsResult' } & Pick<
                    Types.EntityRelationshipsResult,
                    'start' | 'count' | 'total'
                > & {
                        relationships: Array<
                            { __typename?: 'EntityRelationship' } & {
                                entity?: Types.Maybe<
                                    | { __typename?: 'AccessTokenMetadata' }
                                    | { __typename?: 'Assertion' }
                                    | { __typename?: 'Chart' }
                                    | { __typename?: 'Container' }
                                    | { __typename?: 'CorpGroup' }
                                    | { __typename?: 'CorpUser' }
                                    | { __typename?: 'Dashboard' }
                                    | { __typename?: 'DataFlow' }
                                    | { __typename?: 'DataHubPolicy' }
                                    | { __typename?: 'DataHubRole' }
                                    | { __typename?: 'DataHubView' }
                                    | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type' | 'jobId'> & {
                                              dataFlow?: Types.Maybe<
                                                  { __typename?: 'DataFlow' } & Pick<
                                                      Types.DataFlow,
                                                      'urn' | 'type' | 'orchestrator'
                                                  > & {
                                                          platform: {
                                                              __typename?: 'DataPlatform';
                                                          } & PlatformFieldsFragment;
                                                      }
                                              >;
                                              ownership?: Types.Maybe<
                                                  { __typename?: 'Ownership' } & OwnershipFieldsFragment
                                              >;
                                              properties?: Types.Maybe<
                                                  { __typename?: 'DataJobProperties' } & Pick<
                                                      Types.DataJobProperties,
                                                      'name' | 'description'
                                                  >
                                              >;
                                              editableProperties?: Types.Maybe<
                                                  { __typename?: 'DataJobEditableProperties' } & Pick<
                                                      Types.DataJobEditableProperties,
                                                      'description'
                                                  >
                                              >;
                                              globalTags?: Types.Maybe<
                                                  { __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment
                                              >;
                                              glossaryTerms?: Types.Maybe<
                                                  { __typename?: 'GlossaryTerms' } & GlossaryTermsFragment
                                              >;
                                              deprecation?: Types.Maybe<
                                                  { __typename?: 'Deprecation' } & DeprecationFieldsFragment
                                              >;
                                          })
                                    | { __typename?: 'DataPlatform' }
                                    | { __typename?: 'DataPlatformInstance' }
                                    | { __typename?: 'DataProcessInstance' }
                                    | { __typename?: 'Dataset' }
                                    | { __typename?: 'Domain' }
                                    | { __typename?: 'GlossaryNode' }
                                    | { __typename?: 'GlossaryTerm' }
                                    | { __typename?: 'MLFeature' }
                                    | { __typename?: 'MLFeatureTable' }
                                    | { __typename?: 'MLModel' }
                                    | { __typename?: 'MLModelGroup' }
                                    | { __typename?: 'MLPrimaryKey' }
                                    | { __typename?: 'Notebook' }
                                    | { __typename?: 'Post' }
                                    | { __typename?: 'QueryEntity' }
                                    | { __typename?: 'SchemaFieldEntity' }
                                    | { __typename?: 'Tag' }
                                    | { __typename?: 'Test' }
                                    | { __typename?: 'VersionedDataset' }
                                >;
                            }
                        >;
                    }
            >;
        } & DataFlowFieldsFragment
    >;
};

export type UpdateDataFlowMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.DataFlowUpdateInput;
}>;

export type UpdateDataFlowMutation = { __typename?: 'Mutation' } & {
    updateDataFlow?: Types.Maybe<{ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn'>>;
};

export const DataFlowFieldsFragmentDoc = gql`
    fragment dataFlowFields on DataFlow {
        urn
        type
        exists
        lastIngested
        orchestrator
        flowId
        cluster
        platform {
            ...platformFields
        }
        properties {
            name
            description
            project
            externalUrl
            customProperties {
                key
                value
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
        institutionalMemory {
            ...institutionalMemoryFields
        }
        glossaryTerms {
            ...glossaryTerms
        }
        domain {
            ...entityDomain
        }
        status {
            removed
        }
        deprecation {
            ...deprecationFields
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
`;
export const GetDataFlowDocument = gql`
    query getDataFlow($urn: String!) {
        dataFlow(urn: $urn) {
            ...dataFlowFields
            upstream: lineage(input: { direction: UPSTREAM, start: 0, count: 100 }) {
                ...partialLineageResults
            }
            downstream: lineage(input: { direction: DOWNSTREAM, start: 0, count: 100 }) {
                ...partialLineageResults
            }
            childJobs: relationships(input: { types: ["IsPartOf"], direction: INCOMING, start: 0, count: 100 }) {
                start
                count
                total
                relationships {
                    entity {
                        ... on DataJob {
                            urn
                            type
                            jobId
                            dataFlow {
                                urn
                                type
                                orchestrator
                                platform {
                                    ...platformFields
                                }
                            }
                            ownership {
                                ...ownershipFields
                            }
                            properties {
                                name
                                description
                            }
                            editableProperties {
                                description
                            }
                            globalTags {
                                ...globalTagsFields
                            }
                            glossaryTerms {
                                ...glossaryTerms
                            }
                            deprecation {
                                ...deprecationFields
                            }
                        }
                    }
                }
            }
        }
    }
    ${DataFlowFieldsFragmentDoc}
    ${PartialLineageResultsFragmentDoc}
    ${PlatformFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
`;

/**
 * __useGetDataFlowQuery__
 *
 * To run a query within a React component, call `useGetDataFlowQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDataFlowQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDataFlowQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetDataFlowQuery(baseOptions: Apollo.QueryHookOptions<GetDataFlowQuery, GetDataFlowQueryVariables>) {
    return Apollo.useQuery<GetDataFlowQuery, GetDataFlowQueryVariables>(GetDataFlowDocument, baseOptions);
}
export function useGetDataFlowLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDataFlowQuery, GetDataFlowQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDataFlowQuery, GetDataFlowQueryVariables>(GetDataFlowDocument, baseOptions);
}
export type GetDataFlowQueryHookResult = ReturnType<typeof useGetDataFlowQuery>;
export type GetDataFlowLazyQueryHookResult = ReturnType<typeof useGetDataFlowLazyQuery>;
export type GetDataFlowQueryResult = Apollo.QueryResult<GetDataFlowQuery, GetDataFlowQueryVariables>;
export const UpdateDataFlowDocument = gql`
    mutation updateDataFlow($urn: String!, $input: DataFlowUpdateInput!) {
        updateDataFlow(urn: $urn, input: $input) {
            urn
        }
    }
`;
export type UpdateDataFlowMutationFn = Apollo.MutationFunction<UpdateDataFlowMutation, UpdateDataFlowMutationVariables>;

/**
 * __useUpdateDataFlowMutation__
 *
 * To run a mutation, you first call `useUpdateDataFlowMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateDataFlowMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateDataFlowMutation, { data, loading, error }] = useUpdateDataFlowMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateDataFlowMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateDataFlowMutation, UpdateDataFlowMutationVariables>,
) {
    return Apollo.useMutation<UpdateDataFlowMutation, UpdateDataFlowMutationVariables>(
        UpdateDataFlowDocument,
        baseOptions,
    );
}
export type UpdateDataFlowMutationHookResult = ReturnType<typeof useUpdateDataFlowMutation>;
export type UpdateDataFlowMutationResult = Apollo.MutationResult<UpdateDataFlowMutation>;
export type UpdateDataFlowMutationOptions = Apollo.BaseMutationOptions<
    UpdateDataFlowMutation,
    UpdateDataFlowMutationVariables
>;
