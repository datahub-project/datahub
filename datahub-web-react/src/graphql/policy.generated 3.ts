/* eslint-disable */
import * as Types from '../types.generated';

import {
    SearchResultFields_AccessTokenMetadata_Fragment,
    SearchResultFields_Assertion_Fragment,
    SearchResultFields_Chart_Fragment,
    SearchResultFields_Container_Fragment,
    SearchResultFields_CorpGroup_Fragment,
    SearchResultFields_CorpUser_Fragment,
    SearchResultFields_Dashboard_Fragment,
    SearchResultFields_DataFlow_Fragment,
    SearchResultFields_DataHubPolicy_Fragment,
    SearchResultFields_DataHubRole_Fragment,
    SearchResultFields_DataHubView_Fragment,
    SearchResultFields_DataJob_Fragment,
    SearchResultFields_DataPlatform_Fragment,
    SearchResultFields_DataPlatformInstance_Fragment,
    SearchResultFields_DataProcessInstance_Fragment,
    SearchResultFields_Dataset_Fragment,
    SearchResultFields_Domain_Fragment,
    SearchResultFields_GlossaryNode_Fragment,
    SearchResultFields_GlossaryTerm_Fragment,
    SearchResultFields_MlFeature_Fragment,
    SearchResultFields_MlFeatureTable_Fragment,
    SearchResultFields_MlModel_Fragment,
    SearchResultFields_MlModelGroup_Fragment,
    SearchResultFields_MlPrimaryKey_Fragment,
    SearchResultFields_Notebook_Fragment,
    SearchResultFields_Post_Fragment,
    SearchResultFields_QueryEntity_Fragment,
    SearchResultFields_SchemaFieldEntity_Fragment,
    SearchResultFields_Tag_Fragment,
    SearchResultFields_Test_Fragment,
    SearchResultFields_VersionedDataset_Fragment,
} from './search.generated';
import { gql } from '@apollo/client';
import { SearchResultFieldsFragmentDoc } from './search.generated';
import * as Apollo from '@apollo/client';
export type ListPoliciesQueryVariables = Types.Exact<{
    input: Types.ListPoliciesInput;
}>;

export type ListPoliciesQuery = { __typename?: 'Query' } & {
    listPolicies?: Types.Maybe<
        { __typename?: 'ListPoliciesResult' } & Pick<Types.ListPoliciesResult, 'start' | 'count' | 'total'> & {
                policies: Array<
                    { __typename?: 'Policy' } & Pick<
                        Types.Policy,
                        'urn' | 'type' | 'name' | 'description' | 'state' | 'privileges' | 'editable'
                    > & {
                            resources?: Types.Maybe<
                                { __typename?: 'ResourceFilter' } & Pick<
                                    Types.ResourceFilter,
                                    'type' | 'allResources' | 'resources'
                                > & {
                                        filter?: Types.Maybe<
                                            { __typename?: 'PolicyMatchFilter' } & {
                                                criteria?: Types.Maybe<
                                                    Array<
                                                        { __typename?: 'PolicyMatchCriterion' } & Pick<
                                                            Types.PolicyMatchCriterion,
                                                            'field' | 'condition'
                                                        > & {
                                                                values: Array<
                                                                    { __typename?: 'PolicyMatchCriterionValue' } & Pick<
                                                                        Types.PolicyMatchCriterionValue,
                                                                        'value'
                                                                    > & {
                                                                            entity?: Types.Maybe<
                                                                                | ({
                                                                                      __typename?: 'AccessTokenMetadata';
                                                                                  } & SearchResultFields_AccessTokenMetadata_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Assertion';
                                                                                  } & SearchResultFields_Assertion_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Chart';
                                                                                  } & SearchResultFields_Chart_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Container';
                                                                                  } & SearchResultFields_Container_Fragment)
                                                                                | ({
                                                                                      __typename?: 'CorpGroup';
                                                                                  } & SearchResultFields_CorpGroup_Fragment)
                                                                                | ({
                                                                                      __typename?: 'CorpUser';
                                                                                  } & SearchResultFields_CorpUser_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Dashboard';
                                                                                  } & SearchResultFields_Dashboard_Fragment)
                                                                                | ({
                                                                                      __typename?: 'DataFlow';
                                                                                  } & SearchResultFields_DataFlow_Fragment)
                                                                                | ({
                                                                                      __typename?: 'DataHubPolicy';
                                                                                  } & SearchResultFields_DataHubPolicy_Fragment)
                                                                                | ({
                                                                                      __typename?: 'DataHubRole';
                                                                                  } & SearchResultFields_DataHubRole_Fragment)
                                                                                | ({
                                                                                      __typename?: 'DataHubView';
                                                                                  } & SearchResultFields_DataHubView_Fragment)
                                                                                | ({
                                                                                      __typename?: 'DataJob';
                                                                                  } & SearchResultFields_DataJob_Fragment)
                                                                                | ({
                                                                                      __typename?: 'DataPlatform';
                                                                                  } & SearchResultFields_DataPlatform_Fragment)
                                                                                | ({
                                                                                      __typename?: 'DataPlatformInstance';
                                                                                  } & SearchResultFields_DataPlatformInstance_Fragment)
                                                                                | ({
                                                                                      __typename?: 'DataProcessInstance';
                                                                                  } & SearchResultFields_DataProcessInstance_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Dataset';
                                                                                  } & SearchResultFields_Dataset_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Domain';
                                                                                  } & SearchResultFields_Domain_Fragment)
                                                                                | ({
                                                                                      __typename?: 'GlossaryNode';
                                                                                  } & SearchResultFields_GlossaryNode_Fragment)
                                                                                | ({
                                                                                      __typename?: 'GlossaryTerm';
                                                                                  } & SearchResultFields_GlossaryTerm_Fragment)
                                                                                | ({
                                                                                      __typename?: 'MLFeature';
                                                                                  } & SearchResultFields_MlFeature_Fragment)
                                                                                | ({
                                                                                      __typename?: 'MLFeatureTable';
                                                                                  } & SearchResultFields_MlFeatureTable_Fragment)
                                                                                | ({
                                                                                      __typename?: 'MLModel';
                                                                                  } & SearchResultFields_MlModel_Fragment)
                                                                                | ({
                                                                                      __typename?: 'MLModelGroup';
                                                                                  } & SearchResultFields_MlModelGroup_Fragment)
                                                                                | ({
                                                                                      __typename?: 'MLPrimaryKey';
                                                                                  } & SearchResultFields_MlPrimaryKey_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Notebook';
                                                                                  } & SearchResultFields_Notebook_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Post';
                                                                                  } & SearchResultFields_Post_Fragment)
                                                                                | ({
                                                                                      __typename?: 'QueryEntity';
                                                                                  } & SearchResultFields_QueryEntity_Fragment)
                                                                                | ({
                                                                                      __typename?: 'SchemaFieldEntity';
                                                                                  } & SearchResultFields_SchemaFieldEntity_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Tag';
                                                                                  } & SearchResultFields_Tag_Fragment)
                                                                                | ({
                                                                                      __typename?: 'Test';
                                                                                  } & SearchResultFields_Test_Fragment)
                                                                                | ({
                                                                                      __typename?: 'VersionedDataset';
                                                                                  } & SearchResultFields_VersionedDataset_Fragment)
                                                                            >;
                                                                        }
                                                                >;
                                                            }
                                                    >
                                                >;
                                            }
                                        >;
                                    }
                            >;
                            actors: { __typename?: 'ActorFilter' } & Pick<
                                Types.ActorFilter,
                                'users' | 'groups' | 'roles' | 'allUsers' | 'allGroups' | 'resourceOwners'
                            > & {
                                    resolvedUsers?: Types.Maybe<
                                        Array<
                                            { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'username' | 'urn'> & {
                                                    properties?: Types.Maybe<
                                                        { __typename?: 'CorpUserProperties' } & Pick<
                                                            Types.CorpUserProperties,
                                                            | 'active'
                                                            | 'displayName'
                                                            | 'title'
                                                            | 'firstName'
                                                            | 'lastName'
                                                            | 'fullName'
                                                            | 'email'
                                                        >
                                                    >;
                                                    editableProperties?: Types.Maybe<
                                                        { __typename?: 'CorpUserEditableProperties' } & Pick<
                                                            Types.CorpUserEditableProperties,
                                                            'displayName' | 'pictureLink' | 'teams' | 'title' | 'skills'
                                                        >
                                                    >;
                                                }
                                        >
                                    >;
                                    resolvedGroups?: Types.Maybe<
                                        Array<
                                            { __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'name' | 'urn'> & {
                                                    properties?: Types.Maybe<
                                                        { __typename?: 'CorpGroupProperties' } & Pick<
                                                            Types.CorpGroupProperties,
                                                            'displayName' | 'description' | 'email'
                                                        >
                                                    >;
                                                    editableProperties?: Types.Maybe<
                                                        { __typename?: 'CorpGroupEditableProperties' } & Pick<
                                                            Types.CorpGroupEditableProperties,
                                                            'description' | 'slack' | 'email'
                                                        >
                                                    >;
                                                }
                                        >
                                    >;
                                    resolvedRoles?: Types.Maybe<
                                        Array<
                                            { __typename?: 'DataHubRole' } & Pick<
                                                Types.DataHubRole,
                                                'urn' | 'type' | 'name' | 'description'
                                            >
                                        >
                                    >;
                                };
                        }
                >;
            }
    >;
};

export type GetGrantedPrivilegesQueryVariables = Types.Exact<{
    input: Types.GetGrantedPrivilegesInput;
}>;

export type GetGrantedPrivilegesQuery = { __typename?: 'Query' } & {
    getGrantedPrivileges?: Types.Maybe<{ __typename?: 'Privileges' } & Pick<Types.Privileges, 'privileges'>>;
};

export type CreatePolicyMutationVariables = Types.Exact<{
    input: Types.PolicyUpdateInput;
}>;

export type CreatePolicyMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'createPolicy'>;

export type UpdatePolicyMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.PolicyUpdateInput;
}>;

export type UpdatePolicyMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updatePolicy'>;

export type DeletePolicyMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeletePolicyMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deletePolicy'>;

export const ListPoliciesDocument = gql`
    query listPolicies($input: ListPoliciesInput!) {
        listPolicies(input: $input) {
            start
            count
            total
            policies {
                urn
                type
                name
                description
                state
                resources {
                    type
                    allResources
                    resources
                    filter {
                        criteria {
                            field
                            values {
                                value
                                entity {
                                    ...searchResultFields
                                }
                            }
                            condition
                        }
                    }
                }
                privileges
                actors {
                    users
                    groups
                    roles
                    allUsers
                    allGroups
                    resourceOwners
                    resolvedUsers {
                        username
                        urn
                        properties {
                            active
                            displayName
                            title
                            firstName
                            lastName
                            fullName
                            email
                        }
                        editableProperties {
                            displayName
                            pictureLink
                            teams
                            title
                            skills
                        }
                    }
                    resolvedGroups {
                        name
                        urn
                        properties {
                            displayName
                            description
                            email
                        }
                        editableProperties {
                            description
                            slack
                            email
                        }
                    }
                    resolvedRoles {
                        urn
                        type
                        name
                        description
                    }
                }
                editable
            }
        }
    }
    ${SearchResultFieldsFragmentDoc}
`;

/**
 * __useListPoliciesQuery__
 *
 * To run a query within a React component, call `useListPoliciesQuery` and pass it any options that fit your needs.
 * When your component renders, `useListPoliciesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListPoliciesQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useListPoliciesQuery(
    baseOptions: Apollo.QueryHookOptions<ListPoliciesQuery, ListPoliciesQueryVariables>,
) {
    return Apollo.useQuery<ListPoliciesQuery, ListPoliciesQueryVariables>(ListPoliciesDocument, baseOptions);
}
export function useListPoliciesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListPoliciesQuery, ListPoliciesQueryVariables>,
) {
    return Apollo.useLazyQuery<ListPoliciesQuery, ListPoliciesQueryVariables>(ListPoliciesDocument, baseOptions);
}
export type ListPoliciesQueryHookResult = ReturnType<typeof useListPoliciesQuery>;
export type ListPoliciesLazyQueryHookResult = ReturnType<typeof useListPoliciesLazyQuery>;
export type ListPoliciesQueryResult = Apollo.QueryResult<ListPoliciesQuery, ListPoliciesQueryVariables>;
export const GetGrantedPrivilegesDocument = gql`
    query getGrantedPrivileges($input: GetGrantedPrivilegesInput!) {
        getGrantedPrivileges(input: $input) {
            privileges
        }
    }
`;

/**
 * __useGetGrantedPrivilegesQuery__
 *
 * To run a query within a React component, call `useGetGrantedPrivilegesQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetGrantedPrivilegesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetGrantedPrivilegesQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetGrantedPrivilegesQuery(
    baseOptions: Apollo.QueryHookOptions<GetGrantedPrivilegesQuery, GetGrantedPrivilegesQueryVariables>,
) {
    return Apollo.useQuery<GetGrantedPrivilegesQuery, GetGrantedPrivilegesQueryVariables>(
        GetGrantedPrivilegesDocument,
        baseOptions,
    );
}
export function useGetGrantedPrivilegesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetGrantedPrivilegesQuery, GetGrantedPrivilegesQueryVariables>,
) {
    return Apollo.useLazyQuery<GetGrantedPrivilegesQuery, GetGrantedPrivilegesQueryVariables>(
        GetGrantedPrivilegesDocument,
        baseOptions,
    );
}
export type GetGrantedPrivilegesQueryHookResult = ReturnType<typeof useGetGrantedPrivilegesQuery>;
export type GetGrantedPrivilegesLazyQueryHookResult = ReturnType<typeof useGetGrantedPrivilegesLazyQuery>;
export type GetGrantedPrivilegesQueryResult = Apollo.QueryResult<
    GetGrantedPrivilegesQuery,
    GetGrantedPrivilegesQueryVariables
>;
export const CreatePolicyDocument = gql`
    mutation createPolicy($input: PolicyUpdateInput!) {
        createPolicy(input: $input)
    }
`;
export type CreatePolicyMutationFn = Apollo.MutationFunction<CreatePolicyMutation, CreatePolicyMutationVariables>;

/**
 * __useCreatePolicyMutation__
 *
 * To run a mutation, you first call `useCreatePolicyMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreatePolicyMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createPolicyMutation, { data, loading, error }] = useCreatePolicyMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreatePolicyMutation(
    baseOptions?: Apollo.MutationHookOptions<CreatePolicyMutation, CreatePolicyMutationVariables>,
) {
    return Apollo.useMutation<CreatePolicyMutation, CreatePolicyMutationVariables>(CreatePolicyDocument, baseOptions);
}
export type CreatePolicyMutationHookResult = ReturnType<typeof useCreatePolicyMutation>;
export type CreatePolicyMutationResult = Apollo.MutationResult<CreatePolicyMutation>;
export type CreatePolicyMutationOptions = Apollo.BaseMutationOptions<
    CreatePolicyMutation,
    CreatePolicyMutationVariables
>;
export const UpdatePolicyDocument = gql`
    mutation updatePolicy($urn: String!, $input: PolicyUpdateInput!) {
        updatePolicy(urn: $urn, input: $input)
    }
`;
export type UpdatePolicyMutationFn = Apollo.MutationFunction<UpdatePolicyMutation, UpdatePolicyMutationVariables>;

/**
 * __useUpdatePolicyMutation__
 *
 * To run a mutation, you first call `useUpdatePolicyMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdatePolicyMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updatePolicyMutation, { data, loading, error }] = useUpdatePolicyMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdatePolicyMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdatePolicyMutation, UpdatePolicyMutationVariables>,
) {
    return Apollo.useMutation<UpdatePolicyMutation, UpdatePolicyMutationVariables>(UpdatePolicyDocument, baseOptions);
}
export type UpdatePolicyMutationHookResult = ReturnType<typeof useUpdatePolicyMutation>;
export type UpdatePolicyMutationResult = Apollo.MutationResult<UpdatePolicyMutation>;
export type UpdatePolicyMutationOptions = Apollo.BaseMutationOptions<
    UpdatePolicyMutation,
    UpdatePolicyMutationVariables
>;
export const DeletePolicyDocument = gql`
    mutation deletePolicy($urn: String!) {
        deletePolicy(urn: $urn)
    }
`;
export type DeletePolicyMutationFn = Apollo.MutationFunction<DeletePolicyMutation, DeletePolicyMutationVariables>;

/**
 * __useDeletePolicyMutation__
 *
 * To run a mutation, you first call `useDeletePolicyMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeletePolicyMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deletePolicyMutation, { data, loading, error }] = useDeletePolicyMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeletePolicyMutation(
    baseOptions?: Apollo.MutationHookOptions<DeletePolicyMutation, DeletePolicyMutationVariables>,
) {
    return Apollo.useMutation<DeletePolicyMutation, DeletePolicyMutationVariables>(DeletePolicyDocument, baseOptions);
}
export type DeletePolicyMutationHookResult = ReturnType<typeof useDeletePolicyMutation>;
export type DeletePolicyMutationResult = Apollo.MutationResult<DeletePolicyMutation>;
export type DeletePolicyMutationOptions = Apollo.BaseMutationOptions<
    DeletePolicyMutation,
    DeletePolicyMutationVariables
>;
