/* eslint-disable */
import * as Types from '../types.generated';

import {
    ParentNodesFieldsFragment,
    OwnershipFieldsFragment,
    GlossaryNodeFragment,
    EntityDomainFragment,
    InstitutionalMemoryFieldsFragment,
    SchemaMetadataFieldsFragment,
    DeprecationFieldsFragment,
} from './fragments.generated';
import { gql } from '@apollo/client';
import {
    ParentNodesFieldsFragmentDoc,
    OwnershipFieldsFragmentDoc,
    GlossaryNodeFragmentDoc,
    EntityDomainFragmentDoc,
    InstitutionalMemoryFieldsFragmentDoc,
    SchemaMetadataFieldsFragmentDoc,
    DeprecationFieldsFragmentDoc,
} from './fragments.generated';
import * as Apollo from '@apollo/client';
export type GetGlossaryTermQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    start?: Types.Maybe<Types.Scalars['Int']>;
    count?: Types.Maybe<Types.Scalars['Int']>;
}>;

export type GetGlossaryTermQuery = { __typename?: 'Query' } & {
    glossaryTerm?: Types.Maybe<
        { __typename?: 'GlossaryTerm' } & Pick<
            Types.GlossaryTerm,
            'urn' | 'type' | 'exists' | 'name' | 'hierarchicalName'
        > & {
                isRelatedTerms?: Types.Maybe<
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
                                        | { __typename?: 'DataJob' }
                                        | { __typename?: 'DataPlatform' }
                                        | { __typename?: 'DataPlatformInstance' }
                                        | { __typename?: 'DataProcessInstance' }
                                        | { __typename?: 'Dataset' }
                                        | { __typename?: 'Domain' }
                                        | { __typename?: 'GlossaryNode' }
                                        | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn'>)
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
                hasRelatedTerms?: Types.Maybe<
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
                                        | { __typename?: 'DataJob' }
                                        | { __typename?: 'DataPlatform' }
                                        | { __typename?: 'DataPlatformInstance' }
                                        | { __typename?: 'DataProcessInstance' }
                                        | { __typename?: 'Dataset' }
                                        | { __typename?: 'Domain' }
                                        | { __typename?: 'GlossaryNode' }
                                        | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn'>)
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
                isAChildren?: Types.Maybe<
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
                                        | { __typename?: 'DataJob' }
                                        | { __typename?: 'DataPlatform' }
                                        | { __typename?: 'DataPlatformInstance' }
                                        | { __typename?: 'DataProcessInstance' }
                                        | { __typename?: 'Dataset' }
                                        | { __typename?: 'Domain' }
                                        | { __typename?: 'GlossaryNode' }
                                        | ({ __typename?: 'GlossaryTerm' } & Pick<
                                              Types.GlossaryTerm,
                                              'urn' | 'hierarchicalName'
                                          >)
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
                containedBy?: Types.Maybe<
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
                                        | { __typename?: 'DataJob' }
                                        | { __typename?: 'DataPlatform' }
                                        | { __typename?: 'DataPlatformInstance' }
                                        | { __typename?: 'DataProcessInstance' }
                                        | { __typename?: 'Dataset' }
                                        | { __typename?: 'Domain' }
                                        | { __typename?: 'GlossaryNode' }
                                        | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn'>)
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
                parentNodes?: Types.Maybe<{ __typename?: 'ParentNodesResult' } & ParentNodesFieldsFragment>;
                ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
                institutionalMemory?: Types.Maybe<
                    { __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment
                >;
                properties?: Types.Maybe<
                    { __typename?: 'GlossaryTermProperties' } & Pick<
                        Types.GlossaryTermProperties,
                        'name' | 'description' | 'termSource' | 'sourceRef' | 'sourceUrl' | 'rawSchema'
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
                schemaMetadata?: Types.Maybe<{ __typename?: 'SchemaMetadata' } & SchemaMetadataFieldsFragment>;
                deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
                privileges?: Types.Maybe<
                    { __typename?: 'EntityPrivileges' } & Pick<Types.EntityPrivileges, 'canManageEntity'>
                >;
            }
    >;
};

export type CreateGlossaryTermMutationVariables = Types.Exact<{
    input: Types.CreateGlossaryEntityInput;
}>;

export type CreateGlossaryTermMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'createGlossaryTerm'>;

export type CreateGlossaryNodeMutationVariables = Types.Exact<{
    input: Types.CreateGlossaryEntityInput;
}>;

export type CreateGlossaryNodeMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'createGlossaryNode'>;

export type AddRelatedTermsMutationVariables = Types.Exact<{
    input: Types.RelatedTermsInput;
}>;

export type AddRelatedTermsMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'addRelatedTerms'>;

export type RemoveRelatedTermsMutationVariables = Types.Exact<{
    input: Types.RelatedTermsInput;
}>;

export type RemoveRelatedTermsMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'removeRelatedTerms'>;

export const GetGlossaryTermDocument = gql`
    query getGlossaryTerm($urn: String!, $start: Int, $count: Int) {
        glossaryTerm(urn: $urn) {
            urn
            type
            exists
            name
            hierarchicalName
            isRelatedTerms: relationships(
                input: { types: ["IsA"], direction: OUTGOING, start: $start, count: $count }
            ) {
                start
                count
                total
                relationships {
                    entity {
                        ... on GlossaryTerm {
                            urn
                        }
                    }
                }
            }
            hasRelatedTerms: relationships(
                input: { types: ["HasA"], direction: OUTGOING, start: $start, count: $count }
            ) {
                start
                count
                total
                relationships {
                    entity {
                        ... on GlossaryTerm {
                            urn
                        }
                    }
                }
            }
            isAChildren: relationships(input: { types: ["IsA"], direction: INCOMING, start: $start, count: $count }) {
                start
                count
                total
                relationships {
                    entity {
                        ... on GlossaryTerm {
                            urn
                            hierarchicalName
                        }
                    }
                }
            }
            containedBy: relationships(input: { types: ["HasA"], direction: INCOMING, start: $start, count: $count }) {
                start
                count
                total
                relationships {
                    entity {
                        ... on GlossaryTerm {
                            urn
                        }
                    }
                }
            }
            parentNodes {
                ...parentNodesFields
            }
            ownership {
                ...ownershipFields
            }
            domain {
                ...entityDomain
            }
            institutionalMemory {
                ...institutionalMemoryFields
            }
            properties {
                name
                description
                termSource
                sourceRef
                sourceUrl
                rawSchema
                customProperties {
                    key
                    value
                }
            }
            schemaMetadata(version: 0) {
                ...schemaMetadataFields
            }
            deprecation {
                ...deprecationFields
            }
            privileges {
                canManageEntity
            }
        }
    }
    ${ParentNodesFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${SchemaMetadataFieldsFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
`;

/**
 * __useGetGlossaryTermQuery__
 *
 * To run a query within a React component, call `useGetGlossaryTermQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetGlossaryTermQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetGlossaryTermQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      start: // value for 'start'
 *      count: // value for 'count'
 *   },
 * });
 */
export function useGetGlossaryTermQuery(
    baseOptions: Apollo.QueryHookOptions<GetGlossaryTermQuery, GetGlossaryTermQueryVariables>,
) {
    return Apollo.useQuery<GetGlossaryTermQuery, GetGlossaryTermQueryVariables>(GetGlossaryTermDocument, baseOptions);
}
export function useGetGlossaryTermLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetGlossaryTermQuery, GetGlossaryTermQueryVariables>,
) {
    return Apollo.useLazyQuery<GetGlossaryTermQuery, GetGlossaryTermQueryVariables>(
        GetGlossaryTermDocument,
        baseOptions,
    );
}
export type GetGlossaryTermQueryHookResult = ReturnType<typeof useGetGlossaryTermQuery>;
export type GetGlossaryTermLazyQueryHookResult = ReturnType<typeof useGetGlossaryTermLazyQuery>;
export type GetGlossaryTermQueryResult = Apollo.QueryResult<GetGlossaryTermQuery, GetGlossaryTermQueryVariables>;
export const CreateGlossaryTermDocument = gql`
    mutation createGlossaryTerm($input: CreateGlossaryEntityInput!) {
        createGlossaryTerm(input: $input)
    }
`;
export type CreateGlossaryTermMutationFn = Apollo.MutationFunction<
    CreateGlossaryTermMutation,
    CreateGlossaryTermMutationVariables
>;

/**
 * __useCreateGlossaryTermMutation__
 *
 * To run a mutation, you first call `useCreateGlossaryTermMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateGlossaryTermMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createGlossaryTermMutation, { data, loading, error }] = useCreateGlossaryTermMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateGlossaryTermMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateGlossaryTermMutation, CreateGlossaryTermMutationVariables>,
) {
    return Apollo.useMutation<CreateGlossaryTermMutation, CreateGlossaryTermMutationVariables>(
        CreateGlossaryTermDocument,
        baseOptions,
    );
}
export type CreateGlossaryTermMutationHookResult = ReturnType<typeof useCreateGlossaryTermMutation>;
export type CreateGlossaryTermMutationResult = Apollo.MutationResult<CreateGlossaryTermMutation>;
export type CreateGlossaryTermMutationOptions = Apollo.BaseMutationOptions<
    CreateGlossaryTermMutation,
    CreateGlossaryTermMutationVariables
>;
export const CreateGlossaryNodeDocument = gql`
    mutation createGlossaryNode($input: CreateGlossaryEntityInput!) {
        createGlossaryNode(input: $input)
    }
`;
export type CreateGlossaryNodeMutationFn = Apollo.MutationFunction<
    CreateGlossaryNodeMutation,
    CreateGlossaryNodeMutationVariables
>;

/**
 * __useCreateGlossaryNodeMutation__
 *
 * To run a mutation, you first call `useCreateGlossaryNodeMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateGlossaryNodeMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createGlossaryNodeMutation, { data, loading, error }] = useCreateGlossaryNodeMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateGlossaryNodeMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateGlossaryNodeMutation, CreateGlossaryNodeMutationVariables>,
) {
    return Apollo.useMutation<CreateGlossaryNodeMutation, CreateGlossaryNodeMutationVariables>(
        CreateGlossaryNodeDocument,
        baseOptions,
    );
}
export type CreateGlossaryNodeMutationHookResult = ReturnType<typeof useCreateGlossaryNodeMutation>;
export type CreateGlossaryNodeMutationResult = Apollo.MutationResult<CreateGlossaryNodeMutation>;
export type CreateGlossaryNodeMutationOptions = Apollo.BaseMutationOptions<
    CreateGlossaryNodeMutation,
    CreateGlossaryNodeMutationVariables
>;
export const AddRelatedTermsDocument = gql`
    mutation addRelatedTerms($input: RelatedTermsInput!) {
        addRelatedTerms(input: $input)
    }
`;
export type AddRelatedTermsMutationFn = Apollo.MutationFunction<
    AddRelatedTermsMutation,
    AddRelatedTermsMutationVariables
>;

/**
 * __useAddRelatedTermsMutation__
 *
 * To run a mutation, you first call `useAddRelatedTermsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useAddRelatedTermsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [addRelatedTermsMutation, { data, loading, error }] = useAddRelatedTermsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useAddRelatedTermsMutation(
    baseOptions?: Apollo.MutationHookOptions<AddRelatedTermsMutation, AddRelatedTermsMutationVariables>,
) {
    return Apollo.useMutation<AddRelatedTermsMutation, AddRelatedTermsMutationVariables>(
        AddRelatedTermsDocument,
        baseOptions,
    );
}
export type AddRelatedTermsMutationHookResult = ReturnType<typeof useAddRelatedTermsMutation>;
export type AddRelatedTermsMutationResult = Apollo.MutationResult<AddRelatedTermsMutation>;
export type AddRelatedTermsMutationOptions = Apollo.BaseMutationOptions<
    AddRelatedTermsMutation,
    AddRelatedTermsMutationVariables
>;
export const RemoveRelatedTermsDocument = gql`
    mutation removeRelatedTerms($input: RelatedTermsInput!) {
        removeRelatedTerms(input: $input)
    }
`;
export type RemoveRelatedTermsMutationFn = Apollo.MutationFunction<
    RemoveRelatedTermsMutation,
    RemoveRelatedTermsMutationVariables
>;

/**
 * __useRemoveRelatedTermsMutation__
 *
 * To run a mutation, you first call `useRemoveRelatedTermsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useRemoveRelatedTermsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [removeRelatedTermsMutation, { data, loading, error }] = useRemoveRelatedTermsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useRemoveRelatedTermsMutation(
    baseOptions?: Apollo.MutationHookOptions<RemoveRelatedTermsMutation, RemoveRelatedTermsMutationVariables>,
) {
    return Apollo.useMutation<RemoveRelatedTermsMutation, RemoveRelatedTermsMutationVariables>(
        RemoveRelatedTermsDocument,
        baseOptions,
    );
}
export type RemoveRelatedTermsMutationHookResult = ReturnType<typeof useRemoveRelatedTermsMutation>;
export type RemoveRelatedTermsMutationResult = Apollo.MutationResult<RemoveRelatedTermsMutation>;
export type RemoveRelatedTermsMutationOptions = Apollo.BaseMutationOptions<
    RemoveRelatedTermsMutation,
    RemoveRelatedTermsMutationVariables
>;
