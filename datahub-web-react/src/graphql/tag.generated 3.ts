/* eslint-disable */
import * as Types from '../types.generated';

import { OwnershipFieldsFragment, ParentNodesFieldsFragment, GlossaryNodeFragment } from './fragments.generated';
import { gql } from '@apollo/client';
import {
    OwnershipFieldsFragmentDoc,
    ParentNodesFieldsFragmentDoc,
    GlossaryNodeFragmentDoc,
} from './fragments.generated';
import * as Apollo from '@apollo/client';
export type GetTagQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetTagQuery = { __typename?: 'Query' } & {
    tag?: Types.Maybe<
        { __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'name' | 'description'> & {
                properties?: Types.Maybe<
                    { __typename?: 'TagProperties' } & Pick<Types.TagProperties, 'name' | 'description' | 'colorHex'>
                >;
                ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
            }
    >;
};

export type UpdateTagMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.TagUpdateInput;
}>;

export type UpdateTagMutation = { __typename?: 'Mutation' } & {
    updateTag?: Types.Maybe<
        { __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'name' | 'description'> & {
                ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
            }
    >;
};

export type DeleteTagMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeleteTagMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deleteTag'>;

export type CreateTagMutationVariables = Types.Exact<{
    input: Types.CreateTagInput;
}>;

export type CreateTagMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'createTag'>;

export const GetTagDocument = gql`
    query getTag($urn: String!) {
        tag(urn: $urn) {
            urn
            name
            description
            properties {
                name
                description
                colorHex
            }
            ownership {
                ...ownershipFields
            }
        }
    }
    ${OwnershipFieldsFragmentDoc}
`;

/**
 * __useGetTagQuery__
 *
 * To run a query within a React component, call `useGetTagQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetTagQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetTagQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetTagQuery(baseOptions: Apollo.QueryHookOptions<GetTagQuery, GetTagQueryVariables>) {
    return Apollo.useQuery<GetTagQuery, GetTagQueryVariables>(GetTagDocument, baseOptions);
}
export function useGetTagLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetTagQuery, GetTagQueryVariables>) {
    return Apollo.useLazyQuery<GetTagQuery, GetTagQueryVariables>(GetTagDocument, baseOptions);
}
export type GetTagQueryHookResult = ReturnType<typeof useGetTagQuery>;
export type GetTagLazyQueryHookResult = ReturnType<typeof useGetTagLazyQuery>;
export type GetTagQueryResult = Apollo.QueryResult<GetTagQuery, GetTagQueryVariables>;
export const UpdateTagDocument = gql`
    mutation updateTag($urn: String!, $input: TagUpdateInput!) {
        updateTag(urn: $urn, input: $input) {
            urn
            name
            description
            ownership {
                ...ownershipFields
            }
        }
    }
    ${OwnershipFieldsFragmentDoc}
`;
export type UpdateTagMutationFn = Apollo.MutationFunction<UpdateTagMutation, UpdateTagMutationVariables>;

/**
 * __useUpdateTagMutation__
 *
 * To run a mutation, you first call `useUpdateTagMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateTagMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateTagMutation, { data, loading, error }] = useUpdateTagMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateTagMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateTagMutation, UpdateTagMutationVariables>,
) {
    return Apollo.useMutation<UpdateTagMutation, UpdateTagMutationVariables>(UpdateTagDocument, baseOptions);
}
export type UpdateTagMutationHookResult = ReturnType<typeof useUpdateTagMutation>;
export type UpdateTagMutationResult = Apollo.MutationResult<UpdateTagMutation>;
export type UpdateTagMutationOptions = Apollo.BaseMutationOptions<UpdateTagMutation, UpdateTagMutationVariables>;
export const DeleteTagDocument = gql`
    mutation deleteTag($urn: String!) {
        deleteTag(urn: $urn)
    }
`;
export type DeleteTagMutationFn = Apollo.MutationFunction<DeleteTagMutation, DeleteTagMutationVariables>;

/**
 * __useDeleteTagMutation__
 *
 * To run a mutation, you first call `useDeleteTagMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeleteTagMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deleteTagMutation, { data, loading, error }] = useDeleteTagMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeleteTagMutation(
    baseOptions?: Apollo.MutationHookOptions<DeleteTagMutation, DeleteTagMutationVariables>,
) {
    return Apollo.useMutation<DeleteTagMutation, DeleteTagMutationVariables>(DeleteTagDocument, baseOptions);
}
export type DeleteTagMutationHookResult = ReturnType<typeof useDeleteTagMutation>;
export type DeleteTagMutationResult = Apollo.MutationResult<DeleteTagMutation>;
export type DeleteTagMutationOptions = Apollo.BaseMutationOptions<DeleteTagMutation, DeleteTagMutationVariables>;
export const CreateTagDocument = gql`
    mutation createTag($input: CreateTagInput!) {
        createTag(input: $input)
    }
`;
export type CreateTagMutationFn = Apollo.MutationFunction<CreateTagMutation, CreateTagMutationVariables>;

/**
 * __useCreateTagMutation__
 *
 * To run a mutation, you first call `useCreateTagMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateTagMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createTagMutation, { data, loading, error }] = useCreateTagMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateTagMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateTagMutation, CreateTagMutationVariables>,
) {
    return Apollo.useMutation<CreateTagMutation, CreateTagMutationVariables>(CreateTagDocument, baseOptions);
}
export type CreateTagMutationHookResult = ReturnType<typeof useCreateTagMutation>;
export type CreateTagMutationResult = Apollo.MutationResult<CreateTagMutation>;
export type CreateTagMutationOptions = Apollo.BaseMutationOptions<CreateTagMutation, CreateTagMutationVariables>;
