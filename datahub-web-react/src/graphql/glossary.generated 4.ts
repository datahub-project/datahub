/* eslint-disable */
import * as Types from '../types.generated';

import { ChildGlossaryTermFragment } from './glossaryNode.generated';
import { GlossaryNodeFragment } from './fragments.generated';
import { gql } from '@apollo/client';
import { ChildGlossaryTermFragmentDoc } from './glossaryNode.generated';
import { GlossaryNodeFragmentDoc } from './fragments.generated';
import * as Apollo from '@apollo/client';
export type GetRootGlossaryTermsQueryVariables = Types.Exact<{ [key: string]: never }>;

export type GetRootGlossaryTermsQuery = { __typename?: 'Query' } & {
    getRootGlossaryTerms?: Types.Maybe<
        { __typename?: 'GetRootGlossaryTermsResult' } & Pick<
            Types.GetRootGlossaryTermsResult,
            'count' | 'start' | 'total'
        > & { terms: Array<{ __typename?: 'GlossaryTerm' } & ChildGlossaryTermFragment> }
    >;
};

export type GetRootGlossaryNodesQueryVariables = Types.Exact<{ [key: string]: never }>;

export type GetRootGlossaryNodesQuery = { __typename?: 'Query' } & {
    getRootGlossaryNodes?: Types.Maybe<
        { __typename?: 'GetRootGlossaryNodesResult' } & Pick<
            Types.GetRootGlossaryNodesResult,
            'count' | 'start' | 'total'
        > & { nodes: Array<{ __typename?: 'GlossaryNode' } & GlossaryNodeFragment> }
    >;
};

export type UpdateParentNodeMutationVariables = Types.Exact<{
    input: Types.UpdateParentNodeInput;
}>;

export type UpdateParentNodeMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updateParentNode'>;

export type DeleteGlossaryEntityMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeleteGlossaryEntityMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deleteGlossaryEntity'>;

export const GetRootGlossaryTermsDocument = gql`
    query getRootGlossaryTerms {
        getRootGlossaryTerms(input: { start: 0, count: 1000 }) {
            count
            start
            total
            terms {
                ...childGlossaryTerm
            }
        }
    }
    ${ChildGlossaryTermFragmentDoc}
`;

/**
 * __useGetRootGlossaryTermsQuery__
 *
 * To run a query within a React component, call `useGetRootGlossaryTermsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetRootGlossaryTermsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetRootGlossaryTermsQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetRootGlossaryTermsQuery(
    baseOptions?: Apollo.QueryHookOptions<GetRootGlossaryTermsQuery, GetRootGlossaryTermsQueryVariables>,
) {
    return Apollo.useQuery<GetRootGlossaryTermsQuery, GetRootGlossaryTermsQueryVariables>(
        GetRootGlossaryTermsDocument,
        baseOptions,
    );
}
export function useGetRootGlossaryTermsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetRootGlossaryTermsQuery, GetRootGlossaryTermsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetRootGlossaryTermsQuery, GetRootGlossaryTermsQueryVariables>(
        GetRootGlossaryTermsDocument,
        baseOptions,
    );
}
export type GetRootGlossaryTermsQueryHookResult = ReturnType<typeof useGetRootGlossaryTermsQuery>;
export type GetRootGlossaryTermsLazyQueryHookResult = ReturnType<typeof useGetRootGlossaryTermsLazyQuery>;
export type GetRootGlossaryTermsQueryResult = Apollo.QueryResult<
    GetRootGlossaryTermsQuery,
    GetRootGlossaryTermsQueryVariables
>;
export const GetRootGlossaryNodesDocument = gql`
    query getRootGlossaryNodes {
        getRootGlossaryNodes(input: { start: 0, count: 1000 }) {
            count
            start
            total
            nodes {
                ...glossaryNode
            }
        }
    }
    ${GlossaryNodeFragmentDoc}
`;

/**
 * __useGetRootGlossaryNodesQuery__
 *
 * To run a query within a React component, call `useGetRootGlossaryNodesQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetRootGlossaryNodesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetRootGlossaryNodesQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetRootGlossaryNodesQuery(
    baseOptions?: Apollo.QueryHookOptions<GetRootGlossaryNodesQuery, GetRootGlossaryNodesQueryVariables>,
) {
    return Apollo.useQuery<GetRootGlossaryNodesQuery, GetRootGlossaryNodesQueryVariables>(
        GetRootGlossaryNodesDocument,
        baseOptions,
    );
}
export function useGetRootGlossaryNodesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetRootGlossaryNodesQuery, GetRootGlossaryNodesQueryVariables>,
) {
    return Apollo.useLazyQuery<GetRootGlossaryNodesQuery, GetRootGlossaryNodesQueryVariables>(
        GetRootGlossaryNodesDocument,
        baseOptions,
    );
}
export type GetRootGlossaryNodesQueryHookResult = ReturnType<typeof useGetRootGlossaryNodesQuery>;
export type GetRootGlossaryNodesLazyQueryHookResult = ReturnType<typeof useGetRootGlossaryNodesLazyQuery>;
export type GetRootGlossaryNodesQueryResult = Apollo.QueryResult<
    GetRootGlossaryNodesQuery,
    GetRootGlossaryNodesQueryVariables
>;
export const UpdateParentNodeDocument = gql`
    mutation updateParentNode($input: UpdateParentNodeInput!) {
        updateParentNode(input: $input)
    }
`;
export type UpdateParentNodeMutationFn = Apollo.MutationFunction<
    UpdateParentNodeMutation,
    UpdateParentNodeMutationVariables
>;

/**
 * __useUpdateParentNodeMutation__
 *
 * To run a mutation, you first call `useUpdateParentNodeMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateParentNodeMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateParentNodeMutation, { data, loading, error }] = useUpdateParentNodeMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateParentNodeMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateParentNodeMutation, UpdateParentNodeMutationVariables>,
) {
    return Apollo.useMutation<UpdateParentNodeMutation, UpdateParentNodeMutationVariables>(
        UpdateParentNodeDocument,
        baseOptions,
    );
}
export type UpdateParentNodeMutationHookResult = ReturnType<typeof useUpdateParentNodeMutation>;
export type UpdateParentNodeMutationResult = Apollo.MutationResult<UpdateParentNodeMutation>;
export type UpdateParentNodeMutationOptions = Apollo.BaseMutationOptions<
    UpdateParentNodeMutation,
    UpdateParentNodeMutationVariables
>;
export const DeleteGlossaryEntityDocument = gql`
    mutation deleteGlossaryEntity($urn: String!) {
        deleteGlossaryEntity(urn: $urn)
    }
`;
export type DeleteGlossaryEntityMutationFn = Apollo.MutationFunction<
    DeleteGlossaryEntityMutation,
    DeleteGlossaryEntityMutationVariables
>;

/**
 * __useDeleteGlossaryEntityMutation__
 *
 * To run a mutation, you first call `useDeleteGlossaryEntityMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeleteGlossaryEntityMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deleteGlossaryEntityMutation, { data, loading, error }] = useDeleteGlossaryEntityMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeleteGlossaryEntityMutation(
    baseOptions?: Apollo.MutationHookOptions<DeleteGlossaryEntityMutation, DeleteGlossaryEntityMutationVariables>,
) {
    return Apollo.useMutation<DeleteGlossaryEntityMutation, DeleteGlossaryEntityMutationVariables>(
        DeleteGlossaryEntityDocument,
        baseOptions,
    );
}
export type DeleteGlossaryEntityMutationHookResult = ReturnType<typeof useDeleteGlossaryEntityMutation>;
export type DeleteGlossaryEntityMutationResult = Apollo.MutationResult<DeleteGlossaryEntityMutation>;
export type DeleteGlossaryEntityMutationOptions = Apollo.BaseMutationOptions<
    DeleteGlossaryEntityMutation,
    DeleteGlossaryEntityMutationVariables
>;
