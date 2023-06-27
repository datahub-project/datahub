/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type QueryFragment = { __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn'> & {
        properties?: Types.Maybe<
            { __typename?: 'QueryProperties' } & Pick<Types.QueryProperties, 'description' | 'name' | 'source'> & {
                    statement: { __typename?: 'QueryStatement' } & Pick<Types.QueryStatement, 'value' | 'language'>;
                    created: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time' | 'actor'>;
                    lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time' | 'actor'>;
                }
        >;
        subjects?: Types.Maybe<
            Array<
                { __typename?: 'QuerySubject' } & {
                    dataset: { __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'name'>;
                }
            >
        >;
    };

export type ListQueriesQueryVariables = Types.Exact<{
    input: Types.ListQueriesInput;
}>;

export type ListQueriesQuery = { __typename?: 'Query' } & {
    listQueries?: Types.Maybe<
        { __typename?: 'ListQueriesResult' } & Pick<Types.ListQueriesResult, 'start' | 'total' | 'count'> & {
                queries: Array<{ __typename?: 'QueryEntity' } & QueryFragment>;
            }
    >;
};

export type CreateQueryMutationVariables = Types.Exact<{
    input: Types.CreateQueryInput;
}>;

export type CreateQueryMutation = { __typename?: 'Mutation' } & {
    createQuery?: Types.Maybe<{ __typename?: 'QueryEntity' } & QueryFragment>;
};

export type UpdateQueryMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.UpdateQueryInput;
}>;

export type UpdateQueryMutation = { __typename?: 'Mutation' } & {
    updateQuery?: Types.Maybe<{ __typename?: 'QueryEntity' } & QueryFragment>;
};

export type DeleteQueryMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeleteQueryMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deleteQuery'>;

export const QueryFragmentDoc = gql`
    fragment query on QueryEntity {
        urn
        properties {
            description
            name
            source
            statement {
                value
                language
            }
            created {
                time
                actor
            }
            lastModified {
                time
                actor
            }
        }
        subjects {
            dataset {
                urn
                name
            }
        }
    }
`;
export const ListQueriesDocument = gql`
    query listQueries($input: ListQueriesInput!) {
        listQueries(input: $input) {
            start
            total
            count
            queries {
                ...query
            }
        }
    }
    ${QueryFragmentDoc}
`;

/**
 * __useListQueriesQuery__
 *
 * To run a query within a React component, call `useListQueriesQuery` and pass it any options that fit your needs.
 * When your component renders, `useListQueriesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListQueriesQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useListQueriesQuery(baseOptions: Apollo.QueryHookOptions<ListQueriesQuery, ListQueriesQueryVariables>) {
    return Apollo.useQuery<ListQueriesQuery, ListQueriesQueryVariables>(ListQueriesDocument, baseOptions);
}
export function useListQueriesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListQueriesQuery, ListQueriesQueryVariables>,
) {
    return Apollo.useLazyQuery<ListQueriesQuery, ListQueriesQueryVariables>(ListQueriesDocument, baseOptions);
}
export type ListQueriesQueryHookResult = ReturnType<typeof useListQueriesQuery>;
export type ListQueriesLazyQueryHookResult = ReturnType<typeof useListQueriesLazyQuery>;
export type ListQueriesQueryResult = Apollo.QueryResult<ListQueriesQuery, ListQueriesQueryVariables>;
export const CreateQueryDocument = gql`
    mutation createQuery($input: CreateQueryInput!) {
        createQuery(input: $input) {
            ...query
        }
    }
    ${QueryFragmentDoc}
`;
export type CreateQueryMutationFn = Apollo.MutationFunction<CreateQueryMutation, CreateQueryMutationVariables>;

/**
 * __useCreateQueryMutation__
 *
 * To run a mutation, you first call `useCreateQueryMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateQueryMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createQueryMutation, { data, loading, error }] = useCreateQueryMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateQueryMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateQueryMutation, CreateQueryMutationVariables>,
) {
    return Apollo.useMutation<CreateQueryMutation, CreateQueryMutationVariables>(CreateQueryDocument, baseOptions);
}
export type CreateQueryMutationHookResult = ReturnType<typeof useCreateQueryMutation>;
export type CreateQueryMutationResult = Apollo.MutationResult<CreateQueryMutation>;
export type CreateQueryMutationOptions = Apollo.BaseMutationOptions<CreateQueryMutation, CreateQueryMutationVariables>;
export const UpdateQueryDocument = gql`
    mutation updateQuery($urn: String!, $input: UpdateQueryInput!) {
        updateQuery(urn: $urn, input: $input) {
            ...query
        }
    }
    ${QueryFragmentDoc}
`;
export type UpdateQueryMutationFn = Apollo.MutationFunction<UpdateQueryMutation, UpdateQueryMutationVariables>;

/**
 * __useUpdateQueryMutation__
 *
 * To run a mutation, you first call `useUpdateQueryMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateQueryMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateQueryMutation, { data, loading, error }] = useUpdateQueryMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateQueryMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateQueryMutation, UpdateQueryMutationVariables>,
) {
    return Apollo.useMutation<UpdateQueryMutation, UpdateQueryMutationVariables>(UpdateQueryDocument, baseOptions);
}
export type UpdateQueryMutationHookResult = ReturnType<typeof useUpdateQueryMutation>;
export type UpdateQueryMutationResult = Apollo.MutationResult<UpdateQueryMutation>;
export type UpdateQueryMutationOptions = Apollo.BaseMutationOptions<UpdateQueryMutation, UpdateQueryMutationVariables>;
export const DeleteQueryDocument = gql`
    mutation deleteQuery($urn: String!) {
        deleteQuery(urn: $urn)
    }
`;
export type DeleteQueryMutationFn = Apollo.MutationFunction<DeleteQueryMutation, DeleteQueryMutationVariables>;

/**
 * __useDeleteQueryMutation__
 *
 * To run a mutation, you first call `useDeleteQueryMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeleteQueryMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deleteQueryMutation, { data, loading, error }] = useDeleteQueryMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeleteQueryMutation(
    baseOptions?: Apollo.MutationHookOptions<DeleteQueryMutation, DeleteQueryMutationVariables>,
) {
    return Apollo.useMutation<DeleteQueryMutation, DeleteQueryMutationVariables>(DeleteQueryDocument, baseOptions);
}
export type DeleteQueryMutationHookResult = ReturnType<typeof useDeleteQueryMutation>;
export type DeleteQueryMutationResult = Apollo.MutationResult<DeleteQueryMutation>;
export type DeleteQueryMutationOptions = Apollo.BaseMutationOptions<DeleteQueryMutation, DeleteQueryMutationVariables>;
