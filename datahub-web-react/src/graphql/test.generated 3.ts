/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type TestFieldsFragment = { __typename?: 'Test' } & Pick<
    Types.Test,
    'urn' | 'name' | 'category' | 'description'
> & { definition: { __typename?: 'TestDefinition' } & Pick<Types.TestDefinition, 'json'> };

export type GetTestQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetTestQuery = { __typename?: 'Query' } & {
    test?: Types.Maybe<{ __typename?: 'Test' } & TestFieldsFragment>;
};

export type ListTestsQueryVariables = Types.Exact<{
    input: Types.ListTestsInput;
}>;

export type ListTestsQuery = { __typename?: 'Query' } & {
    listTests?: Types.Maybe<
        { __typename?: 'ListTestsResult' } & Pick<Types.ListTestsResult, 'start' | 'count' | 'total'> & {
                tests: Array<{ __typename?: 'Test' } & TestFieldsFragment>;
            }
    >;
};

export type CreateTestMutationVariables = Types.Exact<{
    input: Types.CreateTestInput;
}>;

export type CreateTestMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'createTest'>;

export type DeleteTestMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeleteTestMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deleteTest'>;

export const TestFieldsFragmentDoc = gql`
    fragment testFields on Test {
        urn
        name
        category
        description
        definition {
            json
        }
    }
`;
export const GetTestDocument = gql`
    query getTest($urn: String!) {
        test(urn: $urn) {
            ...testFields
        }
    }
    ${TestFieldsFragmentDoc}
`;

/**
 * __useGetTestQuery__
 *
 * To run a query within a React component, call `useGetTestQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetTestQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetTestQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetTestQuery(baseOptions: Apollo.QueryHookOptions<GetTestQuery, GetTestQueryVariables>) {
    return Apollo.useQuery<GetTestQuery, GetTestQueryVariables>(GetTestDocument, baseOptions);
}
export function useGetTestLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetTestQuery, GetTestQueryVariables>) {
    return Apollo.useLazyQuery<GetTestQuery, GetTestQueryVariables>(GetTestDocument, baseOptions);
}
export type GetTestQueryHookResult = ReturnType<typeof useGetTestQuery>;
export type GetTestLazyQueryHookResult = ReturnType<typeof useGetTestLazyQuery>;
export type GetTestQueryResult = Apollo.QueryResult<GetTestQuery, GetTestQueryVariables>;
export const ListTestsDocument = gql`
    query listTests($input: ListTestsInput!) {
        listTests(input: $input) {
            start
            count
            total
            tests {
                ...testFields
            }
        }
    }
    ${TestFieldsFragmentDoc}
`;

/**
 * __useListTestsQuery__
 *
 * To run a query within a React component, call `useListTestsQuery` and pass it any options that fit your needs.
 * When your component renders, `useListTestsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListTestsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useListTestsQuery(baseOptions: Apollo.QueryHookOptions<ListTestsQuery, ListTestsQueryVariables>) {
    return Apollo.useQuery<ListTestsQuery, ListTestsQueryVariables>(ListTestsDocument, baseOptions);
}
export function useListTestsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListTestsQuery, ListTestsQueryVariables>,
) {
    return Apollo.useLazyQuery<ListTestsQuery, ListTestsQueryVariables>(ListTestsDocument, baseOptions);
}
export type ListTestsQueryHookResult = ReturnType<typeof useListTestsQuery>;
export type ListTestsLazyQueryHookResult = ReturnType<typeof useListTestsLazyQuery>;
export type ListTestsQueryResult = Apollo.QueryResult<ListTestsQuery, ListTestsQueryVariables>;
export const CreateTestDocument = gql`
    mutation createTest($input: CreateTestInput!) {
        createTest(input: $input)
    }
`;
export type CreateTestMutationFn = Apollo.MutationFunction<CreateTestMutation, CreateTestMutationVariables>;

/**
 * __useCreateTestMutation__
 *
 * To run a mutation, you first call `useCreateTestMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateTestMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createTestMutation, { data, loading, error }] = useCreateTestMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateTestMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateTestMutation, CreateTestMutationVariables>,
) {
    return Apollo.useMutation<CreateTestMutation, CreateTestMutationVariables>(CreateTestDocument, baseOptions);
}
export type CreateTestMutationHookResult = ReturnType<typeof useCreateTestMutation>;
export type CreateTestMutationResult = Apollo.MutationResult<CreateTestMutation>;
export type CreateTestMutationOptions = Apollo.BaseMutationOptions<CreateTestMutation, CreateTestMutationVariables>;
export const DeleteTestDocument = gql`
    mutation deleteTest($urn: String!) {
        deleteTest(urn: $urn)
    }
`;
export type DeleteTestMutationFn = Apollo.MutationFunction<DeleteTestMutation, DeleteTestMutationVariables>;

/**
 * __useDeleteTestMutation__
 *
 * To run a mutation, you first call `useDeleteTestMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeleteTestMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deleteTestMutation, { data, loading, error }] = useDeleteTestMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeleteTestMutation(
    baseOptions?: Apollo.MutationHookOptions<DeleteTestMutation, DeleteTestMutationVariables>,
) {
    return Apollo.useMutation<DeleteTestMutation, DeleteTestMutationVariables>(DeleteTestDocument, baseOptions);
}
export type DeleteTestMutationHookResult = ReturnType<typeof useDeleteTestMutation>;
export type DeleteTestMutationResult = Apollo.MutationResult<DeleteTestMutation>;
export type DeleteTestMutationOptions = Apollo.BaseMutationOptions<DeleteTestMutation, DeleteTestMutationVariables>;
