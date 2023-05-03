/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type BatchGetStepStatesQueryVariables = Types.Exact<{
    input: Types.BatchGetStepStatesInput;
}>;

export type BatchGetStepStatesQuery = { __typename?: 'Query' } & {
    batchGetStepStates: { __typename?: 'BatchGetStepStatesResult' } & {
        results: Array<
            { __typename?: 'StepStateResult' } & Pick<Types.StepStateResult, 'id'> & {
                    properties: Array<{ __typename?: 'StringMapEntry' } & Pick<Types.StringMapEntry, 'key' | 'value'>>;
                }
        >;
    };
};

export type BatchUpdateStepStatesMutationVariables = Types.Exact<{
    input: Types.BatchUpdateStepStatesInput;
}>;

export type BatchUpdateStepStatesMutation = { __typename?: 'Mutation' } & {
    batchUpdateStepStates: { __typename?: 'BatchUpdateStepStatesResult' } & {
        results: Array<
            { __typename?: 'UpdateStepStateResult' } & Pick<Types.UpdateStepStateResult, 'id' | 'succeeded'>
        >;
    };
};

export const BatchGetStepStatesDocument = gql`
    query batchGetStepStates($input: BatchGetStepStatesInput!) {
        batchGetStepStates(input: $input) {
            results {
                id
                properties {
                    key
                    value
                }
            }
        }
    }
`;

/**
 * __useBatchGetStepStatesQuery__
 *
 * To run a query within a React component, call `useBatchGetStepStatesQuery` and pass it any options that fit your needs.
 * When your component renders, `useBatchGetStepStatesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useBatchGetStepStatesQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchGetStepStatesQuery(
    baseOptions: Apollo.QueryHookOptions<BatchGetStepStatesQuery, BatchGetStepStatesQueryVariables>,
) {
    return Apollo.useQuery<BatchGetStepStatesQuery, BatchGetStepStatesQueryVariables>(
        BatchGetStepStatesDocument,
        baseOptions,
    );
}
export function useBatchGetStepStatesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<BatchGetStepStatesQuery, BatchGetStepStatesQueryVariables>,
) {
    return Apollo.useLazyQuery<BatchGetStepStatesQuery, BatchGetStepStatesQueryVariables>(
        BatchGetStepStatesDocument,
        baseOptions,
    );
}
export type BatchGetStepStatesQueryHookResult = ReturnType<typeof useBatchGetStepStatesQuery>;
export type BatchGetStepStatesLazyQueryHookResult = ReturnType<typeof useBatchGetStepStatesLazyQuery>;
export type BatchGetStepStatesQueryResult = Apollo.QueryResult<
    BatchGetStepStatesQuery,
    BatchGetStepStatesQueryVariables
>;
export const BatchUpdateStepStatesDocument = gql`
    mutation batchUpdateStepStates($input: BatchUpdateStepStatesInput!) {
        batchUpdateStepStates(input: $input) {
            results {
                id
                succeeded
            }
        }
    }
`;
export type BatchUpdateStepStatesMutationFn = Apollo.MutationFunction<
    BatchUpdateStepStatesMutation,
    BatchUpdateStepStatesMutationVariables
>;

/**
 * __useBatchUpdateStepStatesMutation__
 *
 * To run a mutation, you first call `useBatchUpdateStepStatesMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchUpdateStepStatesMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchUpdateStepStatesMutation, { data, loading, error }] = useBatchUpdateStepStatesMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchUpdateStepStatesMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchUpdateStepStatesMutation, BatchUpdateStepStatesMutationVariables>,
) {
    return Apollo.useMutation<BatchUpdateStepStatesMutation, BatchUpdateStepStatesMutationVariables>(
        BatchUpdateStepStatesDocument,
        baseOptions,
    );
}
export type BatchUpdateStepStatesMutationHookResult = ReturnType<typeof useBatchUpdateStepStatesMutation>;
export type BatchUpdateStepStatesMutationResult = Apollo.MutationResult<BatchUpdateStepStatesMutation>;
export type BatchUpdateStepStatesMutationOptions = Apollo.BaseMutationOptions<
    BatchUpdateStepStatesMutation,
    BatchUpdateStepStatesMutationVariables
>;
