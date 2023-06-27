/* eslint-disable */
import * as Types from '../types.generated';

import { DataJobFieldsFragment } from './fragments.generated';
import { RunResultsFragment } from './dataProcess.generated';
import { gql } from '@apollo/client';
import { DataJobFieldsFragmentDoc } from './fragments.generated';
import { RunResultsFragmentDoc } from './dataProcess.generated';
import * as Apollo from '@apollo/client';
export type GetDataJobQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetDataJobQuery = { __typename?: 'Query' } & {
    dataJob?: Types.Maybe<
        { __typename?: 'DataJob' } & {
            runs?: Types.Maybe<
                { __typename?: 'DataProcessInstanceResult' } & Pick<
                    Types.DataProcessInstanceResult,
                    'count' | 'start' | 'total'
                >
            >;
        } & DataJobFieldsFragment
    >;
};

export type UpdateDataJobMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.DataJobUpdateInput;
}>;

export type UpdateDataJobMutation = { __typename?: 'Mutation' } & {
    updateDataJob?: Types.Maybe<{ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn'>>;
};

export type GetDataJobRunsQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    start: Types.Scalars['Int'];
    count: Types.Scalars['Int'];
}>;

export type GetDataJobRunsQuery = { __typename?: 'Query' } & {
    dataJob?: Types.Maybe<
        { __typename?: 'DataJob' } & {
            runs?: Types.Maybe<{ __typename?: 'DataProcessInstanceResult' } & RunResultsFragment>;
        }
    >;
};

export const GetDataJobDocument = gql`
    query getDataJob($urn: String!) {
        dataJob(urn: $urn) {
            ...dataJobFields
            runs(start: 0, count: 20) {
                count
                start
                total
            }
        }
    }
    ${DataJobFieldsFragmentDoc}
`;

/**
 * __useGetDataJobQuery__
 *
 * To run a query within a React component, call `useGetDataJobQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDataJobQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDataJobQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetDataJobQuery(baseOptions: Apollo.QueryHookOptions<GetDataJobQuery, GetDataJobQueryVariables>) {
    return Apollo.useQuery<GetDataJobQuery, GetDataJobQueryVariables>(GetDataJobDocument, baseOptions);
}
export function useGetDataJobLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDataJobQuery, GetDataJobQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDataJobQuery, GetDataJobQueryVariables>(GetDataJobDocument, baseOptions);
}
export type GetDataJobQueryHookResult = ReturnType<typeof useGetDataJobQuery>;
export type GetDataJobLazyQueryHookResult = ReturnType<typeof useGetDataJobLazyQuery>;
export type GetDataJobQueryResult = Apollo.QueryResult<GetDataJobQuery, GetDataJobQueryVariables>;
export const UpdateDataJobDocument = gql`
    mutation updateDataJob($urn: String!, $input: DataJobUpdateInput!) {
        updateDataJob(urn: $urn, input: $input) {
            urn
        }
    }
`;
export type UpdateDataJobMutationFn = Apollo.MutationFunction<UpdateDataJobMutation, UpdateDataJobMutationVariables>;

/**
 * __useUpdateDataJobMutation__
 *
 * To run a mutation, you first call `useUpdateDataJobMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateDataJobMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateDataJobMutation, { data, loading, error }] = useUpdateDataJobMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateDataJobMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateDataJobMutation, UpdateDataJobMutationVariables>,
) {
    return Apollo.useMutation<UpdateDataJobMutation, UpdateDataJobMutationVariables>(
        UpdateDataJobDocument,
        baseOptions,
    );
}
export type UpdateDataJobMutationHookResult = ReturnType<typeof useUpdateDataJobMutation>;
export type UpdateDataJobMutationResult = Apollo.MutationResult<UpdateDataJobMutation>;
export type UpdateDataJobMutationOptions = Apollo.BaseMutationOptions<
    UpdateDataJobMutation,
    UpdateDataJobMutationVariables
>;
export const GetDataJobRunsDocument = gql`
    query getDataJobRuns($urn: String!, $start: Int!, $count: Int!) {
        dataJob(urn: $urn) {
            runs(start: $start, count: $count) {
                ...runResults
            }
        }
    }
    ${RunResultsFragmentDoc}
`;

/**
 * __useGetDataJobRunsQuery__
 *
 * To run a query within a React component, call `useGetDataJobRunsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDataJobRunsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDataJobRunsQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      start: // value for 'start'
 *      count: // value for 'count'
 *   },
 * });
 */
export function useGetDataJobRunsQuery(
    baseOptions: Apollo.QueryHookOptions<GetDataJobRunsQuery, GetDataJobRunsQueryVariables>,
) {
    return Apollo.useQuery<GetDataJobRunsQuery, GetDataJobRunsQueryVariables>(GetDataJobRunsDocument, baseOptions);
}
export function useGetDataJobRunsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDataJobRunsQuery, GetDataJobRunsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDataJobRunsQuery, GetDataJobRunsQueryVariables>(GetDataJobRunsDocument, baseOptions);
}
export type GetDataJobRunsQueryHookResult = ReturnType<typeof useGetDataJobRunsQuery>;
export type GetDataJobRunsLazyQueryHookResult = ReturnType<typeof useGetDataJobRunsLazyQuery>;
export type GetDataJobRunsQueryResult = Apollo.QueryResult<GetDataJobRunsQuery, GetDataJobRunsQueryVariables>;
