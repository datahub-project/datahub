/* eslint-disable */
import * as Types from '../types.generated';

import { DataPlatformInstanceFieldsFragment } from './fragments.generated';
import { gql } from '@apollo/client';
import { DataPlatformInstanceFieldsFragmentDoc } from './fragments.generated';
import * as Apollo from '@apollo/client';
export type AssertionDetailsFragment = { __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'> & {
        platform: { __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn'> & {
                properties?: Types.Maybe<
                    { __typename?: 'DataPlatformProperties' } & Pick<
                        Types.DataPlatformProperties,
                        'displayName' | 'logoUrl'
                    >
                >;
                info?: Types.Maybe<
                    { __typename?: 'DataPlatformInfo' } & Pick<Types.DataPlatformInfo, 'displayName' | 'logoUrl'>
                >;
            };
        dataPlatformInstance?: Types.Maybe<
            { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
        >;
        info?: Types.Maybe<
            { __typename?: 'AssertionInfo' } & Pick<Types.AssertionInfo, 'type'> & {
                    datasetAssertion?: Types.Maybe<
                        { __typename?: 'DatasetAssertionInfo' } & Pick<
                            Types.DatasetAssertionInfo,
                            'scope' | 'aggregation' | 'operator' | 'nativeType' | 'logic'
                        > & {
                                parameters?: Types.Maybe<
                                    { __typename?: 'AssertionStdParameters' } & {
                                        value?: Types.Maybe<
                                            { __typename?: 'AssertionStdParameter' } & Pick<
                                                Types.AssertionStdParameter,
                                                'value' | 'type'
                                            >
                                        >;
                                        minValue?: Types.Maybe<
                                            { __typename?: 'AssertionStdParameter' } & Pick<
                                                Types.AssertionStdParameter,
                                                'value' | 'type'
                                            >
                                        >;
                                        maxValue?: Types.Maybe<
                                            { __typename?: 'AssertionStdParameter' } & Pick<
                                                Types.AssertionStdParameter,
                                                'value' | 'type'
                                            >
                                        >;
                                    }
                                >;
                                fields?: Types.Maybe<
                                    Array<
                                        { __typename?: 'SchemaFieldRef' } & Pick<Types.SchemaFieldRef, 'urn' | 'path'>
                                    >
                                >;
                                nativeParameters?: Types.Maybe<
                                    Array<
                                        { __typename?: 'StringMapEntry' } & Pick<Types.StringMapEntry, 'key' | 'value'>
                                    >
                                >;
                            }
                    >;
                }
        >;
    };

export type AssertionRunEventDetailsFragment = { __typename?: 'AssertionRunEvent' } & Pick<
    Types.AssertionRunEvent,
    'timestampMillis' | 'assertionUrn' | 'status'
> & {
        runtimeContext?: Types.Maybe<
            Array<{ __typename?: 'StringMapEntry' } & Pick<Types.StringMapEntry, 'key' | 'value'>>
        >;
        result?: Types.Maybe<
            { __typename?: 'AssertionResult' } & Pick<
                Types.AssertionResult,
                'type' | 'actualAggValue' | 'rowCount' | 'missingCount' | 'unexpectedCount' | 'externalUrl'
            > & {
                    nativeResults?: Types.Maybe<
                        Array<{ __typename?: 'StringMapEntry' } & Pick<Types.StringMapEntry, 'key' | 'value'>>
                    >;
                }
        >;
    };

export type GetAssertionRunsQueryVariables = Types.Exact<{
    assertionUrn: Types.Scalars['String'];
    startTime?: Types.Maybe<Types.Scalars['Long']>;
    endTime?: Types.Maybe<Types.Scalars['Long']>;
    limit?: Types.Maybe<Types.Scalars['Int']>;
}>;

export type GetAssertionRunsQuery = { __typename?: 'Query' } & {
    assertion?: Types.Maybe<
        { __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn'> & {
                runEvents?: Types.Maybe<
                    { __typename?: 'AssertionRunEventsResult' } & Pick<
                        Types.AssertionRunEventsResult,
                        'total' | 'failed' | 'succeeded'
                    > & { runEvents: Array<{ __typename?: 'AssertionRunEvent' } & AssertionRunEventDetailsFragment> }
                >;
            } & AssertionDetailsFragment
    >;
};

export type DeleteAssertionMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeleteAssertionMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deleteAssertion'>;

export const AssertionDetailsFragmentDoc = gql`
    fragment assertionDetails on Assertion {
        urn
        type
        platform {
            urn
            properties {
                displayName
                logoUrl
            }
            info {
                displayName
                logoUrl
            }
        }
        dataPlatformInstance {
            ...dataPlatformInstanceFields
        }
        info {
            type
            datasetAssertion {
                scope
                aggregation
                operator
                parameters {
                    value {
                        value
                        type
                    }
                    minValue {
                        value
                        type
                    }
                    maxValue {
                        value
                        type
                    }
                }
                fields {
                    urn
                    path
                }
                nativeType
                nativeParameters {
                    key
                    value
                }
                logic
            }
        }
    }
    ${DataPlatformInstanceFieldsFragmentDoc}
`;
export const AssertionRunEventDetailsFragmentDoc = gql`
    fragment assertionRunEventDetails on AssertionRunEvent {
        timestampMillis
        assertionUrn
        status
        runtimeContext {
            key
            value
        }
        result {
            type
            actualAggValue
            rowCount
            missingCount
            unexpectedCount
            externalUrl
            nativeResults {
                key
                value
            }
        }
    }
`;
export const GetAssertionRunsDocument = gql`
    query getAssertionRuns($assertionUrn: String!, $startTime: Long, $endTime: Long, $limit: Int) {
        assertion(urn: $assertionUrn) {
            urn
            ...assertionDetails
            runEvents(status: COMPLETE, startTimeMillis: $startTime, endTimeMillis: $endTime, limit: $limit) {
                total
                failed
                succeeded
                runEvents {
                    ...assertionRunEventDetails
                }
            }
        }
    }
    ${AssertionDetailsFragmentDoc}
    ${AssertionRunEventDetailsFragmentDoc}
`;

/**
 * __useGetAssertionRunsQuery__
 *
 * To run a query within a React component, call `useGetAssertionRunsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetAssertionRunsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetAssertionRunsQuery({
 *   variables: {
 *      assertionUrn: // value for 'assertionUrn'
 *      startTime: // value for 'startTime'
 *      endTime: // value for 'endTime'
 *      limit: // value for 'limit'
 *   },
 * });
 */
export function useGetAssertionRunsQuery(
    baseOptions: Apollo.QueryHookOptions<GetAssertionRunsQuery, GetAssertionRunsQueryVariables>,
) {
    return Apollo.useQuery<GetAssertionRunsQuery, GetAssertionRunsQueryVariables>(
        GetAssertionRunsDocument,
        baseOptions,
    );
}
export function useGetAssertionRunsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetAssertionRunsQuery, GetAssertionRunsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetAssertionRunsQuery, GetAssertionRunsQueryVariables>(
        GetAssertionRunsDocument,
        baseOptions,
    );
}
export type GetAssertionRunsQueryHookResult = ReturnType<typeof useGetAssertionRunsQuery>;
export type GetAssertionRunsLazyQueryHookResult = ReturnType<typeof useGetAssertionRunsLazyQuery>;
export type GetAssertionRunsQueryResult = Apollo.QueryResult<GetAssertionRunsQuery, GetAssertionRunsQueryVariables>;
export const DeleteAssertionDocument = gql`
    mutation deleteAssertion($urn: String!) {
        deleteAssertion(urn: $urn)
    }
`;
export type DeleteAssertionMutationFn = Apollo.MutationFunction<
    DeleteAssertionMutation,
    DeleteAssertionMutationVariables
>;

/**
 * __useDeleteAssertionMutation__
 *
 * To run a mutation, you first call `useDeleteAssertionMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeleteAssertionMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deleteAssertionMutation, { data, loading, error }] = useDeleteAssertionMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeleteAssertionMutation(
    baseOptions?: Apollo.MutationHookOptions<DeleteAssertionMutation, DeleteAssertionMutationVariables>,
) {
    return Apollo.useMutation<DeleteAssertionMutation, DeleteAssertionMutationVariables>(
        DeleteAssertionDocument,
        baseOptions,
    );
}
export type DeleteAssertionMutationHookResult = ReturnType<typeof useDeleteAssertionMutation>;
export type DeleteAssertionMutationResult = Apollo.MutationResult<DeleteAssertionMutation>;
export type DeleteAssertionMutationOptions = Apollo.BaseMutationOptions<
    DeleteAssertionMutation,
    DeleteAssertionMutationVariables
>;
