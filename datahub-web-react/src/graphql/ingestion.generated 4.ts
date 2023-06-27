/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type ListIngestionSourcesQueryVariables = Types.Exact<{
    input: Types.ListIngestionSourcesInput;
}>;

export type ListIngestionSourcesQuery = { __typename?: 'Query' } & {
    listIngestionSources?: Types.Maybe<
        { __typename?: 'ListIngestionSourcesResult' } & Pick<
            Types.ListIngestionSourcesResult,
            'start' | 'count' | 'total'
        > & {
                ingestionSources: Array<
                    { __typename?: 'IngestionSource' } & Pick<Types.IngestionSource, 'urn' | 'name' | 'type'> & {
                            config: { __typename?: 'IngestionConfig' } & Pick<
                                Types.IngestionConfig,
                                'recipe' | 'version' | 'executorId' | 'debugMode'
                            >;
                            schedule?: Types.Maybe<
                                { __typename?: 'IngestionSchedule' } & Pick<
                                    Types.IngestionSchedule,
                                    'interval' | 'timezone'
                                >
                            >;
                            platform?: Types.Maybe<{ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn'>>;
                            executions?: Types.Maybe<
                                { __typename?: 'IngestionSourceExecutionRequests' } & Pick<
                                    Types.IngestionSourceExecutionRequests,
                                    'start' | 'count' | 'total'
                                > & {
                                        executionRequests: Array<
                                            { __typename?: 'ExecutionRequest' } & Pick<
                                                Types.ExecutionRequest,
                                                'urn' | 'id'
                                            > & {
                                                    input: { __typename?: 'ExecutionRequestInput' } & Pick<
                                                        Types.ExecutionRequestInput,
                                                        'requestedAt'
                                                    >;
                                                    result?: Types.Maybe<
                                                        { __typename?: 'ExecutionRequestResult' } & Pick<
                                                            Types.ExecutionRequestResult,
                                                            'status' | 'startTimeMs' | 'durationMs'
                                                        >
                                                    >;
                                                }
                                        >;
                                    }
                            >;
                        }
                >;
            }
    >;
};

export type GetIngestionSourceQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    runStart?: Types.Maybe<Types.Scalars['Int']>;
    runCount?: Types.Maybe<Types.Scalars['Int']>;
}>;

export type GetIngestionSourceQuery = { __typename?: 'Query' } & {
    ingestionSource?: Types.Maybe<
        { __typename?: 'IngestionSource' } & Pick<Types.IngestionSource, 'urn' | 'name' | 'type'> & {
                config: { __typename?: 'IngestionConfig' } & Pick<
                    Types.IngestionConfig,
                    'recipe' | 'version' | 'executorId' | 'debugMode'
                >;
                schedule?: Types.Maybe<
                    { __typename?: 'IngestionSchedule' } & Pick<Types.IngestionSchedule, 'interval' | 'timezone'>
                >;
                platform?: Types.Maybe<{ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn'>>;
                executions?: Types.Maybe<
                    { __typename?: 'IngestionSourceExecutionRequests' } & Pick<
                        Types.IngestionSourceExecutionRequests,
                        'start' | 'count' | 'total'
                    > & {
                            executionRequests: Array<
                                { __typename?: 'ExecutionRequest' } & Pick<Types.ExecutionRequest, 'urn' | 'id'> & {
                                        input: { __typename?: 'ExecutionRequestInput' } & Pick<
                                            Types.ExecutionRequestInput,
                                            'requestedAt'
                                        > & {
                                                source: { __typename?: 'ExecutionRequestSource' } & Pick<
                                                    Types.ExecutionRequestSource,
                                                    'type'
                                                >;
                                            };
                                        result?: Types.Maybe<
                                            { __typename?: 'ExecutionRequestResult' } & Pick<
                                                Types.ExecutionRequestResult,
                                                'status' | 'startTimeMs' | 'durationMs'
                                            >
                                        >;
                                    }
                            >;
                        }
                >;
            }
    >;
};

export type GetIngestionExecutionRequestQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetIngestionExecutionRequestQuery = { __typename?: 'Query' } & {
    executionRequest?: Types.Maybe<
        { __typename?: 'ExecutionRequest' } & Pick<Types.ExecutionRequest, 'urn' | 'id'> & {
                input: { __typename?: 'ExecutionRequestInput' } & {
                    source: { __typename?: 'ExecutionRequestSource' } & Pick<Types.ExecutionRequestSource, 'type'>;
                };
                result?: Types.Maybe<
                    { __typename?: 'ExecutionRequestResult' } & Pick<
                        Types.ExecutionRequestResult,
                        'status' | 'startTimeMs' | 'durationMs' | 'report'
                    > & {
                            structuredReport?: Types.Maybe<
                                { __typename?: 'StructuredReport' } & Pick<
                                    Types.StructuredReport,
                                    'type' | 'serializedValue'
                                >
                            >;
                        }
                >;
            }
    >;
};

export type CreateIngestionSourceMutationVariables = Types.Exact<{
    input: Types.UpdateIngestionSourceInput;
}>;

export type CreateIngestionSourceMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'createIngestionSource'>;

export type UpdateIngestionSourceMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.UpdateIngestionSourceInput;
}>;

export type UpdateIngestionSourceMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updateIngestionSource'>;

export type DeleteIngestionSourceMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeleteIngestionSourceMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deleteIngestionSource'>;

export type ListSecretsQueryVariables = Types.Exact<{
    input: Types.ListSecretsInput;
}>;

export type ListSecretsQuery = { __typename?: 'Query' } & {
    listSecrets?: Types.Maybe<
        { __typename?: 'ListSecretsResult' } & Pick<Types.ListSecretsResult, 'start' | 'count' | 'total'> & {
                secrets: Array<{ __typename?: 'Secret' } & Pick<Types.Secret, 'urn' | 'name' | 'description'>>;
            }
    >;
};

export type CreateSecretMutationVariables = Types.Exact<{
    input: Types.CreateSecretInput;
}>;

export type CreateSecretMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'createSecret'>;

export type DeleteSecretMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeleteSecretMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deleteSecret'>;

export type CreateIngestionExecutionRequestMutationVariables = Types.Exact<{
    input: Types.CreateIngestionExecutionRequestInput;
}>;

export type CreateIngestionExecutionRequestMutation = { __typename?: 'Mutation' } & Pick<
    Types.Mutation,
    'createIngestionExecutionRequest'
>;

export type CancelIngestionExecutionRequestMutationVariables = Types.Exact<{
    input: Types.CancelIngestionExecutionRequestInput;
}>;

export type CancelIngestionExecutionRequestMutation = { __typename?: 'Mutation' } & Pick<
    Types.Mutation,
    'cancelIngestionExecutionRequest'
>;

export type CreateTestConnectionRequestMutationVariables = Types.Exact<{
    input: Types.CreateTestConnectionRequestInput;
}>;

export type CreateTestConnectionRequestMutation = { __typename?: 'Mutation' } & Pick<
    Types.Mutation,
    'createTestConnectionRequest'
>;

export type RollbackIngestionMutationVariables = Types.Exact<{
    input: Types.RollbackIngestionInput;
}>;

export type RollbackIngestionMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'rollbackIngestion'>;

export const ListIngestionSourcesDocument = gql`
    query listIngestionSources($input: ListIngestionSourcesInput!) {
        listIngestionSources(input: $input) {
            start
            count
            total
            ingestionSources {
                urn
                name
                type
                config {
                    recipe
                    version
                    executorId
                    debugMode
                }
                schedule {
                    interval
                    timezone
                }
                platform {
                    urn
                }
                executions(start: 0, count: 1) {
                    start
                    count
                    total
                    executionRequests {
                        urn
                        id
                        input {
                            requestedAt
                        }
                        result {
                            status
                            startTimeMs
                            durationMs
                        }
                    }
                }
            }
        }
    }
`;

/**
 * __useListIngestionSourcesQuery__
 *
 * To run a query within a React component, call `useListIngestionSourcesQuery` and pass it any options that fit your needs.
 * When your component renders, `useListIngestionSourcesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListIngestionSourcesQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useListIngestionSourcesQuery(
    baseOptions: Apollo.QueryHookOptions<ListIngestionSourcesQuery, ListIngestionSourcesQueryVariables>,
) {
    return Apollo.useQuery<ListIngestionSourcesQuery, ListIngestionSourcesQueryVariables>(
        ListIngestionSourcesDocument,
        baseOptions,
    );
}
export function useListIngestionSourcesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListIngestionSourcesQuery, ListIngestionSourcesQueryVariables>,
) {
    return Apollo.useLazyQuery<ListIngestionSourcesQuery, ListIngestionSourcesQueryVariables>(
        ListIngestionSourcesDocument,
        baseOptions,
    );
}
export type ListIngestionSourcesQueryHookResult = ReturnType<typeof useListIngestionSourcesQuery>;
export type ListIngestionSourcesLazyQueryHookResult = ReturnType<typeof useListIngestionSourcesLazyQuery>;
export type ListIngestionSourcesQueryResult = Apollo.QueryResult<
    ListIngestionSourcesQuery,
    ListIngestionSourcesQueryVariables
>;
export const GetIngestionSourceDocument = gql`
    query getIngestionSource($urn: String!, $runStart: Int, $runCount: Int) {
        ingestionSource(urn: $urn) {
            urn
            name
            type
            config {
                recipe
                version
                executorId
                debugMode
            }
            schedule {
                interval
                timezone
            }
            platform {
                urn
            }
            executions(start: $runStart, count: $runCount) {
                start
                count
                total
                executionRequests {
                    urn
                    id
                    input {
                        requestedAt
                        source {
                            type
                        }
                    }
                    result {
                        status
                        startTimeMs
                        durationMs
                    }
                }
            }
        }
    }
`;

/**
 * __useGetIngestionSourceQuery__
 *
 * To run a query within a React component, call `useGetIngestionSourceQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetIngestionSourceQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetIngestionSourceQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      runStart: // value for 'runStart'
 *      runCount: // value for 'runCount'
 *   },
 * });
 */
export function useGetIngestionSourceQuery(
    baseOptions: Apollo.QueryHookOptions<GetIngestionSourceQuery, GetIngestionSourceQueryVariables>,
) {
    return Apollo.useQuery<GetIngestionSourceQuery, GetIngestionSourceQueryVariables>(
        GetIngestionSourceDocument,
        baseOptions,
    );
}
export function useGetIngestionSourceLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetIngestionSourceQuery, GetIngestionSourceQueryVariables>,
) {
    return Apollo.useLazyQuery<GetIngestionSourceQuery, GetIngestionSourceQueryVariables>(
        GetIngestionSourceDocument,
        baseOptions,
    );
}
export type GetIngestionSourceQueryHookResult = ReturnType<typeof useGetIngestionSourceQuery>;
export type GetIngestionSourceLazyQueryHookResult = ReturnType<typeof useGetIngestionSourceLazyQuery>;
export type GetIngestionSourceQueryResult = Apollo.QueryResult<
    GetIngestionSourceQuery,
    GetIngestionSourceQueryVariables
>;
export const GetIngestionExecutionRequestDocument = gql`
    query getIngestionExecutionRequest($urn: String!) {
        executionRequest(urn: $urn) {
            urn
            id
            input {
                source {
                    type
                }
            }
            result {
                status
                startTimeMs
                durationMs
                report
                structuredReport {
                    type
                    serializedValue
                }
            }
        }
    }
`;

/**
 * __useGetIngestionExecutionRequestQuery__
 *
 * To run a query within a React component, call `useGetIngestionExecutionRequestQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetIngestionExecutionRequestQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetIngestionExecutionRequestQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetIngestionExecutionRequestQuery(
    baseOptions: Apollo.QueryHookOptions<GetIngestionExecutionRequestQuery, GetIngestionExecutionRequestQueryVariables>,
) {
    return Apollo.useQuery<GetIngestionExecutionRequestQuery, GetIngestionExecutionRequestQueryVariables>(
        GetIngestionExecutionRequestDocument,
        baseOptions,
    );
}
export function useGetIngestionExecutionRequestLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<
        GetIngestionExecutionRequestQuery,
        GetIngestionExecutionRequestQueryVariables
    >,
) {
    return Apollo.useLazyQuery<GetIngestionExecutionRequestQuery, GetIngestionExecutionRequestQueryVariables>(
        GetIngestionExecutionRequestDocument,
        baseOptions,
    );
}
export type GetIngestionExecutionRequestQueryHookResult = ReturnType<typeof useGetIngestionExecutionRequestQuery>;
export type GetIngestionExecutionRequestLazyQueryHookResult = ReturnType<
    typeof useGetIngestionExecutionRequestLazyQuery
>;
export type GetIngestionExecutionRequestQueryResult = Apollo.QueryResult<
    GetIngestionExecutionRequestQuery,
    GetIngestionExecutionRequestQueryVariables
>;
export const CreateIngestionSourceDocument = gql`
    mutation createIngestionSource($input: UpdateIngestionSourceInput!) {
        createIngestionSource(input: $input)
    }
`;
export type CreateIngestionSourceMutationFn = Apollo.MutationFunction<
    CreateIngestionSourceMutation,
    CreateIngestionSourceMutationVariables
>;

/**
 * __useCreateIngestionSourceMutation__
 *
 * To run a mutation, you first call `useCreateIngestionSourceMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateIngestionSourceMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createIngestionSourceMutation, { data, loading, error }] = useCreateIngestionSourceMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateIngestionSourceMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateIngestionSourceMutation, CreateIngestionSourceMutationVariables>,
) {
    return Apollo.useMutation<CreateIngestionSourceMutation, CreateIngestionSourceMutationVariables>(
        CreateIngestionSourceDocument,
        baseOptions,
    );
}
export type CreateIngestionSourceMutationHookResult = ReturnType<typeof useCreateIngestionSourceMutation>;
export type CreateIngestionSourceMutationResult = Apollo.MutationResult<CreateIngestionSourceMutation>;
export type CreateIngestionSourceMutationOptions = Apollo.BaseMutationOptions<
    CreateIngestionSourceMutation,
    CreateIngestionSourceMutationVariables
>;
export const UpdateIngestionSourceDocument = gql`
    mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
        updateIngestionSource(urn: $urn, input: $input)
    }
`;
export type UpdateIngestionSourceMutationFn = Apollo.MutationFunction<
    UpdateIngestionSourceMutation,
    UpdateIngestionSourceMutationVariables
>;

/**
 * __useUpdateIngestionSourceMutation__
 *
 * To run a mutation, you first call `useUpdateIngestionSourceMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateIngestionSourceMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateIngestionSourceMutation, { data, loading, error }] = useUpdateIngestionSourceMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateIngestionSourceMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateIngestionSourceMutation, UpdateIngestionSourceMutationVariables>,
) {
    return Apollo.useMutation<UpdateIngestionSourceMutation, UpdateIngestionSourceMutationVariables>(
        UpdateIngestionSourceDocument,
        baseOptions,
    );
}
export type UpdateIngestionSourceMutationHookResult = ReturnType<typeof useUpdateIngestionSourceMutation>;
export type UpdateIngestionSourceMutationResult = Apollo.MutationResult<UpdateIngestionSourceMutation>;
export type UpdateIngestionSourceMutationOptions = Apollo.BaseMutationOptions<
    UpdateIngestionSourceMutation,
    UpdateIngestionSourceMutationVariables
>;
export const DeleteIngestionSourceDocument = gql`
    mutation deleteIngestionSource($urn: String!) {
        deleteIngestionSource(urn: $urn)
    }
`;
export type DeleteIngestionSourceMutationFn = Apollo.MutationFunction<
    DeleteIngestionSourceMutation,
    DeleteIngestionSourceMutationVariables
>;

/**
 * __useDeleteIngestionSourceMutation__
 *
 * To run a mutation, you first call `useDeleteIngestionSourceMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeleteIngestionSourceMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deleteIngestionSourceMutation, { data, loading, error }] = useDeleteIngestionSourceMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeleteIngestionSourceMutation(
    baseOptions?: Apollo.MutationHookOptions<DeleteIngestionSourceMutation, DeleteIngestionSourceMutationVariables>,
) {
    return Apollo.useMutation<DeleteIngestionSourceMutation, DeleteIngestionSourceMutationVariables>(
        DeleteIngestionSourceDocument,
        baseOptions,
    );
}
export type DeleteIngestionSourceMutationHookResult = ReturnType<typeof useDeleteIngestionSourceMutation>;
export type DeleteIngestionSourceMutationResult = Apollo.MutationResult<DeleteIngestionSourceMutation>;
export type DeleteIngestionSourceMutationOptions = Apollo.BaseMutationOptions<
    DeleteIngestionSourceMutation,
    DeleteIngestionSourceMutationVariables
>;
export const ListSecretsDocument = gql`
    query listSecrets($input: ListSecretsInput!) {
        listSecrets(input: $input) {
            start
            count
            total
            secrets {
                urn
                name
                description
            }
        }
    }
`;

/**
 * __useListSecretsQuery__
 *
 * To run a query within a React component, call `useListSecretsQuery` and pass it any options that fit your needs.
 * When your component renders, `useListSecretsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListSecretsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useListSecretsQuery(baseOptions: Apollo.QueryHookOptions<ListSecretsQuery, ListSecretsQueryVariables>) {
    return Apollo.useQuery<ListSecretsQuery, ListSecretsQueryVariables>(ListSecretsDocument, baseOptions);
}
export function useListSecretsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListSecretsQuery, ListSecretsQueryVariables>,
) {
    return Apollo.useLazyQuery<ListSecretsQuery, ListSecretsQueryVariables>(ListSecretsDocument, baseOptions);
}
export type ListSecretsQueryHookResult = ReturnType<typeof useListSecretsQuery>;
export type ListSecretsLazyQueryHookResult = ReturnType<typeof useListSecretsLazyQuery>;
export type ListSecretsQueryResult = Apollo.QueryResult<ListSecretsQuery, ListSecretsQueryVariables>;
export const CreateSecretDocument = gql`
    mutation createSecret($input: CreateSecretInput!) {
        createSecret(input: $input)
    }
`;
export type CreateSecretMutationFn = Apollo.MutationFunction<CreateSecretMutation, CreateSecretMutationVariables>;

/**
 * __useCreateSecretMutation__
 *
 * To run a mutation, you first call `useCreateSecretMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateSecretMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createSecretMutation, { data, loading, error }] = useCreateSecretMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateSecretMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateSecretMutation, CreateSecretMutationVariables>,
) {
    return Apollo.useMutation<CreateSecretMutation, CreateSecretMutationVariables>(CreateSecretDocument, baseOptions);
}
export type CreateSecretMutationHookResult = ReturnType<typeof useCreateSecretMutation>;
export type CreateSecretMutationResult = Apollo.MutationResult<CreateSecretMutation>;
export type CreateSecretMutationOptions = Apollo.BaseMutationOptions<
    CreateSecretMutation,
    CreateSecretMutationVariables
>;
export const DeleteSecretDocument = gql`
    mutation deleteSecret($urn: String!) {
        deleteSecret(urn: $urn)
    }
`;
export type DeleteSecretMutationFn = Apollo.MutationFunction<DeleteSecretMutation, DeleteSecretMutationVariables>;

/**
 * __useDeleteSecretMutation__
 *
 * To run a mutation, you first call `useDeleteSecretMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeleteSecretMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deleteSecretMutation, { data, loading, error }] = useDeleteSecretMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeleteSecretMutation(
    baseOptions?: Apollo.MutationHookOptions<DeleteSecretMutation, DeleteSecretMutationVariables>,
) {
    return Apollo.useMutation<DeleteSecretMutation, DeleteSecretMutationVariables>(DeleteSecretDocument, baseOptions);
}
export type DeleteSecretMutationHookResult = ReturnType<typeof useDeleteSecretMutation>;
export type DeleteSecretMutationResult = Apollo.MutationResult<DeleteSecretMutation>;
export type DeleteSecretMutationOptions = Apollo.BaseMutationOptions<
    DeleteSecretMutation,
    DeleteSecretMutationVariables
>;
export const CreateIngestionExecutionRequestDocument = gql`
    mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
        createIngestionExecutionRequest(input: $input)
    }
`;
export type CreateIngestionExecutionRequestMutationFn = Apollo.MutationFunction<
    CreateIngestionExecutionRequestMutation,
    CreateIngestionExecutionRequestMutationVariables
>;

/**
 * __useCreateIngestionExecutionRequestMutation__
 *
 * To run a mutation, you first call `useCreateIngestionExecutionRequestMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateIngestionExecutionRequestMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createIngestionExecutionRequestMutation, { data, loading, error }] = useCreateIngestionExecutionRequestMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateIngestionExecutionRequestMutation(
    baseOptions?: Apollo.MutationHookOptions<
        CreateIngestionExecutionRequestMutation,
        CreateIngestionExecutionRequestMutationVariables
    >,
) {
    return Apollo.useMutation<
        CreateIngestionExecutionRequestMutation,
        CreateIngestionExecutionRequestMutationVariables
    >(CreateIngestionExecutionRequestDocument, baseOptions);
}
export type CreateIngestionExecutionRequestMutationHookResult = ReturnType<
    typeof useCreateIngestionExecutionRequestMutation
>;
export type CreateIngestionExecutionRequestMutationResult =
    Apollo.MutationResult<CreateIngestionExecutionRequestMutation>;
export type CreateIngestionExecutionRequestMutationOptions = Apollo.BaseMutationOptions<
    CreateIngestionExecutionRequestMutation,
    CreateIngestionExecutionRequestMutationVariables
>;
export const CancelIngestionExecutionRequestDocument = gql`
    mutation cancelIngestionExecutionRequest($input: CancelIngestionExecutionRequestInput!) {
        cancelIngestionExecutionRequest(input: $input)
    }
`;
export type CancelIngestionExecutionRequestMutationFn = Apollo.MutationFunction<
    CancelIngestionExecutionRequestMutation,
    CancelIngestionExecutionRequestMutationVariables
>;

/**
 * __useCancelIngestionExecutionRequestMutation__
 *
 * To run a mutation, you first call `useCancelIngestionExecutionRequestMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCancelIngestionExecutionRequestMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [cancelIngestionExecutionRequestMutation, { data, loading, error }] = useCancelIngestionExecutionRequestMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCancelIngestionExecutionRequestMutation(
    baseOptions?: Apollo.MutationHookOptions<
        CancelIngestionExecutionRequestMutation,
        CancelIngestionExecutionRequestMutationVariables
    >,
) {
    return Apollo.useMutation<
        CancelIngestionExecutionRequestMutation,
        CancelIngestionExecutionRequestMutationVariables
    >(CancelIngestionExecutionRequestDocument, baseOptions);
}
export type CancelIngestionExecutionRequestMutationHookResult = ReturnType<
    typeof useCancelIngestionExecutionRequestMutation
>;
export type CancelIngestionExecutionRequestMutationResult =
    Apollo.MutationResult<CancelIngestionExecutionRequestMutation>;
export type CancelIngestionExecutionRequestMutationOptions = Apollo.BaseMutationOptions<
    CancelIngestionExecutionRequestMutation,
    CancelIngestionExecutionRequestMutationVariables
>;
export const CreateTestConnectionRequestDocument = gql`
    mutation createTestConnectionRequest($input: CreateTestConnectionRequestInput!) {
        createTestConnectionRequest(input: $input)
    }
`;
export type CreateTestConnectionRequestMutationFn = Apollo.MutationFunction<
    CreateTestConnectionRequestMutation,
    CreateTestConnectionRequestMutationVariables
>;

/**
 * __useCreateTestConnectionRequestMutation__
 *
 * To run a mutation, you first call `useCreateTestConnectionRequestMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateTestConnectionRequestMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createTestConnectionRequestMutation, { data, loading, error }] = useCreateTestConnectionRequestMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateTestConnectionRequestMutation(
    baseOptions?: Apollo.MutationHookOptions<
        CreateTestConnectionRequestMutation,
        CreateTestConnectionRequestMutationVariables
    >,
) {
    return Apollo.useMutation<CreateTestConnectionRequestMutation, CreateTestConnectionRequestMutationVariables>(
        CreateTestConnectionRequestDocument,
        baseOptions,
    );
}
export type CreateTestConnectionRequestMutationHookResult = ReturnType<typeof useCreateTestConnectionRequestMutation>;
export type CreateTestConnectionRequestMutationResult = Apollo.MutationResult<CreateTestConnectionRequestMutation>;
export type CreateTestConnectionRequestMutationOptions = Apollo.BaseMutationOptions<
    CreateTestConnectionRequestMutation,
    CreateTestConnectionRequestMutationVariables
>;
export const RollbackIngestionDocument = gql`
    mutation rollbackIngestion($input: RollbackIngestionInput!) {
        rollbackIngestion(input: $input)
    }
`;
export type RollbackIngestionMutationFn = Apollo.MutationFunction<
    RollbackIngestionMutation,
    RollbackIngestionMutationVariables
>;

/**
 * __useRollbackIngestionMutation__
 *
 * To run a mutation, you first call `useRollbackIngestionMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useRollbackIngestionMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [rollbackIngestionMutation, { data, loading, error }] = useRollbackIngestionMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useRollbackIngestionMutation(
    baseOptions?: Apollo.MutationHookOptions<RollbackIngestionMutation, RollbackIngestionMutationVariables>,
) {
    return Apollo.useMutation<RollbackIngestionMutation, RollbackIngestionMutationVariables>(
        RollbackIngestionDocument,
        baseOptions,
    );
}
export type RollbackIngestionMutationHookResult = ReturnType<typeof useRollbackIngestionMutation>;
export type RollbackIngestionMutationResult = Apollo.MutationResult<RollbackIngestionMutation>;
export type RollbackIngestionMutationOptions = Apollo.BaseMutationOptions<
    RollbackIngestionMutation,
    RollbackIngestionMutationVariables
>;
