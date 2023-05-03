/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type ViewFragment = { __typename?: 'DataHubView' } & Pick<
    Types.DataHubView,
    'urn' | 'type' | 'viewType' | 'name' | 'description'
> & {
        definition: { __typename?: 'DataHubViewDefinition' } & Pick<Types.DataHubViewDefinition, 'entityTypes'> & {
                filter: { __typename?: 'DataHubViewFilter' } & Pick<Types.DataHubViewFilter, 'operator'> & {
                        filters: Array<
                            { __typename?: 'FacetFilter' } & Pick<
                                Types.FacetFilter,
                                'field' | 'condition' | 'values' | 'negated'
                            >
                        >;
                    };
            };
    };

export type ListViewResultsFragment = { __typename?: 'ListViewsResult' } & Pick<
    Types.ListViewsResult,
    'start' | 'count' | 'total'
> & { views: Array<{ __typename?: 'DataHubView' } & ViewFragment> };

export type ListMyViewsQueryVariables = Types.Exact<{
    viewType?: Types.Maybe<Types.DataHubViewType>;
    start: Types.Scalars['Int'];
    count: Types.Scalars['Int'];
    query?: Types.Maybe<Types.Scalars['String']>;
}>;

export type ListMyViewsQuery = { __typename?: 'Query' } & {
    listMyViews?: Types.Maybe<{ __typename?: 'ListViewsResult' } & ListViewResultsFragment>;
};

export type ListGlobalViewsQueryVariables = Types.Exact<{
    start: Types.Scalars['Int'];
    count: Types.Scalars['Int'];
    query?: Types.Maybe<Types.Scalars['String']>;
}>;

export type ListGlobalViewsQuery = { __typename?: 'Query' } & {
    listGlobalViews?: Types.Maybe<{ __typename?: 'ListViewsResult' } & ListViewResultsFragment>;
};

export type CreateViewMutationVariables = Types.Exact<{
    input: Types.CreateViewInput;
}>;

export type CreateViewMutation = { __typename?: 'Mutation' } & {
    createView?: Types.Maybe<{ __typename?: 'DataHubView' } & ViewFragment>;
};

export type UpdateViewMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.UpdateViewInput;
}>;

export type UpdateViewMutation = { __typename?: 'Mutation' } & {
    updateView?: Types.Maybe<{ __typename?: 'DataHubView' } & ViewFragment>;
};

export type DeleteViewMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeleteViewMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deleteView'>;

export const ViewFragmentDoc = gql`
    fragment view on DataHubView {
        urn
        type
        viewType
        name
        description
        definition {
            entityTypes
            filter {
                operator
                filters {
                    field
                    condition
                    values
                    negated
                }
            }
        }
    }
`;
export const ListViewResultsFragmentDoc = gql`
    fragment listViewResults on ListViewsResult {
        start
        count
        total
        views {
            ...view
        }
    }
    ${ViewFragmentDoc}
`;
export const ListMyViewsDocument = gql`
    query listMyViews($viewType: DataHubViewType, $start: Int!, $count: Int!, $query: String) {
        listMyViews(input: { viewType: $viewType, start: $start, count: $count, query: $query }) {
            ...listViewResults
        }
    }
    ${ListViewResultsFragmentDoc}
`;

/**
 * __useListMyViewsQuery__
 *
 * To run a query within a React component, call `useListMyViewsQuery` and pass it any options that fit your needs.
 * When your component renders, `useListMyViewsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListMyViewsQuery({
 *   variables: {
 *      viewType: // value for 'viewType'
 *      start: // value for 'start'
 *      count: // value for 'count'
 *      query: // value for 'query'
 *   },
 * });
 */
export function useListMyViewsQuery(baseOptions: Apollo.QueryHookOptions<ListMyViewsQuery, ListMyViewsQueryVariables>) {
    return Apollo.useQuery<ListMyViewsQuery, ListMyViewsQueryVariables>(ListMyViewsDocument, baseOptions);
}
export function useListMyViewsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListMyViewsQuery, ListMyViewsQueryVariables>,
) {
    return Apollo.useLazyQuery<ListMyViewsQuery, ListMyViewsQueryVariables>(ListMyViewsDocument, baseOptions);
}
export type ListMyViewsQueryHookResult = ReturnType<typeof useListMyViewsQuery>;
export type ListMyViewsLazyQueryHookResult = ReturnType<typeof useListMyViewsLazyQuery>;
export type ListMyViewsQueryResult = Apollo.QueryResult<ListMyViewsQuery, ListMyViewsQueryVariables>;
export const ListGlobalViewsDocument = gql`
    query listGlobalViews($start: Int!, $count: Int!, $query: String) {
        listGlobalViews(input: { start: $start, count: $count, query: $query }) {
            ...listViewResults
        }
    }
    ${ListViewResultsFragmentDoc}
`;

/**
 * __useListGlobalViewsQuery__
 *
 * To run a query within a React component, call `useListGlobalViewsQuery` and pass it any options that fit your needs.
 * When your component renders, `useListGlobalViewsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListGlobalViewsQuery({
 *   variables: {
 *      start: // value for 'start'
 *      count: // value for 'count'
 *      query: // value for 'query'
 *   },
 * });
 */
export function useListGlobalViewsQuery(
    baseOptions: Apollo.QueryHookOptions<ListGlobalViewsQuery, ListGlobalViewsQueryVariables>,
) {
    return Apollo.useQuery<ListGlobalViewsQuery, ListGlobalViewsQueryVariables>(ListGlobalViewsDocument, baseOptions);
}
export function useListGlobalViewsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListGlobalViewsQuery, ListGlobalViewsQueryVariables>,
) {
    return Apollo.useLazyQuery<ListGlobalViewsQuery, ListGlobalViewsQueryVariables>(
        ListGlobalViewsDocument,
        baseOptions,
    );
}
export type ListGlobalViewsQueryHookResult = ReturnType<typeof useListGlobalViewsQuery>;
export type ListGlobalViewsLazyQueryHookResult = ReturnType<typeof useListGlobalViewsLazyQuery>;
export type ListGlobalViewsQueryResult = Apollo.QueryResult<ListGlobalViewsQuery, ListGlobalViewsQueryVariables>;
export const CreateViewDocument = gql`
    mutation createView($input: CreateViewInput!) {
        createView(input: $input) {
            ...view
        }
    }
    ${ViewFragmentDoc}
`;
export type CreateViewMutationFn = Apollo.MutationFunction<CreateViewMutation, CreateViewMutationVariables>;

/**
 * __useCreateViewMutation__
 *
 * To run a mutation, you first call `useCreateViewMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateViewMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createViewMutation, { data, loading, error }] = useCreateViewMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateViewMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateViewMutation, CreateViewMutationVariables>,
) {
    return Apollo.useMutation<CreateViewMutation, CreateViewMutationVariables>(CreateViewDocument, baseOptions);
}
export type CreateViewMutationHookResult = ReturnType<typeof useCreateViewMutation>;
export type CreateViewMutationResult = Apollo.MutationResult<CreateViewMutation>;
export type CreateViewMutationOptions = Apollo.BaseMutationOptions<CreateViewMutation, CreateViewMutationVariables>;
export const UpdateViewDocument = gql`
    mutation updateView($urn: String!, $input: UpdateViewInput!) {
        updateView(urn: $urn, input: $input) {
            ...view
        }
    }
    ${ViewFragmentDoc}
`;
export type UpdateViewMutationFn = Apollo.MutationFunction<UpdateViewMutation, UpdateViewMutationVariables>;

/**
 * __useUpdateViewMutation__
 *
 * To run a mutation, you first call `useUpdateViewMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateViewMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateViewMutation, { data, loading, error }] = useUpdateViewMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateViewMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateViewMutation, UpdateViewMutationVariables>,
) {
    return Apollo.useMutation<UpdateViewMutation, UpdateViewMutationVariables>(UpdateViewDocument, baseOptions);
}
export type UpdateViewMutationHookResult = ReturnType<typeof useUpdateViewMutation>;
export type UpdateViewMutationResult = Apollo.MutationResult<UpdateViewMutation>;
export type UpdateViewMutationOptions = Apollo.BaseMutationOptions<UpdateViewMutation, UpdateViewMutationVariables>;
export const DeleteViewDocument = gql`
    mutation deleteView($urn: String!) {
        deleteView(urn: $urn)
    }
`;
export type DeleteViewMutationFn = Apollo.MutationFunction<DeleteViewMutation, DeleteViewMutationVariables>;

/**
 * __useDeleteViewMutation__
 *
 * To run a mutation, you first call `useDeleteViewMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeleteViewMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deleteViewMutation, { data, loading, error }] = useDeleteViewMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeleteViewMutation(
    baseOptions?: Apollo.MutationHookOptions<DeleteViewMutation, DeleteViewMutationVariables>,
) {
    return Apollo.useMutation<DeleteViewMutation, DeleteViewMutationVariables>(DeleteViewDocument, baseOptions);
}
export type DeleteViewMutationHookResult = ReturnType<typeof useDeleteViewMutation>;
export type DeleteViewMutationResult = Apollo.MutationResult<DeleteViewMutation>;
export type DeleteViewMutationOptions = Apollo.BaseMutationOptions<DeleteViewMutation, DeleteViewMutationVariables>;
