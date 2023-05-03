/* eslint-disable */
import * as Types from '../types.generated';

import { DashboardFieldsFragment } from './fragments.generated';
import { FullRelationshipResultsFragment } from './relationships.generated';
import { gql } from '@apollo/client';
import { DashboardFieldsFragmentDoc } from './fragments.generated';
import { FullRelationshipResultsFragmentDoc } from './relationships.generated';
import * as Apollo from '@apollo/client';
export type GetDashboardQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetDashboardQuery = { __typename?: 'Query' } & {
    dashboard?: Types.Maybe<
        { __typename?: 'Dashboard' } & {
            charts?: Types.Maybe<{ __typename?: 'EntityRelationshipsResult' } & FullRelationshipResultsFragment>;
            datasets?: Types.Maybe<{ __typename?: 'EntityRelationshipsResult' } & FullRelationshipResultsFragment>;
        } & DashboardFieldsFragment
    >;
};

export type UpdateDashboardMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.DashboardUpdateInput;
}>;

export type UpdateDashboardMutation = { __typename?: 'Mutation' } & {
    updateDashboard?: Types.Maybe<{ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn'>>;
};

export const GetDashboardDocument = gql`
    query getDashboard($urn: String!) {
        dashboard(urn: $urn) {
            ...dashboardFields
            charts: relationships(input: { types: ["Contains"], direction: OUTGOING, start: 0, count: 100 }) {
                ...fullRelationshipResults
            }
            datasets: relationships(input: { types: ["Consumes"], direction: OUTGOING, start: 0, count: 100 }) {
                ...fullRelationshipResults
            }
        }
    }
    ${DashboardFieldsFragmentDoc}
    ${FullRelationshipResultsFragmentDoc}
`;

/**
 * __useGetDashboardQuery__
 *
 * To run a query within a React component, call `useGetDashboardQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDashboardQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDashboardQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetDashboardQuery(
    baseOptions: Apollo.QueryHookOptions<GetDashboardQuery, GetDashboardQueryVariables>,
) {
    return Apollo.useQuery<GetDashboardQuery, GetDashboardQueryVariables>(GetDashboardDocument, baseOptions);
}
export function useGetDashboardLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDashboardQuery, GetDashboardQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDashboardQuery, GetDashboardQueryVariables>(GetDashboardDocument, baseOptions);
}
export type GetDashboardQueryHookResult = ReturnType<typeof useGetDashboardQuery>;
export type GetDashboardLazyQueryHookResult = ReturnType<typeof useGetDashboardLazyQuery>;
export type GetDashboardQueryResult = Apollo.QueryResult<GetDashboardQuery, GetDashboardQueryVariables>;
export const UpdateDashboardDocument = gql`
    mutation updateDashboard($urn: String!, $input: DashboardUpdateInput!) {
        updateDashboard(urn: $urn, input: $input) {
            urn
        }
    }
`;
export type UpdateDashboardMutationFn = Apollo.MutationFunction<
    UpdateDashboardMutation,
    UpdateDashboardMutationVariables
>;

/**
 * __useUpdateDashboardMutation__
 *
 * To run a mutation, you first call `useUpdateDashboardMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateDashboardMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateDashboardMutation, { data, loading, error }] = useUpdateDashboardMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateDashboardMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateDashboardMutation, UpdateDashboardMutationVariables>,
) {
    return Apollo.useMutation<UpdateDashboardMutation, UpdateDashboardMutationVariables>(
        UpdateDashboardDocument,
        baseOptions,
    );
}
export type UpdateDashboardMutationHookResult = ReturnType<typeof useUpdateDashboardMutation>;
export type UpdateDashboardMutationResult = Apollo.MutationResult<UpdateDashboardMutation>;
export type UpdateDashboardMutationOptions = Apollo.BaseMutationOptions<
    UpdateDashboardMutation,
    UpdateDashboardMutationVariables
>;
