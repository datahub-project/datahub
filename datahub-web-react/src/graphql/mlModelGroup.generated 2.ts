/* eslint-disable */
import * as Types from '../types.generated';

import { NonRecursiveMlModelGroupFieldsFragment } from './fragments.generated';
import { FullRelationshipResultsFragment } from './relationships.generated';
import { gql } from '@apollo/client';
import { NonRecursiveMlModelGroupFieldsFragmentDoc } from './fragments.generated';
import { FullRelationshipResultsFragmentDoc } from './relationships.generated';
import * as Apollo from '@apollo/client';
export type GetMlModelGroupQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetMlModelGroupQuery = { __typename?: 'Query' } & {
    mlModelGroup?: Types.Maybe<
        { __typename?: 'MLModelGroup' } & {
            incoming?: Types.Maybe<{ __typename?: 'EntityRelationshipsResult' } & FullRelationshipResultsFragment>;
            outgoing?: Types.Maybe<{ __typename?: 'EntityRelationshipsResult' } & FullRelationshipResultsFragment>;
        } & NonRecursiveMlModelGroupFieldsFragment
    >;
};

export const GetMlModelGroupDocument = gql`
    query getMLModelGroup($urn: String!) {
        mlModelGroup(urn: $urn) {
            ...nonRecursiveMLModelGroupFields
            incoming: relationships(
                input: {
                    types: ["DownstreamOf", "Consumes", "Produces", "TrainedBy", "MemberOf"]
                    direction: INCOMING
                    start: 0
                    count: 100
                }
            ) {
                ...fullRelationshipResults
            }
            outgoing: relationships(
                input: {
                    types: ["DownstreamOf", "Consumes", "Produces", "TrainedBy", "MemberOf"]
                    direction: OUTGOING
                    start: 0
                    count: 100
                }
            ) {
                ...fullRelationshipResults
            }
        }
    }
    ${NonRecursiveMlModelGroupFieldsFragmentDoc}
    ${FullRelationshipResultsFragmentDoc}
`;

/**
 * __useGetMlModelGroupQuery__
 *
 * To run a query within a React component, call `useGetMlModelGroupQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetMlModelGroupQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetMlModelGroupQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetMlModelGroupQuery(
    baseOptions: Apollo.QueryHookOptions<GetMlModelGroupQuery, GetMlModelGroupQueryVariables>,
) {
    return Apollo.useQuery<GetMlModelGroupQuery, GetMlModelGroupQueryVariables>(GetMlModelGroupDocument, baseOptions);
}
export function useGetMlModelGroupLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetMlModelGroupQuery, GetMlModelGroupQueryVariables>,
) {
    return Apollo.useLazyQuery<GetMlModelGroupQuery, GetMlModelGroupQueryVariables>(
        GetMlModelGroupDocument,
        baseOptions,
    );
}
export type GetMlModelGroupQueryHookResult = ReturnType<typeof useGetMlModelGroupQuery>;
export type GetMlModelGroupLazyQueryHookResult = ReturnType<typeof useGetMlModelGroupLazyQuery>;
export type GetMlModelGroupQueryResult = Apollo.QueryResult<GetMlModelGroupQuery, GetMlModelGroupQueryVariables>;
