/* eslint-disable */
import * as Types from '../types.generated';

import { NonRecursiveMlPrimaryKeyFragment } from './fragments.generated';
import { FullRelationshipResultsFragment } from './relationships.generated';
import { gql } from '@apollo/client';
import { NonRecursiveMlPrimaryKeyFragmentDoc } from './fragments.generated';
import { FullRelationshipResultsFragmentDoc } from './relationships.generated';
import * as Apollo from '@apollo/client';
export type GetMlPrimaryKeyQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetMlPrimaryKeyQuery = { __typename?: 'Query' } & {
    mlPrimaryKey?: Types.Maybe<
        { __typename?: 'MLPrimaryKey' } & {
            featureTables?: Types.Maybe<{ __typename?: 'EntityRelationshipsResult' } & FullRelationshipResultsFragment>;
        } & NonRecursiveMlPrimaryKeyFragment
    >;
};

export const GetMlPrimaryKeyDocument = gql`
    query getMLPrimaryKey($urn: String!) {
        mlPrimaryKey(urn: $urn) {
            ...nonRecursiveMLPrimaryKey
            featureTables: relationships(input: { types: ["KeyedBy"], direction: INCOMING, start: 0, count: 100 }) {
                ...fullRelationshipResults
            }
        }
    }
    ${NonRecursiveMlPrimaryKeyFragmentDoc}
    ${FullRelationshipResultsFragmentDoc}
`;

/**
 * __useGetMlPrimaryKeyQuery__
 *
 * To run a query within a React component, call `useGetMlPrimaryKeyQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetMlPrimaryKeyQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetMlPrimaryKeyQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetMlPrimaryKeyQuery(
    baseOptions: Apollo.QueryHookOptions<GetMlPrimaryKeyQuery, GetMlPrimaryKeyQueryVariables>,
) {
    return Apollo.useQuery<GetMlPrimaryKeyQuery, GetMlPrimaryKeyQueryVariables>(GetMlPrimaryKeyDocument, baseOptions);
}
export function useGetMlPrimaryKeyLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetMlPrimaryKeyQuery, GetMlPrimaryKeyQueryVariables>,
) {
    return Apollo.useLazyQuery<GetMlPrimaryKeyQuery, GetMlPrimaryKeyQueryVariables>(
        GetMlPrimaryKeyDocument,
        baseOptions,
    );
}
export type GetMlPrimaryKeyQueryHookResult = ReturnType<typeof useGetMlPrimaryKeyQuery>;
export type GetMlPrimaryKeyLazyQueryHookResult = ReturnType<typeof useGetMlPrimaryKeyLazyQuery>;
export type GetMlPrimaryKeyQueryResult = Apollo.QueryResult<GetMlPrimaryKeyQuery, GetMlPrimaryKeyQueryVariables>;
