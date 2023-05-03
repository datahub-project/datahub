/* eslint-disable */
import * as Types from '../types.generated';

import { NonRecursiveMlFeatureFragment } from './fragments.generated';
import { FullRelationshipResultsFragment } from './relationships.generated';
import { gql } from '@apollo/client';
import { NonRecursiveMlFeatureFragmentDoc } from './fragments.generated';
import { FullRelationshipResultsFragmentDoc } from './relationships.generated';
import * as Apollo from '@apollo/client';
export type GetMlFeatureQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetMlFeatureQuery = { __typename?: 'Query' } & {
    mlFeature?: Types.Maybe<
        { __typename?: 'MLFeature' } & {
            featureTables?: Types.Maybe<{ __typename?: 'EntityRelationshipsResult' } & FullRelationshipResultsFragment>;
        } & NonRecursiveMlFeatureFragment
    >;
};

export const GetMlFeatureDocument = gql`
    query getMLFeature($urn: String!) {
        mlFeature(urn: $urn) {
            ...nonRecursiveMLFeature
            featureTables: relationships(input: { types: ["Contains"], direction: INCOMING, start: 0, count: 100 }) {
                ...fullRelationshipResults
            }
        }
    }
    ${NonRecursiveMlFeatureFragmentDoc}
    ${FullRelationshipResultsFragmentDoc}
`;

/**
 * __useGetMlFeatureQuery__
 *
 * To run a query within a React component, call `useGetMlFeatureQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetMlFeatureQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetMlFeatureQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetMlFeatureQuery(
    baseOptions: Apollo.QueryHookOptions<GetMlFeatureQuery, GetMlFeatureQueryVariables>,
) {
    return Apollo.useQuery<GetMlFeatureQuery, GetMlFeatureQueryVariables>(GetMlFeatureDocument, baseOptions);
}
export function useGetMlFeatureLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetMlFeatureQuery, GetMlFeatureQueryVariables>,
) {
    return Apollo.useLazyQuery<GetMlFeatureQuery, GetMlFeatureQueryVariables>(GetMlFeatureDocument, baseOptions);
}
export type GetMlFeatureQueryHookResult = ReturnType<typeof useGetMlFeatureQuery>;
export type GetMlFeatureLazyQueryHookResult = ReturnType<typeof useGetMlFeatureLazyQuery>;
export type GetMlFeatureQueryResult = Apollo.QueryResult<GetMlFeatureQuery, GetMlFeatureQueryVariables>;
