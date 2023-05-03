/* eslint-disable */
import * as Types from '../types.generated';

import { NonRecursiveMlFeatureTableFragment } from './fragments.generated';
import { gql } from '@apollo/client';
import { NonRecursiveMlFeatureTableFragmentDoc } from './fragments.generated';
import * as Apollo from '@apollo/client';
export type GetMlFeatureTableQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetMlFeatureTableQuery = { __typename?: 'Query' } & {
    mlFeatureTable?: Types.Maybe<{ __typename?: 'MLFeatureTable' } & NonRecursiveMlFeatureTableFragment>;
};

export const GetMlFeatureTableDocument = gql`
    query getMLFeatureTable($urn: String!) {
        mlFeatureTable(urn: $urn) {
            ...nonRecursiveMLFeatureTable
        }
    }
    ${NonRecursiveMlFeatureTableFragmentDoc}
`;

/**
 * __useGetMlFeatureTableQuery__
 *
 * To run a query within a React component, call `useGetMlFeatureTableQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetMlFeatureTableQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetMlFeatureTableQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetMlFeatureTableQuery(
    baseOptions: Apollo.QueryHookOptions<GetMlFeatureTableQuery, GetMlFeatureTableQueryVariables>,
) {
    return Apollo.useQuery<GetMlFeatureTableQuery, GetMlFeatureTableQueryVariables>(
        GetMlFeatureTableDocument,
        baseOptions,
    );
}
export function useGetMlFeatureTableLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetMlFeatureTableQuery, GetMlFeatureTableQueryVariables>,
) {
    return Apollo.useLazyQuery<GetMlFeatureTableQuery, GetMlFeatureTableQueryVariables>(
        GetMlFeatureTableDocument,
        baseOptions,
    );
}
export type GetMlFeatureTableQueryHookResult = ReturnType<typeof useGetMlFeatureTableQuery>;
export type GetMlFeatureTableLazyQueryHookResult = ReturnType<typeof useGetMlFeatureTableLazyQuery>;
export type GetMlFeatureTableQueryResult = Apollo.QueryResult<GetMlFeatureTableQuery, GetMlFeatureTableQueryVariables>;
