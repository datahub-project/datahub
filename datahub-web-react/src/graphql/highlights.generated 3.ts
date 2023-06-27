/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type GetHighlightsQueryVariables = Types.Exact<{ [key: string]: never }>;

export type GetHighlightsQuery = { __typename?: 'Query' } & {
    getHighlights: Array<{ __typename?: 'Highlight' } & Pick<Types.Highlight, 'value' | 'title' | 'body'>>;
};

export const GetHighlightsDocument = gql`
    query getHighlights {
        getHighlights {
            value
            title
            body
        }
    }
`;

/**
 * __useGetHighlightsQuery__
 *
 * To run a query within a React component, call `useGetHighlightsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetHighlightsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetHighlightsQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetHighlightsQuery(
    baseOptions?: Apollo.QueryHookOptions<GetHighlightsQuery, GetHighlightsQueryVariables>,
) {
    return Apollo.useQuery<GetHighlightsQuery, GetHighlightsQueryVariables>(GetHighlightsDocument, baseOptions);
}
export function useGetHighlightsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetHighlightsQuery, GetHighlightsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetHighlightsQuery, GetHighlightsQueryVariables>(GetHighlightsDocument, baseOptions);
}
export type GetHighlightsQueryHookResult = ReturnType<typeof useGetHighlightsQuery>;
export type GetHighlightsLazyQueryHookResult = ReturnType<typeof useGetHighlightsLazyQuery>;
export type GetHighlightsQueryResult = Apollo.QueryResult<GetHighlightsQuery, GetHighlightsQueryVariables>;
