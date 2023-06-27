/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type GetAccessTokenQueryVariables = Types.Exact<{
    input: Types.GetAccessTokenInput;
}>;

export type GetAccessTokenQuery = { __typename?: 'Query' } & {
    getAccessToken?: Types.Maybe<{ __typename?: 'AccessToken' } & Pick<Types.AccessToken, 'accessToken'>>;
};

export type ListAccessTokensQueryVariables = Types.Exact<{
    input: Types.ListAccessTokenInput;
}>;

export type ListAccessTokensQuery = { __typename?: 'Query' } & {
    listAccessTokens: { __typename?: 'ListAccessTokenResult' } & Pick<
        Types.ListAccessTokenResult,
        'start' | 'count' | 'total'
    > & {
            tokens: Array<
                { __typename?: 'AccessTokenMetadata' } & Pick<
                    Types.AccessTokenMetadata,
                    'urn' | 'type' | 'id' | 'name' | 'description' | 'actorUrn' | 'ownerUrn' | 'createdAt' | 'expiresAt'
                >
            >;
        };
};

export type CreateAccessTokenMutationVariables = Types.Exact<{
    input: Types.CreateAccessTokenInput;
}>;

export type CreateAccessTokenMutation = { __typename?: 'Mutation' } & {
    createAccessToken?: Types.Maybe<{ __typename?: 'AccessToken' } & Pick<Types.AccessToken, 'accessToken'>>;
};

export type RevokeAccessTokenMutationVariables = Types.Exact<{
    tokenId: Types.Scalars['String'];
}>;

export type RevokeAccessTokenMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'revokeAccessToken'>;

export const GetAccessTokenDocument = gql`
    query getAccessToken($input: GetAccessTokenInput!) {
        getAccessToken(input: $input) {
            accessToken
        }
    }
`;

/**
 * __useGetAccessTokenQuery__
 *
 * To run a query within a React component, call `useGetAccessTokenQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetAccessTokenQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetAccessTokenQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetAccessTokenQuery(
    baseOptions: Apollo.QueryHookOptions<GetAccessTokenQuery, GetAccessTokenQueryVariables>,
) {
    return Apollo.useQuery<GetAccessTokenQuery, GetAccessTokenQueryVariables>(GetAccessTokenDocument, baseOptions);
}
export function useGetAccessTokenLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetAccessTokenQuery, GetAccessTokenQueryVariables>,
) {
    return Apollo.useLazyQuery<GetAccessTokenQuery, GetAccessTokenQueryVariables>(GetAccessTokenDocument, baseOptions);
}
export type GetAccessTokenQueryHookResult = ReturnType<typeof useGetAccessTokenQuery>;
export type GetAccessTokenLazyQueryHookResult = ReturnType<typeof useGetAccessTokenLazyQuery>;
export type GetAccessTokenQueryResult = Apollo.QueryResult<GetAccessTokenQuery, GetAccessTokenQueryVariables>;
export const ListAccessTokensDocument = gql`
    query listAccessTokens($input: ListAccessTokenInput!) {
        listAccessTokens(input: $input) {
            start
            count
            total
            tokens {
                urn
                type
                id
                name
                description
                actorUrn
                ownerUrn
                createdAt
                expiresAt
            }
        }
    }
`;

/**
 * __useListAccessTokensQuery__
 *
 * To run a query within a React component, call `useListAccessTokensQuery` and pass it any options that fit your needs.
 * When your component renders, `useListAccessTokensQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListAccessTokensQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useListAccessTokensQuery(
    baseOptions: Apollo.QueryHookOptions<ListAccessTokensQuery, ListAccessTokensQueryVariables>,
) {
    return Apollo.useQuery<ListAccessTokensQuery, ListAccessTokensQueryVariables>(
        ListAccessTokensDocument,
        baseOptions,
    );
}
export function useListAccessTokensLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListAccessTokensQuery, ListAccessTokensQueryVariables>,
) {
    return Apollo.useLazyQuery<ListAccessTokensQuery, ListAccessTokensQueryVariables>(
        ListAccessTokensDocument,
        baseOptions,
    );
}
export type ListAccessTokensQueryHookResult = ReturnType<typeof useListAccessTokensQuery>;
export type ListAccessTokensLazyQueryHookResult = ReturnType<typeof useListAccessTokensLazyQuery>;
export type ListAccessTokensQueryResult = Apollo.QueryResult<ListAccessTokensQuery, ListAccessTokensQueryVariables>;
export const CreateAccessTokenDocument = gql`
    mutation createAccessToken($input: CreateAccessTokenInput!) {
        createAccessToken(input: $input) {
            accessToken
        }
    }
`;
export type CreateAccessTokenMutationFn = Apollo.MutationFunction<
    CreateAccessTokenMutation,
    CreateAccessTokenMutationVariables
>;

/**
 * __useCreateAccessTokenMutation__
 *
 * To run a mutation, you first call `useCreateAccessTokenMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateAccessTokenMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createAccessTokenMutation, { data, loading, error }] = useCreateAccessTokenMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateAccessTokenMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateAccessTokenMutation, CreateAccessTokenMutationVariables>,
) {
    return Apollo.useMutation<CreateAccessTokenMutation, CreateAccessTokenMutationVariables>(
        CreateAccessTokenDocument,
        baseOptions,
    );
}
export type CreateAccessTokenMutationHookResult = ReturnType<typeof useCreateAccessTokenMutation>;
export type CreateAccessTokenMutationResult = Apollo.MutationResult<CreateAccessTokenMutation>;
export type CreateAccessTokenMutationOptions = Apollo.BaseMutationOptions<
    CreateAccessTokenMutation,
    CreateAccessTokenMutationVariables
>;
export const RevokeAccessTokenDocument = gql`
    mutation revokeAccessToken($tokenId: String!) {
        revokeAccessToken(tokenId: $tokenId)
    }
`;
export type RevokeAccessTokenMutationFn = Apollo.MutationFunction<
    RevokeAccessTokenMutation,
    RevokeAccessTokenMutationVariables
>;

/**
 * __useRevokeAccessTokenMutation__
 *
 * To run a mutation, you first call `useRevokeAccessTokenMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useRevokeAccessTokenMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [revokeAccessTokenMutation, { data, loading, error }] = useRevokeAccessTokenMutation({
 *   variables: {
 *      tokenId: // value for 'tokenId'
 *   },
 * });
 */
export function useRevokeAccessTokenMutation(
    baseOptions?: Apollo.MutationHookOptions<RevokeAccessTokenMutation, RevokeAccessTokenMutationVariables>,
) {
    return Apollo.useMutation<RevokeAccessTokenMutation, RevokeAccessTokenMutationVariables>(
        RevokeAccessTokenDocument,
        baseOptions,
    );
}
export type RevokeAccessTokenMutationHookResult = ReturnType<typeof useRevokeAccessTokenMutation>;
export type RevokeAccessTokenMutationResult = Apollo.MutationResult<RevokeAccessTokenMutation>;
export type RevokeAccessTokenMutationOptions = Apollo.BaseMutationOptions<
    RevokeAccessTokenMutation,
    RevokeAccessTokenMutationVariables
>;
