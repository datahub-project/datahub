/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type ListRolesQueryVariables = Types.Exact<{
    input: Types.ListRolesInput;
}>;

export type ListRolesQuery = { __typename?: 'Query' } & {
    listRoles?: Types.Maybe<
        { __typename?: 'ListRolesResult' } & Pick<Types.ListRolesResult, 'start' | 'count' | 'total'> & {
                roles: Array<
                    { __typename?: 'DataHubRole' } & Pick<
                        Types.DataHubRole,
                        'urn' | 'type' | 'name' | 'description'
                    > & {
                            users?: Types.Maybe<
                                { __typename?: 'EntityRelationshipsResult' } & Pick<
                                    Types.EntityRelationshipsResult,
                                    'start' | 'count' | 'total'
                                > & {
                                        relationships: Array<
                                            { __typename?: 'EntityRelationship' } & {
                                                entity?: Types.Maybe<
                                                    | { __typename?: 'AccessTokenMetadata' }
                                                    | { __typename?: 'Assertion' }
                                                    | { __typename?: 'Chart' }
                                                    | { __typename?: 'Container' }
                                                    | { __typename?: 'CorpGroup' }
                                                    | ({ __typename?: 'CorpUser' } & Pick<
                                                          Types.CorpUser,
                                                          'urn' | 'type' | 'username'
                                                      > & {
                                                              info?: Types.Maybe<
                                                                  { __typename?: 'CorpUserInfo' } & Pick<
                                                                      Types.CorpUserInfo,
                                                                      | 'active'
                                                                      | 'displayName'
                                                                      | 'title'
                                                                      | 'firstName'
                                                                      | 'lastName'
                                                                      | 'fullName'
                                                                  >
                                                              >;
                                                              editableProperties?: Types.Maybe<
                                                                  { __typename?: 'CorpUserEditableProperties' } & Pick<
                                                                      Types.CorpUserEditableProperties,
                                                                      'displayName' | 'title' | 'pictureLink'
                                                                  >
                                                              >;
                                                          })
                                                    | { __typename?: 'Dashboard' }
                                                    | { __typename?: 'DataFlow' }
                                                    | { __typename?: 'DataHubPolicy' }
                                                    | { __typename?: 'DataHubRole' }
                                                    | { __typename?: 'DataHubView' }
                                                    | { __typename?: 'DataJob' }
                                                    | { __typename?: 'DataPlatform' }
                                                    | { __typename?: 'DataPlatformInstance' }
                                                    | { __typename?: 'DataProcessInstance' }
                                                    | { __typename?: 'Dataset' }
                                                    | { __typename?: 'Domain' }
                                                    | { __typename?: 'GlossaryNode' }
                                                    | { __typename?: 'GlossaryTerm' }
                                                    | { __typename?: 'MLFeature' }
                                                    | { __typename?: 'MLFeatureTable' }
                                                    | { __typename?: 'MLModel' }
                                                    | { __typename?: 'MLModelGroup' }
                                                    | { __typename?: 'MLPrimaryKey' }
                                                    | { __typename?: 'Notebook' }
                                                    | { __typename?: 'Post' }
                                                    | { __typename?: 'QueryEntity' }
                                                    | { __typename?: 'SchemaFieldEntity' }
                                                    | { __typename?: 'Tag' }
                                                    | { __typename?: 'Test' }
                                                    | { __typename?: 'VersionedDataset' }
                                                >;
                                            }
                                        >;
                                    }
                            >;
                            policies?: Types.Maybe<
                                { __typename?: 'EntityRelationshipsResult' } & Pick<
                                    Types.EntityRelationshipsResult,
                                    'start' | 'count' | 'total'
                                > & {
                                        relationships: Array<
                                            { __typename?: 'EntityRelationship' } & {
                                                entity?: Types.Maybe<
                                                    | { __typename?: 'AccessTokenMetadata' }
                                                    | { __typename?: 'Assertion' }
                                                    | { __typename?: 'Chart' }
                                                    | { __typename?: 'Container' }
                                                    | { __typename?: 'CorpGroup' }
                                                    | { __typename?: 'CorpUser' }
                                                    | { __typename?: 'Dashboard' }
                                                    | { __typename?: 'DataFlow' }
                                                    | ({ __typename?: 'DataHubPolicy' } & Pick<
                                                          Types.DataHubPolicy,
                                                          'urn' | 'type' | 'name'
                                                      > & {
                                                              relationships?: Types.Maybe<
                                                                  { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                                      Types.EntityRelationshipsResult,
                                                                      'start' | 'count' | 'total'
                                                                  >
                                                              >;
                                                          })
                                                    | { __typename?: 'DataHubRole' }
                                                    | { __typename?: 'DataHubView' }
                                                    | { __typename?: 'DataJob' }
                                                    | { __typename?: 'DataPlatform' }
                                                    | { __typename?: 'DataPlatformInstance' }
                                                    | { __typename?: 'DataProcessInstance' }
                                                    | { __typename?: 'Dataset' }
                                                    | { __typename?: 'Domain' }
                                                    | { __typename?: 'GlossaryNode' }
                                                    | { __typename?: 'GlossaryTerm' }
                                                    | { __typename?: 'MLFeature' }
                                                    | { __typename?: 'MLFeatureTable' }
                                                    | { __typename?: 'MLModel' }
                                                    | { __typename?: 'MLModelGroup' }
                                                    | { __typename?: 'MLPrimaryKey' }
                                                    | { __typename?: 'Notebook' }
                                                    | { __typename?: 'Post' }
                                                    | { __typename?: 'QueryEntity' }
                                                    | { __typename?: 'SchemaFieldEntity' }
                                                    | { __typename?: 'Tag' }
                                                    | { __typename?: 'Test' }
                                                    | { __typename?: 'VersionedDataset' }
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

export type GetInviteTokenQueryVariables = Types.Exact<{
    input: Types.GetInviteTokenInput;
}>;

export type GetInviteTokenQuery = { __typename?: 'Query' } & {
    getInviteToken?: Types.Maybe<{ __typename?: 'InviteToken' } & Pick<Types.InviteToken, 'inviteToken'>>;
};

export const ListRolesDocument = gql`
    query listRoles($input: ListRolesInput!) {
        listRoles(input: $input) {
            start
            count
            total
            roles {
                urn
                type
                name
                description
                users: relationships(input: { types: ["IsMemberOfRole"], direction: INCOMING, start: 0, count: 10 }) {
                    start
                    count
                    total
                    relationships {
                        entity {
                            ... on CorpUser {
                                urn
                                type
                                username
                                info {
                                    active
                                    displayName
                                    title
                                    firstName
                                    lastName
                                    fullName
                                }
                                editableProperties {
                                    displayName
                                    title
                                    pictureLink
                                }
                            }
                        }
                    }
                }
                policies: relationships(
                    input: { types: ["IsAssociatedWithRole"], direction: INCOMING, start: 0, count: 10 }
                ) {
                    start
                    count
                    total
                    relationships {
                        entity {
                            ... on DataHubPolicy {
                                urn
                                type
                                name
                                relationships(input: { types: ["IsAssociatedWithRole"], direction: OUTGOING }) {
                                    start
                                    count
                                    total
                                }
                            }
                        }
                    }
                }
            }
        }
    }
`;

/**
 * __useListRolesQuery__
 *
 * To run a query within a React component, call `useListRolesQuery` and pass it any options that fit your needs.
 * When your component renders, `useListRolesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListRolesQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useListRolesQuery(baseOptions: Apollo.QueryHookOptions<ListRolesQuery, ListRolesQueryVariables>) {
    return Apollo.useQuery<ListRolesQuery, ListRolesQueryVariables>(ListRolesDocument, baseOptions);
}
export function useListRolesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListRolesQuery, ListRolesQueryVariables>,
) {
    return Apollo.useLazyQuery<ListRolesQuery, ListRolesQueryVariables>(ListRolesDocument, baseOptions);
}
export type ListRolesQueryHookResult = ReturnType<typeof useListRolesQuery>;
export type ListRolesLazyQueryHookResult = ReturnType<typeof useListRolesLazyQuery>;
export type ListRolesQueryResult = Apollo.QueryResult<ListRolesQuery, ListRolesQueryVariables>;
export const GetInviteTokenDocument = gql`
    query getInviteToken($input: GetInviteTokenInput!) {
        getInviteToken(input: $input) {
            inviteToken
        }
    }
`;

/**
 * __useGetInviteTokenQuery__
 *
 * To run a query within a React component, call `useGetInviteTokenQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetInviteTokenQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetInviteTokenQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetInviteTokenQuery(
    baseOptions: Apollo.QueryHookOptions<GetInviteTokenQuery, GetInviteTokenQueryVariables>,
) {
    return Apollo.useQuery<GetInviteTokenQuery, GetInviteTokenQueryVariables>(GetInviteTokenDocument, baseOptions);
}
export function useGetInviteTokenLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetInviteTokenQuery, GetInviteTokenQueryVariables>,
) {
    return Apollo.useLazyQuery<GetInviteTokenQuery, GetInviteTokenQueryVariables>(GetInviteTokenDocument, baseOptions);
}
export type GetInviteTokenQueryHookResult = ReturnType<typeof useGetInviteTokenQuery>;
export type GetInviteTokenLazyQueryHookResult = ReturnType<typeof useGetInviteTokenLazyQuery>;
export type GetInviteTokenQueryResult = Apollo.QueryResult<GetInviteTokenQuery, GetInviteTokenQueryVariables>;
