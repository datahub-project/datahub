/* eslint-disable */
import * as Types from '../types.generated';

import { GlobalTagsFieldsFragment, GlossaryTermsFragment } from './fragments.generated';
import { gql } from '@apollo/client';
import { GlobalTagsFieldsFragmentDoc, GlossaryTermsFragmentDoc } from './fragments.generated';
import * as Apollo from '@apollo/client';
export type GetUserQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    groupsCount: Types.Scalars['Int'];
}>;

export type GetUserQuery = { __typename?: 'Query' } & {
    corpUser?: Types.Maybe<
        { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'username' | 'isNativeUser'> & {
                info?: Types.Maybe<
                    { __typename?: 'CorpUserInfo' } & Pick<
                        Types.CorpUserInfo,
                        | 'active'
                        | 'displayName'
                        | 'title'
                        | 'firstName'
                        | 'lastName'
                        | 'fullName'
                        | 'email'
                        | 'departmentName'
                    >
                >;
                editableProperties?: Types.Maybe<
                    { __typename?: 'CorpUserEditableProperties' } & Pick<
                        Types.CorpUserEditableProperties,
                        | 'slack'
                        | 'phone'
                        | 'pictureLink'
                        | 'aboutMe'
                        | 'teams'
                        | 'skills'
                        | 'displayName'
                        | 'title'
                        | 'email'
                    >
                >;
                globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                groups?: Types.Maybe<
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
                                        | ({ __typename?: 'CorpGroup' } & Pick<
                                              Types.CorpGroup,
                                              'urn' | 'type' | 'name'
                                          > & {
                                                  info?: Types.Maybe<
                                                      { __typename?: 'CorpGroupInfo' } & Pick<
                                                          Types.CorpGroupInfo,
                                                          'displayName' | 'description' | 'email'
                                                      >
                                                  >;
                                                  relationships?: Types.Maybe<
                                                      { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                          Types.EntityRelationshipsResult,
                                                          'start' | 'count' | 'total'
                                                      >
                                                  >;
                                              })
                                        | { __typename?: 'CorpUser' }
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
                roles?: Types.Maybe<
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
                                        | { __typename?: 'DataHubPolicy' }
                                        | ({ __typename?: 'DataHubRole' } & Pick<
                                              Types.DataHubRole,
                                              'urn' | 'type' | 'name'
                                          > & {
                                                  relationships?: Types.Maybe<
                                                      { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                          Types.EntityRelationshipsResult,
                                                          'start' | 'count' | 'total'
                                                      >
                                                  >;
                                              })
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
};

export type GetUserGroupsQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    start: Types.Scalars['Int'];
    count: Types.Scalars['Int'];
}>;

export type GetUserGroupsQuery = { __typename?: 'Query' } & {
    corpUser?: Types.Maybe<
        { __typename?: 'CorpUser' } & {
            relationships?: Types.Maybe<
                { __typename?: 'EntityRelationshipsResult' } & Pick<
                    Types.EntityRelationshipsResult,
                    'start' | 'count' | 'total'
                > & {
                        relationships: Array<
                            { __typename?: 'EntityRelationship' } & {
                                entity?: Types.Maybe<
                                    | ({ __typename?: 'AccessTokenMetadata' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Assertion' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Chart' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Container' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type' | 'name'> & {
                                              info?: Types.Maybe<
                                                  { __typename?: 'CorpGroupInfo' } & Pick<
                                                      Types.CorpGroupInfo,
                                                      'displayName' | 'description' | 'email'
                                                  >
                                              >;
                                              relationships?: Types.Maybe<
                                                  { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                      Types.EntityRelationshipsResult,
                                                      'start' | 'count' | 'total'
                                                  >
                                              >;
                                          })
                                    | ({ __typename?: 'CorpUser' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Dashboard' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'DataFlow' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'DataHubPolicy' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'DataHubRole' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'DataHubView' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'DataJob' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'DataPlatform' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'DataPlatformInstance' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'DataProcessInstance' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Dataset' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Domain' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'GlossaryNode' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'GlossaryTerm' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'MLFeature' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'MLFeatureTable' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'MLModel' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'MLModelGroup' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'MLPrimaryKey' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Notebook' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Post' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'QueryEntity' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'SchemaFieldEntity' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Tag' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'Test' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                    | ({ __typename?: 'VersionedDataset' } & {
                                          relationships?: Types.Maybe<
                                              { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                  Types.EntityRelationshipsResult,
                                                  'start' | 'count' | 'total'
                                              >
                                          >;
                                      })
                                >;
                            }
                        >;
                    }
            >;
        }
    >;
};

export type ListUsersQueryVariables = Types.Exact<{
    input: Types.ListUsersInput;
}>;

export type ListUsersQuery = { __typename?: 'Query' } & {
    listUsers?: Types.Maybe<
        { __typename?: 'ListUsersResult' } & Pick<Types.ListUsersResult, 'start' | 'count' | 'total'> & {
                users: Array<
                    { __typename?: 'CorpUser' } & Pick<
                        Types.CorpUser,
                        'urn' | 'username' | 'isNativeUser' | 'status'
                    > & {
                            info?: Types.Maybe<
                                { __typename?: 'CorpUserInfo' } & Pick<
                                    Types.CorpUserInfo,
                                    'active' | 'displayName' | 'title' | 'firstName' | 'lastName' | 'fullName' | 'email'
                                >
                            >;
                            editableProperties?: Types.Maybe<
                                { __typename?: 'CorpUserEditableProperties' } & Pick<
                                    Types.CorpUserEditableProperties,
                                    'displayName' | 'pictureLink' | 'teams' | 'title' | 'skills'
                                >
                            >;
                            roles?: Types.Maybe<
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
                                                    | { __typename?: 'DataHubPolicy' }
                                                    | ({ __typename?: 'DataHubRole' } & Pick<
                                                          Types.DataHubRole,
                                                          'urn' | 'type' | 'name'
                                                      > & {
                                                              relationships?: Types.Maybe<
                                                                  { __typename?: 'EntityRelationshipsResult' } & Pick<
                                                                      Types.EntityRelationshipsResult,
                                                                      'start' | 'count' | 'total'
                                                                  >
                                                              >;
                                                          })
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

export type RemoveUserMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type RemoveUserMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'removeUser'>;

export type UpdateUserStatusMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    status: Types.CorpUserStatus;
}>;

export type UpdateUserStatusMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updateUserStatus'>;

export type UpdateCorpUserPropertiesMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.CorpUserUpdateInput;
}>;

export type UpdateCorpUserPropertiesMutation = { __typename?: 'Mutation' } & {
    updateCorpUserProperties?: Types.Maybe<{ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn'>>;
};

export type CreateNativeUserResetTokenMutationVariables = Types.Exact<{
    input: Types.CreateNativeUserResetTokenInput;
}>;

export type CreateNativeUserResetTokenMutation = { __typename?: 'Mutation' } & {
    createNativeUserResetToken?: Types.Maybe<{ __typename?: 'ResetToken' } & Pick<Types.ResetToken, 'resetToken'>>;
};

export type UpdateCorpUserViewsSettingsMutationVariables = Types.Exact<{
    input: Types.UpdateCorpUserViewsSettingsInput;
}>;

export type UpdateCorpUserViewsSettingsMutation = { __typename?: 'Mutation' } & Pick<
    Types.Mutation,
    'updateCorpUserViewsSettings'
>;

export const GetUserDocument = gql`
    query getUser($urn: String!, $groupsCount: Int!) {
        corpUser(urn: $urn) {
            urn
            username
            isNativeUser
            info {
                active
                displayName
                title
                firstName
                lastName
                fullName
                email
                departmentName
            }
            editableProperties {
                slack
                phone
                pictureLink
                aboutMe
                teams
                skills
                displayName
                title
                email
            }
            globalTags {
                ...globalTagsFields
            }
            groups: relationships(
                input: {
                    types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"]
                    direction: OUTGOING
                    start: 0
                    count: $groupsCount
                }
            ) {
                start
                count
                total
                relationships {
                    entity {
                        ... on CorpGroup {
                            urn
                            type
                            name
                            info {
                                displayName
                                description
                                email
                            }
                            relationships(
                                input: { types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"], direction: INCOMING }
                            ) {
                                start
                                count
                                total
                            }
                        }
                    }
                }
            }
            roles: relationships(
                input: { types: ["IsMemberOfRole"], direction: OUTGOING, start: 0, count: $groupsCount }
            ) {
                start
                count
                total
                relationships {
                    entity {
                        ... on DataHubRole {
                            urn
                            type
                            name
                            relationships(input: { types: ["IsMemberOfRole"], direction: INCOMING }) {
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
    ${GlobalTagsFieldsFragmentDoc}
`;

/**
 * __useGetUserQuery__
 *
 * To run a query within a React component, call `useGetUserQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetUserQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetUserQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      groupsCount: // value for 'groupsCount'
 *   },
 * });
 */
export function useGetUserQuery(baseOptions: Apollo.QueryHookOptions<GetUserQuery, GetUserQueryVariables>) {
    return Apollo.useQuery<GetUserQuery, GetUserQueryVariables>(GetUserDocument, baseOptions);
}
export function useGetUserLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetUserQuery, GetUserQueryVariables>) {
    return Apollo.useLazyQuery<GetUserQuery, GetUserQueryVariables>(GetUserDocument, baseOptions);
}
export type GetUserQueryHookResult = ReturnType<typeof useGetUserQuery>;
export type GetUserLazyQueryHookResult = ReturnType<typeof useGetUserLazyQuery>;
export type GetUserQueryResult = Apollo.QueryResult<GetUserQuery, GetUserQueryVariables>;
export const GetUserGroupsDocument = gql`
    query getUserGroups($urn: String!, $start: Int!, $count: Int!) {
        corpUser(urn: $urn) {
            relationships(
                input: {
                    types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"]
                    direction: OUTGOING
                    start: $start
                    count: $count
                }
            ) {
                start
                count
                total
                relationships {
                    entity {
                        ... on CorpGroup {
                            urn
                            type
                            name
                            info {
                                displayName
                                description
                                email
                            }
                        }
                        relationships(
                            input: { types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"], direction: INCOMING }
                        ) {
                            start
                            count
                            total
                        }
                    }
                }
            }
        }
    }
`;

/**
 * __useGetUserGroupsQuery__
 *
 * To run a query within a React component, call `useGetUserGroupsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetUserGroupsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetUserGroupsQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      start: // value for 'start'
 *      count: // value for 'count'
 *   },
 * });
 */
export function useGetUserGroupsQuery(
    baseOptions: Apollo.QueryHookOptions<GetUserGroupsQuery, GetUserGroupsQueryVariables>,
) {
    return Apollo.useQuery<GetUserGroupsQuery, GetUserGroupsQueryVariables>(GetUserGroupsDocument, baseOptions);
}
export function useGetUserGroupsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetUserGroupsQuery, GetUserGroupsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetUserGroupsQuery, GetUserGroupsQueryVariables>(GetUserGroupsDocument, baseOptions);
}
export type GetUserGroupsQueryHookResult = ReturnType<typeof useGetUserGroupsQuery>;
export type GetUserGroupsLazyQueryHookResult = ReturnType<typeof useGetUserGroupsLazyQuery>;
export type GetUserGroupsQueryResult = Apollo.QueryResult<GetUserGroupsQuery, GetUserGroupsQueryVariables>;
export const ListUsersDocument = gql`
    query listUsers($input: ListUsersInput!) {
        listUsers(input: $input) {
            start
            count
            total
            users {
                urn
                username
                isNativeUser
                info {
                    active
                    displayName
                    title
                    firstName
                    lastName
                    fullName
                    email
                }
                editableProperties {
                    displayName
                    pictureLink
                    teams
                    title
                    skills
                }
                status
                roles: relationships(input: { types: ["IsMemberOfRole"], direction: OUTGOING, start: 0 }) {
                    start
                    count
                    total
                    relationships {
                        entity {
                            ... on DataHubRole {
                                urn
                                type
                                name
                                relationships(input: { types: ["IsMemberOfRole"], direction: INCOMING }) {
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
 * __useListUsersQuery__
 *
 * To run a query within a React component, call `useListUsersQuery` and pass it any options that fit your needs.
 * When your component renders, `useListUsersQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListUsersQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useListUsersQuery(baseOptions: Apollo.QueryHookOptions<ListUsersQuery, ListUsersQueryVariables>) {
    return Apollo.useQuery<ListUsersQuery, ListUsersQueryVariables>(ListUsersDocument, baseOptions);
}
export function useListUsersLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListUsersQuery, ListUsersQueryVariables>,
) {
    return Apollo.useLazyQuery<ListUsersQuery, ListUsersQueryVariables>(ListUsersDocument, baseOptions);
}
export type ListUsersQueryHookResult = ReturnType<typeof useListUsersQuery>;
export type ListUsersLazyQueryHookResult = ReturnType<typeof useListUsersLazyQuery>;
export type ListUsersQueryResult = Apollo.QueryResult<ListUsersQuery, ListUsersQueryVariables>;
export const RemoveUserDocument = gql`
    mutation removeUser($urn: String!) {
        removeUser(urn: $urn)
    }
`;
export type RemoveUserMutationFn = Apollo.MutationFunction<RemoveUserMutation, RemoveUserMutationVariables>;

/**
 * __useRemoveUserMutation__
 *
 * To run a mutation, you first call `useRemoveUserMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useRemoveUserMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [removeUserMutation, { data, loading, error }] = useRemoveUserMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useRemoveUserMutation(
    baseOptions?: Apollo.MutationHookOptions<RemoveUserMutation, RemoveUserMutationVariables>,
) {
    return Apollo.useMutation<RemoveUserMutation, RemoveUserMutationVariables>(RemoveUserDocument, baseOptions);
}
export type RemoveUserMutationHookResult = ReturnType<typeof useRemoveUserMutation>;
export type RemoveUserMutationResult = Apollo.MutationResult<RemoveUserMutation>;
export type RemoveUserMutationOptions = Apollo.BaseMutationOptions<RemoveUserMutation, RemoveUserMutationVariables>;
export const UpdateUserStatusDocument = gql`
    mutation updateUserStatus($urn: String!, $status: CorpUserStatus!) {
        updateUserStatus(urn: $urn, status: $status)
    }
`;
export type UpdateUserStatusMutationFn = Apollo.MutationFunction<
    UpdateUserStatusMutation,
    UpdateUserStatusMutationVariables
>;

/**
 * __useUpdateUserStatusMutation__
 *
 * To run a mutation, you first call `useUpdateUserStatusMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateUserStatusMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateUserStatusMutation, { data, loading, error }] = useUpdateUserStatusMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      status: // value for 'status'
 *   },
 * });
 */
export function useUpdateUserStatusMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateUserStatusMutation, UpdateUserStatusMutationVariables>,
) {
    return Apollo.useMutation<UpdateUserStatusMutation, UpdateUserStatusMutationVariables>(
        UpdateUserStatusDocument,
        baseOptions,
    );
}
export type UpdateUserStatusMutationHookResult = ReturnType<typeof useUpdateUserStatusMutation>;
export type UpdateUserStatusMutationResult = Apollo.MutationResult<UpdateUserStatusMutation>;
export type UpdateUserStatusMutationOptions = Apollo.BaseMutationOptions<
    UpdateUserStatusMutation,
    UpdateUserStatusMutationVariables
>;
export const UpdateCorpUserPropertiesDocument = gql`
    mutation updateCorpUserProperties($urn: String!, $input: CorpUserUpdateInput!) {
        updateCorpUserProperties(urn: $urn, input: $input) {
            urn
        }
    }
`;
export type UpdateCorpUserPropertiesMutationFn = Apollo.MutationFunction<
    UpdateCorpUserPropertiesMutation,
    UpdateCorpUserPropertiesMutationVariables
>;

/**
 * __useUpdateCorpUserPropertiesMutation__
 *
 * To run a mutation, you first call `useUpdateCorpUserPropertiesMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateCorpUserPropertiesMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateCorpUserPropertiesMutation, { data, loading, error }] = useUpdateCorpUserPropertiesMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateCorpUserPropertiesMutation(
    baseOptions?: Apollo.MutationHookOptions<
        UpdateCorpUserPropertiesMutation,
        UpdateCorpUserPropertiesMutationVariables
    >,
) {
    return Apollo.useMutation<UpdateCorpUserPropertiesMutation, UpdateCorpUserPropertiesMutationVariables>(
        UpdateCorpUserPropertiesDocument,
        baseOptions,
    );
}
export type UpdateCorpUserPropertiesMutationHookResult = ReturnType<typeof useUpdateCorpUserPropertiesMutation>;
export type UpdateCorpUserPropertiesMutationResult = Apollo.MutationResult<UpdateCorpUserPropertiesMutation>;
export type UpdateCorpUserPropertiesMutationOptions = Apollo.BaseMutationOptions<
    UpdateCorpUserPropertiesMutation,
    UpdateCorpUserPropertiesMutationVariables
>;
export const CreateNativeUserResetTokenDocument = gql`
    mutation createNativeUserResetToken($input: CreateNativeUserResetTokenInput!) {
        createNativeUserResetToken(input: $input) {
            resetToken
        }
    }
`;
export type CreateNativeUserResetTokenMutationFn = Apollo.MutationFunction<
    CreateNativeUserResetTokenMutation,
    CreateNativeUserResetTokenMutationVariables
>;

/**
 * __useCreateNativeUserResetTokenMutation__
 *
 * To run a mutation, you first call `useCreateNativeUserResetTokenMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateNativeUserResetTokenMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createNativeUserResetTokenMutation, { data, loading, error }] = useCreateNativeUserResetTokenMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateNativeUserResetTokenMutation(
    baseOptions?: Apollo.MutationHookOptions<
        CreateNativeUserResetTokenMutation,
        CreateNativeUserResetTokenMutationVariables
    >,
) {
    return Apollo.useMutation<CreateNativeUserResetTokenMutation, CreateNativeUserResetTokenMutationVariables>(
        CreateNativeUserResetTokenDocument,
        baseOptions,
    );
}
export type CreateNativeUserResetTokenMutationHookResult = ReturnType<typeof useCreateNativeUserResetTokenMutation>;
export type CreateNativeUserResetTokenMutationResult = Apollo.MutationResult<CreateNativeUserResetTokenMutation>;
export type CreateNativeUserResetTokenMutationOptions = Apollo.BaseMutationOptions<
    CreateNativeUserResetTokenMutation,
    CreateNativeUserResetTokenMutationVariables
>;
export const UpdateCorpUserViewsSettingsDocument = gql`
    mutation updateCorpUserViewsSettings($input: UpdateCorpUserViewsSettingsInput!) {
        updateCorpUserViewsSettings(input: $input)
    }
`;
export type UpdateCorpUserViewsSettingsMutationFn = Apollo.MutationFunction<
    UpdateCorpUserViewsSettingsMutation,
    UpdateCorpUserViewsSettingsMutationVariables
>;

/**
 * __useUpdateCorpUserViewsSettingsMutation__
 *
 * To run a mutation, you first call `useUpdateCorpUserViewsSettingsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateCorpUserViewsSettingsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateCorpUserViewsSettingsMutation, { data, loading, error }] = useUpdateCorpUserViewsSettingsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateCorpUserViewsSettingsMutation(
    baseOptions?: Apollo.MutationHookOptions<
        UpdateCorpUserViewsSettingsMutation,
        UpdateCorpUserViewsSettingsMutationVariables
    >,
) {
    return Apollo.useMutation<UpdateCorpUserViewsSettingsMutation, UpdateCorpUserViewsSettingsMutationVariables>(
        UpdateCorpUserViewsSettingsDocument,
        baseOptions,
    );
}
export type UpdateCorpUserViewsSettingsMutationHookResult = ReturnType<typeof useUpdateCorpUserViewsSettingsMutation>;
export type UpdateCorpUserViewsSettingsMutationResult = Apollo.MutationResult<UpdateCorpUserViewsSettingsMutation>;
export type UpdateCorpUserViewsSettingsMutationOptions = Apollo.BaseMutationOptions<
    UpdateCorpUserViewsSettingsMutation,
    UpdateCorpUserViewsSettingsMutationVariables
>;
