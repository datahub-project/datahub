/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type GetMeQueryVariables = Types.Exact<{ [key: string]: never }>;

export type GetMeQuery = { __typename?: 'Query' } & {
    me?: Types.Maybe<
        { __typename?: 'AuthenticatedUser' } & {
            corpUser: { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'username'> & {
                    info?: Types.Maybe<
                        { __typename?: 'CorpUserInfo' } & Pick<
                            Types.CorpUserInfo,
                            'active' | 'displayName' | 'title' | 'firstName' | 'lastName' | 'fullName' | 'email'
                        >
                    >;
                    editableProperties?: Types.Maybe<
                        { __typename?: 'CorpUserEditableProperties' } & Pick<
                            Types.CorpUserEditableProperties,
                            'displayName' | 'title' | 'pictureLink' | 'teams' | 'skills'
                        >
                    >;
                    settings?: Types.Maybe<
                        { __typename?: 'CorpUserSettings' } & {
                            appearance?: Types.Maybe<
                                { __typename?: 'CorpUserAppearanceSettings' } & Pick<
                                    Types.CorpUserAppearanceSettings,
                                    'showSimplifiedHomepage'
                                >
                            >;
                            views?: Types.Maybe<
                                { __typename?: 'CorpUserViewsSettings' } & {
                                    defaultView?: Types.Maybe<
                                        { __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn'>
                                    >;
                                }
                            >;
                        }
                    >;
                };
            platformPrivileges: { __typename?: 'PlatformPrivileges' } & Pick<
                Types.PlatformPrivileges,
                | 'viewAnalytics'
                | 'managePolicies'
                | 'manageIdentities'
                | 'generatePersonalAccessTokens'
                | 'manageIngestion'
                | 'manageSecrets'
                | 'manageDomains'
                | 'manageTests'
                | 'manageGlossaries'
                | 'manageUserCredentials'
                | 'manageTags'
                | 'createDomains'
                | 'createTags'
                | 'manageGlobalViews'
            >;
        }
    >;
};

export type UpdateUserSettingMutationVariables = Types.Exact<{
    input: Types.UpdateUserSettingInput;
}>;

export type UpdateUserSettingMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updateUserSetting'>;

export const GetMeDocument = gql`
    query getMe {
        me {
            corpUser {
                urn
                username
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
                    title
                    pictureLink
                    teams
                    skills
                }
                settings {
                    appearance {
                        showSimplifiedHomepage
                    }
                    views {
                        defaultView {
                            urn
                        }
                    }
                }
            }
            platformPrivileges {
                viewAnalytics
                managePolicies
                manageIdentities
                generatePersonalAccessTokens
                manageIngestion
                manageSecrets
                manageDomains
                manageTests
                manageGlossaries
                manageUserCredentials
                manageTags
                createDomains
                createTags
                manageGlobalViews
            }
        }
    }
`;

/**
 * __useGetMeQuery__
 *
 * To run a query within a React component, call `useGetMeQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetMeQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetMeQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetMeQuery(baseOptions?: Apollo.QueryHookOptions<GetMeQuery, GetMeQueryVariables>) {
    return Apollo.useQuery<GetMeQuery, GetMeQueryVariables>(GetMeDocument, baseOptions);
}
export function useGetMeLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetMeQuery, GetMeQueryVariables>) {
    return Apollo.useLazyQuery<GetMeQuery, GetMeQueryVariables>(GetMeDocument, baseOptions);
}
export type GetMeQueryHookResult = ReturnType<typeof useGetMeQuery>;
export type GetMeLazyQueryHookResult = ReturnType<typeof useGetMeLazyQuery>;
export type GetMeQueryResult = Apollo.QueryResult<GetMeQuery, GetMeQueryVariables>;
export const UpdateUserSettingDocument = gql`
    mutation updateUserSetting($input: UpdateUserSettingInput!) {
        updateUserSetting(input: $input)
    }
`;
export type UpdateUserSettingMutationFn = Apollo.MutationFunction<
    UpdateUserSettingMutation,
    UpdateUserSettingMutationVariables
>;

/**
 * __useUpdateUserSettingMutation__
 *
 * To run a mutation, you first call `useUpdateUserSettingMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateUserSettingMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateUserSettingMutation, { data, loading, error }] = useUpdateUserSettingMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateUserSettingMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateUserSettingMutation, UpdateUserSettingMutationVariables>,
) {
    return Apollo.useMutation<UpdateUserSettingMutation, UpdateUserSettingMutationVariables>(
        UpdateUserSettingDocument,
        baseOptions,
    );
}
export type UpdateUserSettingMutationHookResult = ReturnType<typeof useUpdateUserSettingMutation>;
export type UpdateUserSettingMutationResult = Apollo.MutationResult<UpdateUserSettingMutation>;
export type UpdateUserSettingMutationOptions = Apollo.BaseMutationOptions<
    UpdateUserSettingMutation,
    UpdateUserSettingMutationVariables
>;
