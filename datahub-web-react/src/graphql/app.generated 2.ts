/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type AppConfigQueryVariables = Types.Exact<{ [key: string]: never }>;

export type AppConfigQuery = { __typename?: 'Query' } & {
    appConfig?: Types.Maybe<
        { __typename?: 'AppConfig' } & Pick<Types.AppConfig, 'appVersion'> & {
                policiesConfig: { __typename?: 'PoliciesConfig' } & Pick<Types.PoliciesConfig, 'enabled'> & {
                        platformPrivileges: Array<
                            { __typename?: 'Privilege' } & Pick<Types.Privilege, 'type' | 'displayName' | 'description'>
                        >;
                        resourcePrivileges: Array<
                            { __typename?: 'ResourcePrivileges' } & Pick<
                                Types.ResourcePrivileges,
                                'resourceType' | 'resourceTypeDisplayName' | 'entityType'
                            > & {
                                    privileges: Array<
                                        { __typename?: 'Privilege' } & Pick<
                                            Types.Privilege,
                                            'type' | 'displayName' | 'description'
                                        >
                                    >;
                                }
                        >;
                    };
                analyticsConfig: { __typename?: 'AnalyticsConfig' } & Pick<Types.AnalyticsConfig, 'enabled'>;
                authConfig: { __typename?: 'AuthConfig' } & Pick<Types.AuthConfig, 'tokenAuthEnabled'>;
                identityManagementConfig: { __typename?: 'IdentityManagementConfig' } & Pick<
                    Types.IdentityManagementConfig,
                    'enabled'
                >;
                lineageConfig: { __typename?: 'LineageConfig' } & Pick<Types.LineageConfig, 'supportsImpactAnalysis'>;
                managedIngestionConfig: { __typename?: 'ManagedIngestionConfig' } & Pick<
                    Types.ManagedIngestionConfig,
                    'enabled'
                >;
                visualConfig: { __typename?: 'VisualConfig' } & Pick<Types.VisualConfig, 'logoUrl' | 'faviconUrl'> & {
                        queriesTab?: Types.Maybe<
                            { __typename?: 'QueriesTabConfig' } & Pick<Types.QueriesTabConfig, 'queriesTabResultSize'>
                        >;
                    };
                telemetryConfig: { __typename?: 'TelemetryConfig' } & Pick<
                    Types.TelemetryConfig,
                    'enableThirdPartyLogging'
                >;
                testsConfig: { __typename?: 'TestsConfig' } & Pick<Types.TestsConfig, 'enabled'>;
                viewsConfig: { __typename?: 'ViewsConfig' } & Pick<Types.ViewsConfig, 'enabled'>;
            }
    >;
};

export type GetEntityCountsQueryVariables = Types.Exact<{
    input?: Types.Maybe<Types.EntityCountInput>;
}>;

export type GetEntityCountsQuery = { __typename?: 'Query' } & {
    getEntityCounts?: Types.Maybe<
        { __typename?: 'EntityCountResults' } & {
            counts?: Types.Maybe<
                Array<{ __typename?: 'EntityCountResult' } & Pick<Types.EntityCountResult, 'entityType' | 'count'>>
            >;
        }
    >;
};

export type GetGlobalViewsSettingsQueryVariables = Types.Exact<{ [key: string]: never }>;

export type GetGlobalViewsSettingsQuery = { __typename?: 'Query' } & {
    globalViewsSettings?: Types.Maybe<
        { __typename?: 'GlobalViewsSettings' } & Pick<Types.GlobalViewsSettings, 'defaultView'>
    >;
};

export type UpdateGlobalViewsSettingsMutationVariables = Types.Exact<{
    input: Types.UpdateGlobalViewsSettingsInput;
}>;

export type UpdateGlobalViewsSettingsMutation = { __typename?: 'Mutation' } & Pick<
    Types.Mutation,
    'updateGlobalViewsSettings'
>;

export const AppConfigDocument = gql`
    query appConfig {
        appConfig {
            appVersion
            policiesConfig {
                enabled
                platformPrivileges {
                    type
                    displayName
                    description
                }
                resourcePrivileges {
                    resourceType
                    resourceTypeDisplayName
                    entityType
                    privileges {
                        type
                        displayName
                        description
                    }
                }
            }
            analyticsConfig {
                enabled
            }
            authConfig {
                tokenAuthEnabled
            }
            identityManagementConfig {
                enabled
            }
            lineageConfig {
                supportsImpactAnalysis
            }
            managedIngestionConfig {
                enabled
            }
            visualConfig {
                logoUrl
                faviconUrl
                queriesTab {
                    queriesTabResultSize
                }
            }
            telemetryConfig {
                enableThirdPartyLogging
            }
            testsConfig {
                enabled
            }
            viewsConfig {
                enabled
            }
        }
    }
`;

/**
 * __useAppConfigQuery__
 *
 * To run a query within a React component, call `useAppConfigQuery` and pass it any options that fit your needs.
 * When your component renders, `useAppConfigQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useAppConfigQuery({
 *   variables: {
 *   },
 * });
 */
export function useAppConfigQuery(baseOptions?: Apollo.QueryHookOptions<AppConfigQuery, AppConfigQueryVariables>) {
    return Apollo.useQuery<AppConfigQuery, AppConfigQueryVariables>(AppConfigDocument, baseOptions);
}
export function useAppConfigLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<AppConfigQuery, AppConfigQueryVariables>,
) {
    return Apollo.useLazyQuery<AppConfigQuery, AppConfigQueryVariables>(AppConfigDocument, baseOptions);
}
export type AppConfigQueryHookResult = ReturnType<typeof useAppConfigQuery>;
export type AppConfigLazyQueryHookResult = ReturnType<typeof useAppConfigLazyQuery>;
export type AppConfigQueryResult = Apollo.QueryResult<AppConfigQuery, AppConfigQueryVariables>;
export const GetEntityCountsDocument = gql`
    query getEntityCounts($input: EntityCountInput) {
        getEntityCounts(input: $input) {
            counts {
                entityType
                count
            }
        }
    }
`;

/**
 * __useGetEntityCountsQuery__
 *
 * To run a query within a React component, call `useGetEntityCountsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetEntityCountsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetEntityCountsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetEntityCountsQuery(
    baseOptions?: Apollo.QueryHookOptions<GetEntityCountsQuery, GetEntityCountsQueryVariables>,
) {
    return Apollo.useQuery<GetEntityCountsQuery, GetEntityCountsQueryVariables>(GetEntityCountsDocument, baseOptions);
}
export function useGetEntityCountsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetEntityCountsQuery, GetEntityCountsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetEntityCountsQuery, GetEntityCountsQueryVariables>(
        GetEntityCountsDocument,
        baseOptions,
    );
}
export type GetEntityCountsQueryHookResult = ReturnType<typeof useGetEntityCountsQuery>;
export type GetEntityCountsLazyQueryHookResult = ReturnType<typeof useGetEntityCountsLazyQuery>;
export type GetEntityCountsQueryResult = Apollo.QueryResult<GetEntityCountsQuery, GetEntityCountsQueryVariables>;
export const GetGlobalViewsSettingsDocument = gql`
    query getGlobalViewsSettings {
        globalViewsSettings {
            defaultView
        }
    }
`;

/**
 * __useGetGlobalViewsSettingsQuery__
 *
 * To run a query within a React component, call `useGetGlobalViewsSettingsQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetGlobalViewsSettingsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetGlobalViewsSettingsQuery({
 *   variables: {
 *   },
 * });
 */
export function useGetGlobalViewsSettingsQuery(
    baseOptions?: Apollo.QueryHookOptions<GetGlobalViewsSettingsQuery, GetGlobalViewsSettingsQueryVariables>,
) {
    return Apollo.useQuery<GetGlobalViewsSettingsQuery, GetGlobalViewsSettingsQueryVariables>(
        GetGlobalViewsSettingsDocument,
        baseOptions,
    );
}
export function useGetGlobalViewsSettingsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetGlobalViewsSettingsQuery, GetGlobalViewsSettingsQueryVariables>,
) {
    return Apollo.useLazyQuery<GetGlobalViewsSettingsQuery, GetGlobalViewsSettingsQueryVariables>(
        GetGlobalViewsSettingsDocument,
        baseOptions,
    );
}
export type GetGlobalViewsSettingsQueryHookResult = ReturnType<typeof useGetGlobalViewsSettingsQuery>;
export type GetGlobalViewsSettingsLazyQueryHookResult = ReturnType<typeof useGetGlobalViewsSettingsLazyQuery>;
export type GetGlobalViewsSettingsQueryResult = Apollo.QueryResult<
    GetGlobalViewsSettingsQuery,
    GetGlobalViewsSettingsQueryVariables
>;
export const UpdateGlobalViewsSettingsDocument = gql`
    mutation updateGlobalViewsSettings($input: UpdateGlobalViewsSettingsInput!) {
        updateGlobalViewsSettings(input: $input)
    }
`;
export type UpdateGlobalViewsSettingsMutationFn = Apollo.MutationFunction<
    UpdateGlobalViewsSettingsMutation,
    UpdateGlobalViewsSettingsMutationVariables
>;

/**
 * __useUpdateGlobalViewsSettingsMutation__
 *
 * To run a mutation, you first call `useUpdateGlobalViewsSettingsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateGlobalViewsSettingsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateGlobalViewsSettingsMutation, { data, loading, error }] = useUpdateGlobalViewsSettingsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateGlobalViewsSettingsMutation(
    baseOptions?: Apollo.MutationHookOptions<
        UpdateGlobalViewsSettingsMutation,
        UpdateGlobalViewsSettingsMutationVariables
    >,
) {
    return Apollo.useMutation<UpdateGlobalViewsSettingsMutation, UpdateGlobalViewsSettingsMutationVariables>(
        UpdateGlobalViewsSettingsDocument,
        baseOptions,
    );
}
export type UpdateGlobalViewsSettingsMutationHookResult = ReturnType<typeof useUpdateGlobalViewsSettingsMutation>;
export type UpdateGlobalViewsSettingsMutationResult = Apollo.MutationResult<UpdateGlobalViewsSettingsMutation>;
export type UpdateGlobalViewsSettingsMutationOptions = Apollo.BaseMutationOptions<
    UpdateGlobalViewsSettingsMutation,
    UpdateGlobalViewsSettingsMutationVariables
>;
