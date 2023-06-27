/* eslint-disable */
import * as Types from '../types.generated';

import {
    PlatformFieldsFragment,
    OwnershipFieldsFragment,
    GlobalTagsFieldsFragment,
    GlossaryTermsFragment,
    EntityDomainFragment,
    NonRecursiveDataFlowFieldsFragment,
    InstitutionalMemoryFieldsFragment,
    DeprecationFieldsFragment,
    EmbedFieldsFragment,
    DataPlatformInstanceFieldsFragment,
    ParentContainersFieldsFragment,
    InputFieldsFieldsFragment,
    EntityContainerFragment,
    ParentNodesFieldsFragment,
    GlossaryNodeFragment,
    NonRecursiveMlFeatureTableFragment,
    NonRecursiveMlFeatureFragment,
    NonRecursiveMlPrimaryKeyFragment,
    SchemaMetadataFieldsFragment,
    NonConflictingPlatformFieldsFragment,
} from './fragments.generated';
import { FullRelationshipResultsFragment } from './relationships.generated';
import { gql } from '@apollo/client';
import {
    PlatformFieldsFragmentDoc,
    OwnershipFieldsFragmentDoc,
    GlobalTagsFieldsFragmentDoc,
    GlossaryTermsFragmentDoc,
    EntityDomainFragmentDoc,
    NonRecursiveDataFlowFieldsFragmentDoc,
    InstitutionalMemoryFieldsFragmentDoc,
    DeprecationFieldsFragmentDoc,
    EmbedFieldsFragmentDoc,
    DataPlatformInstanceFieldsFragmentDoc,
    ParentContainersFieldsFragmentDoc,
    InputFieldsFieldsFragmentDoc,
    EntityContainerFragmentDoc,
    ParentNodesFieldsFragmentDoc,
    GlossaryNodeFragmentDoc,
    NonRecursiveMlFeatureTableFragmentDoc,
    NonRecursiveMlFeatureFragmentDoc,
    NonRecursiveMlPrimaryKeyFragmentDoc,
    SchemaMetadataFieldsFragmentDoc,
    NonConflictingPlatformFieldsFragmentDoc,
} from './fragments.generated';
import { FullRelationshipResultsFragmentDoc } from './relationships.generated';
import * as Apollo from '@apollo/client';
export type GetChartQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetChartQuery = { __typename?: 'Query' } & {
    chart?: Types.Maybe<
        { __typename?: 'Chart' } & Pick<
            Types.Chart,
            'urn' | 'type' | 'exists' | 'lastIngested' | 'tool' | 'chartId'
        > & {
                platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                properties?: Types.Maybe<
                    { __typename?: 'ChartProperties' } & Pick<
                        Types.ChartProperties,
                        'name' | 'description' | 'externalUrl' | 'type' | 'access' | 'lastRefreshed'
                    > & {
                            lastModified: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'>;
                            created: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'time'>;
                            customProperties?: Types.Maybe<
                                Array<
                                    { __typename?: 'CustomPropertiesEntry' } & Pick<
                                        Types.CustomPropertiesEntry,
                                        'key' | 'value'
                                    >
                                >
                            >;
                        }
                >;
                query?: Types.Maybe<{ __typename?: 'ChartQuery' } & Pick<Types.ChartQuery, 'rawQuery' | 'type'>>;
                ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                editableProperties?: Types.Maybe<
                    { __typename?: 'ChartEditableProperties' } & Pick<Types.ChartEditableProperties, 'description'>
                >;
                institutionalMemory?: Types.Maybe<
                    { __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment
                >;
                glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
                domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
                deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
                embed?: Types.Maybe<{ __typename?: 'Embed' } & EmbedFieldsFragment>;
                inputs?: Types.Maybe<{ __typename?: 'EntityRelationshipsResult' } & FullRelationshipResultsFragment>;
                dashboards?: Types.Maybe<
                    { __typename?: 'EntityRelationshipsResult' } & FullRelationshipResultsFragment
                >;
                parentContainers?: Types.Maybe<
                    { __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment
                >;
                status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
                dataPlatformInstance?: Types.Maybe<
                    { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
                >;
                statsSummary?: Types.Maybe<
                    { __typename?: 'ChartStatsSummary' } & Pick<
                        Types.ChartStatsSummary,
                        'viewCount' | 'uniqueUserCountLast30Days'
                    > & {
                            topUsersLast30Days?: Types.Maybe<
                                Array<
                                    { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type' | 'username'> & {
                                            properties?: Types.Maybe<
                                                { __typename?: 'CorpUserProperties' } & Pick<
                                                    Types.CorpUserProperties,
                                                    'displayName' | 'firstName' | 'lastName' | 'fullName'
                                                >
                                            >;
                                            editableProperties?: Types.Maybe<
                                                { __typename?: 'CorpUserEditableProperties' } & Pick<
                                                    Types.CorpUserEditableProperties,
                                                    'displayName' | 'pictureLink'
                                                >
                                            >;
                                        }
                                >
                            >;
                        }
                >;
                inputFields?: Types.Maybe<{ __typename?: 'InputFields' } & InputFieldsFieldsFragment>;
                privileges?: Types.Maybe<
                    { __typename?: 'EntityPrivileges' } & Pick<
                        Types.EntityPrivileges,
                        'canEditLineage' | 'canEditEmbed'
                    >
                >;
            }
    >;
};

export type UpdateChartMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    input: Types.ChartUpdateInput;
}>;

export type UpdateChartMutation = { __typename?: 'Mutation' } & {
    updateChart?: Types.Maybe<{ __typename?: 'Chart' } & Pick<Types.Chart, 'urn'>>;
};

export const GetChartDocument = gql`
    query getChart($urn: String!) {
        chart(urn: $urn) {
            urn
            type
            exists
            lastIngested
            tool
            chartId
            platform {
                ...platformFields
            }
            properties {
                name
                description
                externalUrl
                type
                access
                lastRefreshed
                lastModified {
                    time
                }
                created {
                    time
                }
                customProperties {
                    key
                    value
                }
            }
            query {
                rawQuery
                type
            }
            ownership {
                ...ownershipFields
            }
            globalTags {
                ...globalTagsFields
            }
            editableProperties {
                description
            }
            institutionalMemory {
                ...institutionalMemoryFields
            }
            glossaryTerms {
                ...glossaryTerms
            }
            domain {
                ...entityDomain
            }
            deprecation {
                ...deprecationFields
            }
            embed {
                ...embedFields
            }
            inputs: relationships(input: { types: ["Consumes"], direction: OUTGOING, start: 0, count: 100 }) {
                ...fullRelationshipResults
            }
            dashboards: relationships(input: { types: ["Contains"], direction: INCOMING, start: 0, count: 100 }) {
                ...fullRelationshipResults
            }
            parentContainers {
                ...parentContainersFields
            }
            status {
                removed
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            statsSummary {
                viewCount
                uniqueUserCountLast30Days
                topUsersLast30Days {
                    urn
                    type
                    username
                    properties {
                        displayName
                        firstName
                        lastName
                        fullName
                    }
                    editableProperties {
                        displayName
                        pictureLink
                    }
                }
            }
            inputFields {
                ...inputFieldsFields
            }
            privileges {
                canEditLineage
                canEditEmbed
            }
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${EmbedFieldsFragmentDoc}
    ${FullRelationshipResultsFragmentDoc}
    ${ParentContainersFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
    ${InputFieldsFieldsFragmentDoc}
`;

/**
 * __useGetChartQuery__
 *
 * To run a query within a React component, call `useGetChartQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetChartQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetChartQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetChartQuery(baseOptions: Apollo.QueryHookOptions<GetChartQuery, GetChartQueryVariables>) {
    return Apollo.useQuery<GetChartQuery, GetChartQueryVariables>(GetChartDocument, baseOptions);
}
export function useGetChartLazyQuery(baseOptions?: Apollo.LazyQueryHookOptions<GetChartQuery, GetChartQueryVariables>) {
    return Apollo.useLazyQuery<GetChartQuery, GetChartQueryVariables>(GetChartDocument, baseOptions);
}
export type GetChartQueryHookResult = ReturnType<typeof useGetChartQuery>;
export type GetChartLazyQueryHookResult = ReturnType<typeof useGetChartLazyQuery>;
export type GetChartQueryResult = Apollo.QueryResult<GetChartQuery, GetChartQueryVariables>;
export const UpdateChartDocument = gql`
    mutation updateChart($urn: String!, $input: ChartUpdateInput!) {
        updateChart(urn: $urn, input: $input) {
            urn
        }
    }
`;
export type UpdateChartMutationFn = Apollo.MutationFunction<UpdateChartMutation, UpdateChartMutationVariables>;

/**
 * __useUpdateChartMutation__
 *
 * To run a mutation, you first call `useUpdateChartMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateChartMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateChartMutation, { data, loading, error }] = useUpdateChartMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateChartMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateChartMutation, UpdateChartMutationVariables>,
) {
    return Apollo.useMutation<UpdateChartMutation, UpdateChartMutationVariables>(UpdateChartDocument, baseOptions);
}
export type UpdateChartMutationHookResult = ReturnType<typeof useUpdateChartMutation>;
export type UpdateChartMutationResult = Apollo.MutationResult<UpdateChartMutation>;
export type UpdateChartMutationOptions = Apollo.BaseMutationOptions<UpdateChartMutation, UpdateChartMutationVariables>;
