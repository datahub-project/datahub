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
import * as Apollo from '@apollo/client';
export type GetQuickFiltersQueryVariables = Types.Exact<{
    input: Types.GetQuickFiltersInput;
}>;

export type GetQuickFiltersQuery = { __typename?: 'Query' } & {
    getQuickFilters?: Types.Maybe<
        { __typename?: 'GetQuickFiltersResult' } & {
            quickFilters: Array<
                Types.Maybe<
                    { __typename?: 'QuickFilter' } & Pick<Types.QuickFilter, 'field' | 'value'> & {
                            entity?: Types.Maybe<
                                | ({ __typename?: 'AccessTokenMetadata' } & Pick<
                                      Types.AccessTokenMetadata,
                                      'urn' | 'type'
                                  >)
                                | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'>)
                                | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'>)
                                | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'>)
                                | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'>)
                                | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'>)
                                | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'>)
                                | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'>)
                                | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'>)
                                | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'>)
                                | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'>)
                                | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'>)
                                | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'> &
                                      PlatformFieldsFragment)
                                | ({ __typename?: 'DataPlatformInstance' } & Pick<
                                      Types.DataPlatformInstance,
                                      'urn' | 'type'
                                  >)
                                | ({ __typename?: 'DataProcessInstance' } & Pick<
                                      Types.DataProcessInstance,
                                      'urn' | 'type'
                                  >)
                                | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'>)
                                | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'>)
                                | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'>)
                                | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'>)
                                | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'>)
                                | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'>)
                                | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'>)
                                | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'>)
                                | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'>)
                                | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'>)
                                | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'>)
                                | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'>)
                                | ({ __typename?: 'SchemaFieldEntity' } & Pick<Types.SchemaFieldEntity, 'urn' | 'type'>)
                                | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'>)
                                | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'>)
                                | ({ __typename?: 'VersionedDataset' } & Pick<Types.VersionedDataset, 'urn' | 'type'>)
                            >;
                        }
                >
            >;
        }
    >;
};

export const GetQuickFiltersDocument = gql`
    query getQuickFilters($input: GetQuickFiltersInput!) {
        getQuickFilters(input: $input) {
            quickFilters {
                field
                value
                entity {
                    urn
                    type
                    ... on DataPlatform {
                        ...platformFields
                    }
                }
            }
        }
    }
    ${PlatformFieldsFragmentDoc}
`;

/**
 * __useGetQuickFiltersQuery__
 *
 * To run a query within a React component, call `useGetQuickFiltersQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetQuickFiltersQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetQuickFiltersQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetQuickFiltersQuery(
    baseOptions: Apollo.QueryHookOptions<GetQuickFiltersQuery, GetQuickFiltersQueryVariables>,
) {
    return Apollo.useQuery<GetQuickFiltersQuery, GetQuickFiltersQueryVariables>(GetQuickFiltersDocument, baseOptions);
}
export function useGetQuickFiltersLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetQuickFiltersQuery, GetQuickFiltersQueryVariables>,
) {
    return Apollo.useLazyQuery<GetQuickFiltersQuery, GetQuickFiltersQueryVariables>(
        GetQuickFiltersDocument,
        baseOptions,
    );
}
export type GetQuickFiltersQueryHookResult = ReturnType<typeof useGetQuickFiltersQuery>;
export type GetQuickFiltersLazyQueryHookResult = ReturnType<typeof useGetQuickFiltersLazyQuery>;
export type GetQuickFiltersQueryResult = Apollo.QueryResult<GetQuickFiltersQuery, GetQuickFiltersQueryVariables>;
