/* eslint-disable */
import * as Types from '../types.generated';

import {
    SearchResultFields_AccessTokenMetadata_Fragment,
    SearchResultFields_Assertion_Fragment,
    SearchResultFields_Chart_Fragment,
    SearchResultFields_Container_Fragment,
    SearchResultFields_CorpGroup_Fragment,
    SearchResultFields_CorpUser_Fragment,
    SearchResultFields_Dashboard_Fragment,
    SearchResultFields_DataFlow_Fragment,
    SearchResultFields_DataHubPolicy_Fragment,
    SearchResultFields_DataHubRole_Fragment,
    SearchResultFields_DataHubView_Fragment,
    SearchResultFields_DataJob_Fragment,
    SearchResultFields_DataPlatform_Fragment,
    SearchResultFields_DataPlatformInstance_Fragment,
    SearchResultFields_DataProcessInstance_Fragment,
    SearchResultFields_Dataset_Fragment,
    SearchResultFields_Domain_Fragment,
    SearchResultFields_GlossaryNode_Fragment,
    SearchResultFields_GlossaryTerm_Fragment,
    SearchResultFields_MlFeature_Fragment,
    SearchResultFields_MlFeatureTable_Fragment,
    SearchResultFields_MlModel_Fragment,
    SearchResultFields_MlModelGroup_Fragment,
    SearchResultFields_MlPrimaryKey_Fragment,
    SearchResultFields_Notebook_Fragment,
    SearchResultFields_Post_Fragment,
    SearchResultFields_QueryEntity_Fragment,
    SearchResultFields_SchemaFieldEntity_Fragment,
    SearchResultFields_Tag_Fragment,
    SearchResultFields_Test_Fragment,
    SearchResultFields_VersionedDataset_Fragment,
} from './search.generated';
import { gql } from '@apollo/client';
import { SearchResultFieldsFragmentDoc } from './search.generated';
import * as Apollo from '@apollo/client';
export type GetEntitiesQueryVariables = Types.Exact<{
    urns: Array<Types.Scalars['String']> | Types.Scalars['String'];
}>;

export type GetEntitiesQuery = { __typename?: 'Query' } & {
    entities?: Types.Maybe<
        Array<
            Types.Maybe<
                | ({ __typename?: 'AccessTokenMetadata' } & Pick<Types.AccessTokenMetadata, 'urn' | 'type'> &
                      SearchResultFields_AccessTokenMetadata_Fragment)
                | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'urn' | 'type'> &
                      SearchResultFields_Assertion_Fragment)
                | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'urn' | 'type'> & SearchResultFields_Chart_Fragment)
                | ({ __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'type'> &
                      SearchResultFields_Container_Fragment)
                | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'urn' | 'type'> &
                      SearchResultFields_CorpGroup_Fragment)
                | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'type'> &
                      SearchResultFields_CorpUser_Fragment)
                | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'urn' | 'type'> &
                      SearchResultFields_Dashboard_Fragment)
                | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'urn' | 'type'> &
                      SearchResultFields_DataFlow_Fragment)
                | ({ __typename?: 'DataHubPolicy' } & Pick<Types.DataHubPolicy, 'urn' | 'type'> &
                      SearchResultFields_DataHubPolicy_Fragment)
                | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'urn' | 'type'> &
                      SearchResultFields_DataHubRole_Fragment)
                | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'urn' | 'type'> &
                      SearchResultFields_DataHubView_Fragment)
                | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'urn' | 'type'> &
                      SearchResultFields_DataJob_Fragment)
                | ({ __typename?: 'DataPlatform' } & Pick<Types.DataPlatform, 'urn' | 'type'> &
                      SearchResultFields_DataPlatform_Fragment)
                | ({ __typename?: 'DataPlatformInstance' } & Pick<Types.DataPlatformInstance, 'urn' | 'type'> &
                      SearchResultFields_DataPlatformInstance_Fragment)
                | ({ __typename?: 'DataProcessInstance' } & Pick<Types.DataProcessInstance, 'urn' | 'type'> &
                      SearchResultFields_DataProcessInstance_Fragment)
                | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'urn' | 'type'> &
                      SearchResultFields_Dataset_Fragment)
                | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'type'> & SearchResultFields_Domain_Fragment)
                | ({ __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type'> &
                      SearchResultFields_GlossaryNode_Fragment)
                | ({ __typename?: 'GlossaryTerm' } & Pick<Types.GlossaryTerm, 'urn' | 'type'> &
                      SearchResultFields_GlossaryTerm_Fragment)
                | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'urn' | 'type'> &
                      SearchResultFields_MlFeature_Fragment)
                | ({ __typename?: 'MLFeatureTable' } & Pick<Types.MlFeatureTable, 'urn' | 'type'> &
                      SearchResultFields_MlFeatureTable_Fragment)
                | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'urn' | 'type'> &
                      SearchResultFields_MlModel_Fragment)
                | ({ __typename?: 'MLModelGroup' } & Pick<Types.MlModelGroup, 'urn' | 'type'> &
                      SearchResultFields_MlModelGroup_Fragment)
                | ({ __typename?: 'MLPrimaryKey' } & Pick<Types.MlPrimaryKey, 'urn' | 'type'> &
                      SearchResultFields_MlPrimaryKey_Fragment)
                | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'urn' | 'type'> &
                      SearchResultFields_Notebook_Fragment)
                | ({ __typename?: 'Post' } & Pick<Types.Post, 'urn' | 'type'> & SearchResultFields_Post_Fragment)
                | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'urn' | 'type'> &
                      SearchResultFields_QueryEntity_Fragment)
                | ({ __typename?: 'SchemaFieldEntity' } & Pick<Types.SchemaFieldEntity, 'urn' | 'type'> &
                      SearchResultFields_SchemaFieldEntity_Fragment)
                | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'urn' | 'type'> & SearchResultFields_Tag_Fragment)
                | ({ __typename?: 'Test' } & Pick<Types.Test, 'urn' | 'type'> & SearchResultFields_Test_Fragment)
                | ({ __typename?: 'VersionedDataset' } & Pick<Types.VersionedDataset, 'urn' | 'type'> &
                      SearchResultFields_VersionedDataset_Fragment)
            >
        >
    >;
};

export const GetEntitiesDocument = gql`
    query getEntities($urns: [String!]!) {
        entities(urns: $urns) {
            urn
            type
            ...searchResultFields
        }
    }
    ${SearchResultFieldsFragmentDoc}
`;

/**
 * __useGetEntitiesQuery__
 *
 * To run a query within a React component, call `useGetEntitiesQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetEntitiesQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetEntitiesQuery({
 *   variables: {
 *      urns: // value for 'urns'
 *   },
 * });
 */
export function useGetEntitiesQuery(baseOptions: Apollo.QueryHookOptions<GetEntitiesQuery, GetEntitiesQueryVariables>) {
    return Apollo.useQuery<GetEntitiesQuery, GetEntitiesQueryVariables>(GetEntitiesDocument, baseOptions);
}
export function useGetEntitiesLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetEntitiesQuery, GetEntitiesQueryVariables>,
) {
    return Apollo.useLazyQuery<GetEntitiesQuery, GetEntitiesQueryVariables>(GetEntitiesDocument, baseOptions);
}
export type GetEntitiesQueryHookResult = ReturnType<typeof useGetEntitiesQuery>;
export type GetEntitiesLazyQueryHookResult = ReturnType<typeof useGetEntitiesLazyQuery>;
export type GetEntitiesQueryResult = Apollo.QueryResult<GetEntitiesQuery, GetEntitiesQueryVariables>;
