/* eslint-disable */
import * as Types from '../types.generated';

import { OwnershipFieldsFragment, ParentNodesFieldsFragment, GlossaryNodeFragment } from './fragments.generated';
import { gql } from '@apollo/client';
import {
    OwnershipFieldsFragmentDoc,
    ParentNodesFieldsFragmentDoc,
    GlossaryNodeFragmentDoc,
} from './fragments.generated';
import * as Apollo from '@apollo/client';
export type ChildGlossaryTermFragment = { __typename?: 'GlossaryTerm' } & Pick<
    Types.GlossaryTerm,
    'urn' | 'type' | 'name' | 'hierarchicalName'
> & {
        properties?: Types.Maybe<
            { __typename?: 'GlossaryTermProperties' } & Pick<Types.GlossaryTermProperties, 'name'>
        >;
    };

export type GetGlossaryNodeQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetGlossaryNodeQuery = { __typename?: 'Query' } & {
    glossaryNode?: Types.Maybe<
        { __typename?: 'GlossaryNode' } & Pick<Types.GlossaryNode, 'urn' | 'type' | 'exists'> & {
                properties?: Types.Maybe<
                    { __typename?: 'GlossaryNodeProperties' } & Pick<
                        Types.GlossaryNodeProperties,
                        'name' | 'description'
                    >
                >;
                ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                parentNodes?: Types.Maybe<{ __typename?: 'ParentNodesResult' } & ParentNodesFieldsFragment>;
                privileges?: Types.Maybe<
                    { __typename?: 'EntityPrivileges' } & Pick<
                        Types.EntityPrivileges,
                        'canManageEntity' | 'canManageChildren'
                    >
                >;
                children?: Types.Maybe<
                    { __typename?: 'EntityRelationshipsResult' } & Pick<Types.EntityRelationshipsResult, 'total'> & {
                            relationships: Array<
                                { __typename?: 'EntityRelationship' } & Pick<Types.EntityRelationship, 'direction'> & {
                                        entity?: Types.Maybe<
                                            | ({ __typename?: 'AccessTokenMetadata' } & Pick<
                                                  Types.AccessTokenMetadata,
                                                  'type' | 'urn'
                                              >)
                                            | ({ __typename?: 'Assertion' } & Pick<Types.Assertion, 'type' | 'urn'>)
                                            | ({ __typename?: 'Chart' } & Pick<Types.Chart, 'type' | 'urn'>)
                                            | ({ __typename?: 'Container' } & Pick<Types.Container, 'type' | 'urn'>)
                                            | ({ __typename?: 'CorpGroup' } & Pick<Types.CorpGroup, 'type' | 'urn'>)
                                            | ({ __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'type' | 'urn'>)
                                            | ({ __typename?: 'Dashboard' } & Pick<Types.Dashboard, 'type' | 'urn'>)
                                            | ({ __typename?: 'DataFlow' } & Pick<Types.DataFlow, 'type' | 'urn'>)
                                            | ({ __typename?: 'DataHubPolicy' } & Pick<
                                                  Types.DataHubPolicy,
                                                  'type' | 'urn'
                                              >)
                                            | ({ __typename?: 'DataHubRole' } & Pick<Types.DataHubRole, 'type' | 'urn'>)
                                            | ({ __typename?: 'DataHubView' } & Pick<Types.DataHubView, 'type' | 'urn'>)
                                            | ({ __typename?: 'DataJob' } & Pick<Types.DataJob, 'type' | 'urn'>)
                                            | ({ __typename?: 'DataPlatform' } & Pick<
                                                  Types.DataPlatform,
                                                  'type' | 'urn'
                                              >)
                                            | ({ __typename?: 'DataPlatformInstance' } & Pick<
                                                  Types.DataPlatformInstance,
                                                  'type' | 'urn'
                                              >)
                                            | ({ __typename?: 'DataProcessInstance' } & Pick<
                                                  Types.DataProcessInstance,
                                                  'type' | 'urn'
                                              >)
                                            | ({ __typename?: 'Dataset' } & Pick<Types.Dataset, 'type' | 'urn'>)
                                            | ({ __typename?: 'Domain' } & Pick<Types.Domain, 'type' | 'urn'>)
                                            | ({ __typename?: 'GlossaryNode' } & Pick<
                                                  Types.GlossaryNode,
                                                  'type' | 'urn'
                                              > &
                                                  GlossaryNodeFragment)
                                            | ({ __typename?: 'GlossaryTerm' } & Pick<
                                                  Types.GlossaryTerm,
                                                  'type' | 'urn'
                                              > &
                                                  ChildGlossaryTermFragment)
                                            | ({ __typename?: 'MLFeature' } & Pick<Types.MlFeature, 'type' | 'urn'>)
                                            | ({ __typename?: 'MLFeatureTable' } & Pick<
                                                  Types.MlFeatureTable,
                                                  'type' | 'urn'
                                              >)
                                            | ({ __typename?: 'MLModel' } & Pick<Types.MlModel, 'type' | 'urn'>)
                                            | ({ __typename?: 'MLModelGroup' } & Pick<
                                                  Types.MlModelGroup,
                                                  'type' | 'urn'
                                              >)
                                            | ({ __typename?: 'MLPrimaryKey' } & Pick<
                                                  Types.MlPrimaryKey,
                                                  'type' | 'urn'
                                              >)
                                            | ({ __typename?: 'Notebook' } & Pick<Types.Notebook, 'type' | 'urn'>)
                                            | ({ __typename?: 'Post' } & Pick<Types.Post, 'type' | 'urn'>)
                                            | ({ __typename?: 'QueryEntity' } & Pick<Types.QueryEntity, 'type' | 'urn'>)
                                            | ({ __typename?: 'SchemaFieldEntity' } & Pick<
                                                  Types.SchemaFieldEntity,
                                                  'type' | 'urn'
                                              >)
                                            | ({ __typename?: 'Tag' } & Pick<Types.Tag, 'type' | 'urn'>)
                                            | ({ __typename?: 'Test' } & Pick<Types.Test, 'type' | 'urn'>)
                                            | ({ __typename?: 'VersionedDataset' } & Pick<
                                                  Types.VersionedDataset,
                                                  'type' | 'urn'
                                              >)
                                        >;
                                    }
                            >;
                        }
                >;
            }
    >;
};

export const ChildGlossaryTermFragmentDoc = gql`
    fragment childGlossaryTerm on GlossaryTerm {
        urn
        type
        name
        hierarchicalName
        properties {
            name
        }
    }
`;
export const GetGlossaryNodeDocument = gql`
    query getGlossaryNode($urn: String!) {
        glossaryNode(urn: $urn) {
            urn
            type
            exists
            properties {
                name
                description
            }
            ownership {
                ...ownershipFields
            }
            parentNodes {
                ...parentNodesFields
            }
            privileges {
                canManageEntity
                canManageChildren
            }
            children: relationships(input: { types: ["IsPartOf"], direction: INCOMING, start: 0, count: 10000 }) {
                total
                relationships {
                    direction
                    entity {
                        type
                        urn
                        ... on GlossaryNode {
                            ...glossaryNode
                        }
                        ... on GlossaryTerm {
                            ...childGlossaryTerm
                        }
                    }
                }
            }
        }
    }
    ${OwnershipFieldsFragmentDoc}
    ${ParentNodesFieldsFragmentDoc}
    ${GlossaryNodeFragmentDoc}
    ${ChildGlossaryTermFragmentDoc}
`;

/**
 * __useGetGlossaryNodeQuery__
 *
 * To run a query within a React component, call `useGetGlossaryNodeQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetGlossaryNodeQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetGlossaryNodeQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetGlossaryNodeQuery(
    baseOptions: Apollo.QueryHookOptions<GetGlossaryNodeQuery, GetGlossaryNodeQueryVariables>,
) {
    return Apollo.useQuery<GetGlossaryNodeQuery, GetGlossaryNodeQueryVariables>(GetGlossaryNodeDocument, baseOptions);
}
export function useGetGlossaryNodeLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetGlossaryNodeQuery, GetGlossaryNodeQueryVariables>,
) {
    return Apollo.useLazyQuery<GetGlossaryNodeQuery, GetGlossaryNodeQueryVariables>(
        GetGlossaryNodeDocument,
        baseOptions,
    );
}
export type GetGlossaryNodeQueryHookResult = ReturnType<typeof useGetGlossaryNodeQuery>;
export type GetGlossaryNodeLazyQueryHookResult = ReturnType<typeof useGetGlossaryNodeLazyQuery>;
export type GetGlossaryNodeQueryResult = Apollo.QueryResult<GetGlossaryNodeQuery, GetGlossaryNodeQueryVariables>;
