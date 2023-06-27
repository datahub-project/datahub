/* eslint-disable */
import * as Types from '../types.generated';

import {
    NonRecursiveMlModelFragment,
    NonRecursiveMlFeatureFragment,
    NonRecursiveMlPrimaryKeyFragment,
} from './fragments.generated';
import { gql } from '@apollo/client';
import {
    NonRecursiveMlModelFragmentDoc,
    NonRecursiveMlFeatureFragmentDoc,
    NonRecursiveMlPrimaryKeyFragmentDoc,
} from './fragments.generated';
import * as Apollo from '@apollo/client';
export type GetMlModelQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetMlModelQuery = { __typename?: 'Query' } & {
    mlModel?: Types.Maybe<
        { __typename?: 'MLModel' } & {
            features?: Types.Maybe<
                { __typename?: 'EntityRelationshipsResult' } & Pick<
                    Types.EntityRelationshipsResult,
                    'start' | 'count' | 'total'
                > & {
                        relationships: Array<
                            { __typename?: 'EntityRelationship' } & Pick<
                                Types.EntityRelationship,
                                'type' | 'direction'
                            > & {
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
                                        | ({ __typename?: 'MLFeature' } & NonRecursiveMlFeatureFragment)
                                        | { __typename?: 'MLFeatureTable' }
                                        | { __typename?: 'MLModel' }
                                        | { __typename?: 'MLModelGroup' }
                                        | ({ __typename?: 'MLPrimaryKey' } & NonRecursiveMlPrimaryKeyFragment)
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
        } & NonRecursiveMlModelFragment
    >;
};

export const GetMlModelDocument = gql`
    query getMLModel($urn: String!) {
        mlModel(urn: $urn) {
            ...nonRecursiveMLModel
            features: relationships(input: { types: ["Consumes"], direction: OUTGOING, start: 0, count: 100 }) {
                start
                count
                total
                relationships {
                    type
                    direction
                    entity {
                        ... on MLFeature {
                            ...nonRecursiveMLFeature
                        }
                        ... on MLPrimaryKey {
                            ...nonRecursiveMLPrimaryKey
                        }
                    }
                }
            }
        }
    }
    ${NonRecursiveMlModelFragmentDoc}
    ${NonRecursiveMlFeatureFragmentDoc}
    ${NonRecursiveMlPrimaryKeyFragmentDoc}
`;

/**
 * __useGetMlModelQuery__
 *
 * To run a query within a React component, call `useGetMlModelQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetMlModelQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetMlModelQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetMlModelQuery(baseOptions: Apollo.QueryHookOptions<GetMlModelQuery, GetMlModelQueryVariables>) {
    return Apollo.useQuery<GetMlModelQuery, GetMlModelQueryVariables>(GetMlModelDocument, baseOptions);
}
export function useGetMlModelLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetMlModelQuery, GetMlModelQueryVariables>,
) {
    return Apollo.useLazyQuery<GetMlModelQuery, GetMlModelQueryVariables>(GetMlModelDocument, baseOptions);
}
export type GetMlModelQueryHookResult = ReturnType<typeof useGetMlModelQuery>;
export type GetMlModelLazyQueryHookResult = ReturnType<typeof useGetMlModelLazyQuery>;
export type GetMlModelQueryResult = Apollo.QueryResult<GetMlModelQuery, GetMlModelQueryVariables>;
