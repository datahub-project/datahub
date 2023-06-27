/* eslint-disable */
import * as Types from '../types.generated';

import { GlobalTagsFieldsFragment, GlossaryTermsFragment } from './fragments.generated';
import { gql } from '@apollo/client';
import { GlobalTagsFieldsFragmentDoc, GlossaryTermsFragmentDoc } from './fragments.generated';
import * as Apollo from '@apollo/client';
export type GetVersionedDatasetQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    versionStamp?: Types.Maybe<Types.Scalars['String']>;
}>;

export type GetVersionedDatasetQuery = { __typename?: 'Query' } & {
    versionedDataset?: Types.Maybe<
        { __typename?: 'VersionedDataset' } & {
            schema?: Types.Maybe<
                { __typename?: 'Schema' } & Pick<Types.Schema, 'lastObserved'> & {
                        fields: Array<
                            { __typename?: 'SchemaField' } & Pick<
                                Types.SchemaField,
                                | 'fieldPath'
                                | 'jsonPath'
                                | 'nullable'
                                | 'description'
                                | 'type'
                                | 'nativeDataType'
                                | 'recursive'
                                | 'isPartOfKey'
                            >
                        >;
                    }
            >;
            editableSchemaMetadata?: Types.Maybe<
                { __typename?: 'EditableSchemaMetadata' } & {
                    editableSchemaFieldInfo: Array<
                        { __typename?: 'EditableSchemaFieldInfo' } & Pick<
                            Types.EditableSchemaFieldInfo,
                            'fieldPath' | 'description'
                        > & {
                                globalTags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                                glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
                            }
                    >;
                }
            >;
        }
    >;
};

export const GetVersionedDatasetDocument = gql`
    query getVersionedDataset($urn: String!, $versionStamp: String) {
        versionedDataset(urn: $urn, versionStamp: $versionStamp) {
            schema {
                fields {
                    fieldPath
                    jsonPath
                    nullable
                    description
                    type
                    nativeDataType
                    recursive
                    isPartOfKey
                }
                lastObserved
            }
            editableSchemaMetadata {
                editableSchemaFieldInfo {
                    fieldPath
                    description
                    globalTags {
                        ...globalTagsFields
                    }
                    glossaryTerms {
                        ...glossaryTerms
                    }
                }
            }
        }
    }
    ${GlobalTagsFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
`;

/**
 * __useGetVersionedDatasetQuery__
 *
 * To run a query within a React component, call `useGetVersionedDatasetQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetVersionedDatasetQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetVersionedDatasetQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *      versionStamp: // value for 'versionStamp'
 *   },
 * });
 */
export function useGetVersionedDatasetQuery(
    baseOptions: Apollo.QueryHookOptions<GetVersionedDatasetQuery, GetVersionedDatasetQueryVariables>,
) {
    return Apollo.useQuery<GetVersionedDatasetQuery, GetVersionedDatasetQueryVariables>(
        GetVersionedDatasetDocument,
        baseOptions,
    );
}
export function useGetVersionedDatasetLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetVersionedDatasetQuery, GetVersionedDatasetQueryVariables>,
) {
    return Apollo.useLazyQuery<GetVersionedDatasetQuery, GetVersionedDatasetQueryVariables>(
        GetVersionedDatasetDocument,
        baseOptions,
    );
}
export type GetVersionedDatasetQueryHookResult = ReturnType<typeof useGetVersionedDatasetQuery>;
export type GetVersionedDatasetLazyQueryHookResult = ReturnType<typeof useGetVersionedDatasetLazyQuery>;
export type GetVersionedDatasetQueryResult = Apollo.QueryResult<
    GetVersionedDatasetQuery,
    GetVersionedDatasetQueryVariables
>;
