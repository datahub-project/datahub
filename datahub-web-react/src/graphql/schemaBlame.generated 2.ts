/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type GetSchemaBlameQueryVariables = Types.Exact<{
    input: Types.GetSchemaBlameInput;
}>;

export type GetSchemaBlameQuery = { __typename?: 'Query' } & {
    getSchemaBlame?: Types.Maybe<
        { __typename?: 'GetSchemaBlameResult' } & {
            version?: Types.Maybe<
                { __typename?: 'SemanticVersionStruct' } & Pick<
                    Types.SemanticVersionStruct,
                    'semanticVersion' | 'semanticVersionTimestamp' | 'versionStamp'
                >
            >;
            schemaFieldBlameList?: Types.Maybe<
                Array<
                    { __typename?: 'SchemaFieldBlame' } & Pick<Types.SchemaFieldBlame, 'fieldPath'> & {
                            schemaFieldChange: { __typename?: 'SchemaFieldChange' } & Pick<
                                Types.SchemaFieldChange,
                                'timestampMillis' | 'lastSemanticVersion' | 'lastSchemaFieldChange' | 'versionStamp'
                            >;
                        }
                >
            >;
        }
    >;
};

export type GetSchemaVersionListQueryVariables = Types.Exact<{
    input: Types.GetSchemaVersionListInput;
}>;

export type GetSchemaVersionListQuery = { __typename?: 'Query' } & {
    getSchemaVersionList?: Types.Maybe<
        { __typename?: 'GetSchemaVersionListResult' } & {
            latestVersion?: Types.Maybe<
                { __typename?: 'SemanticVersionStruct' } & Pick<
                    Types.SemanticVersionStruct,
                    'semanticVersion' | 'semanticVersionTimestamp' | 'versionStamp'
                >
            >;
            semanticVersionList?: Types.Maybe<
                Array<
                    { __typename?: 'SemanticVersionStruct' } & Pick<
                        Types.SemanticVersionStruct,
                        'semanticVersion' | 'semanticVersionTimestamp' | 'versionStamp'
                    >
                >
            >;
        }
    >;
};

export const GetSchemaBlameDocument = gql`
    query getSchemaBlame($input: GetSchemaBlameInput!) {
        getSchemaBlame(input: $input) {
            version {
                semanticVersion
                semanticVersionTimestamp
                versionStamp
            }
            schemaFieldBlameList {
                fieldPath
                schemaFieldChange {
                    timestampMillis
                    lastSemanticVersion
                    lastSchemaFieldChange
                    versionStamp
                }
            }
        }
    }
`;

/**
 * __useGetSchemaBlameQuery__
 *
 * To run a query within a React component, call `useGetSchemaBlameQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetSchemaBlameQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetSchemaBlameQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetSchemaBlameQuery(
    baseOptions: Apollo.QueryHookOptions<GetSchemaBlameQuery, GetSchemaBlameQueryVariables>,
) {
    return Apollo.useQuery<GetSchemaBlameQuery, GetSchemaBlameQueryVariables>(GetSchemaBlameDocument, baseOptions);
}
export function useGetSchemaBlameLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetSchemaBlameQuery, GetSchemaBlameQueryVariables>,
) {
    return Apollo.useLazyQuery<GetSchemaBlameQuery, GetSchemaBlameQueryVariables>(GetSchemaBlameDocument, baseOptions);
}
export type GetSchemaBlameQueryHookResult = ReturnType<typeof useGetSchemaBlameQuery>;
export type GetSchemaBlameLazyQueryHookResult = ReturnType<typeof useGetSchemaBlameLazyQuery>;
export type GetSchemaBlameQueryResult = Apollo.QueryResult<GetSchemaBlameQuery, GetSchemaBlameQueryVariables>;
export const GetSchemaVersionListDocument = gql`
    query getSchemaVersionList($input: GetSchemaVersionListInput!) {
        getSchemaVersionList(input: $input) {
            latestVersion {
                semanticVersion
                semanticVersionTimestamp
                versionStamp
            }
            semanticVersionList {
                semanticVersion
                semanticVersionTimestamp
                versionStamp
            }
        }
    }
`;

/**
 * __useGetSchemaVersionListQuery__
 *
 * To run a query within a React component, call `useGetSchemaVersionListQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetSchemaVersionListQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetSchemaVersionListQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useGetSchemaVersionListQuery(
    baseOptions: Apollo.QueryHookOptions<GetSchemaVersionListQuery, GetSchemaVersionListQueryVariables>,
) {
    return Apollo.useQuery<GetSchemaVersionListQuery, GetSchemaVersionListQueryVariables>(
        GetSchemaVersionListDocument,
        baseOptions,
    );
}
export function useGetSchemaVersionListLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetSchemaVersionListQuery, GetSchemaVersionListQueryVariables>,
) {
    return Apollo.useLazyQuery<GetSchemaVersionListQuery, GetSchemaVersionListQueryVariables>(
        GetSchemaVersionListDocument,
        baseOptions,
    );
}
export type GetSchemaVersionListQueryHookResult = ReturnType<typeof useGetSchemaVersionListQuery>;
export type GetSchemaVersionListLazyQueryHookResult = ReturnType<typeof useGetSchemaVersionListLazyQuery>;
export type GetSchemaVersionListQueryResult = Apollo.QueryResult<
    GetSchemaVersionListQuery,
    GetSchemaVersionListQueryVariables
>;
