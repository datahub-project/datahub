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
export type GetDataPlatformQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetDataPlatformQuery = { __typename?: 'Query' } & {
    dataPlatform?: Types.Maybe<{ __typename?: 'DataPlatform' } & PlatformFieldsFragment>;
};

export const GetDataPlatformDocument = gql`
    query getDataPlatform($urn: String!) {
        dataPlatform(urn: $urn) {
            ...platformFields
        }
    }
    ${PlatformFieldsFragmentDoc}
`;

/**
 * __useGetDataPlatformQuery__
 *
 * To run a query within a React component, call `useGetDataPlatformQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDataPlatformQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDataPlatformQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetDataPlatformQuery(
    baseOptions: Apollo.QueryHookOptions<GetDataPlatformQuery, GetDataPlatformQueryVariables>,
) {
    return Apollo.useQuery<GetDataPlatformQuery, GetDataPlatformQueryVariables>(GetDataPlatformDocument, baseOptions);
}
export function useGetDataPlatformLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDataPlatformQuery, GetDataPlatformQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDataPlatformQuery, GetDataPlatformQueryVariables>(
        GetDataPlatformDocument,
        baseOptions,
    );
}
export type GetDataPlatformQueryHookResult = ReturnType<typeof useGetDataPlatformQuery>;
export type GetDataPlatformLazyQueryHookResult = ReturnType<typeof useGetDataPlatformLazyQuery>;
export type GetDataPlatformQueryResult = Apollo.QueryResult<GetDataPlatformQuery, GetDataPlatformQueryVariables>;
