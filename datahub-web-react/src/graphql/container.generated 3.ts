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
export type GetContainerQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetContainerQuery = { __typename?: 'Query' } & {
    container?: Types.Maybe<
        { __typename?: 'Container' } & Pick<Types.Container, 'urn' | 'exists' | 'lastIngested'> & {
                platform: { __typename?: 'DataPlatform' } & PlatformFieldsFragment;
                properties?: Types.Maybe<
                    { __typename?: 'ContainerProperties' } & Pick<Types.ContainerProperties, 'name' | 'description'> & {
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
                editableProperties?: Types.Maybe<
                    { __typename?: 'ContainerEditableProperties' } & Pick<
                        Types.ContainerEditableProperties,
                        'description'
                    >
                >;
                ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                tags?: Types.Maybe<{ __typename?: 'GlobalTags' } & GlobalTagsFieldsFragment>;
                institutionalMemory?: Types.Maybe<
                    { __typename?: 'InstitutionalMemory' } & InstitutionalMemoryFieldsFragment
                >;
                glossaryTerms?: Types.Maybe<{ __typename?: 'GlossaryTerms' } & GlossaryTermsFragment>;
                subTypes?: Types.Maybe<{ __typename?: 'SubTypes' } & Pick<Types.SubTypes, 'typeNames'>>;
                entities?: Types.Maybe<{ __typename?: 'SearchResults' } & Pick<Types.SearchResults, 'total'>>;
                container?: Types.Maybe<{ __typename?: 'Container' } & EntityContainerFragment>;
                parentContainers?: Types.Maybe<
                    { __typename?: 'ParentContainersResult' } & ParentContainersFieldsFragment
                >;
                domain?: Types.Maybe<{ __typename?: 'DomainAssociation' } & EntityDomainFragment>;
                deprecation?: Types.Maybe<{ __typename?: 'Deprecation' } & DeprecationFieldsFragment>;
                dataPlatformInstance?: Types.Maybe<
                    { __typename?: 'DataPlatformInstance' } & DataPlatformInstanceFieldsFragment
                >;
                status?: Types.Maybe<{ __typename?: 'Status' } & Pick<Types.Status, 'removed'>>;
            }
    >;
};

export const GetContainerDocument = gql`
    query getContainer($urn: String!) {
        container(urn: $urn) {
            urn
            exists
            lastIngested
            platform {
                ...platformFields
            }
            properties {
                name
                description
                customProperties {
                    key
                    value
                }
            }
            editableProperties {
                description
            }
            ownership {
                ...ownershipFields
            }
            tags {
                ...globalTagsFields
            }
            institutionalMemory {
                ...institutionalMemoryFields
            }
            glossaryTerms {
                ...glossaryTerms
            }
            subTypes {
                typeNames
            }
            entities(input: { start: 0, count: 1 }) {
                total
            }
            container {
                ...entityContainer
            }
            parentContainers {
                ...parentContainersFields
            }
            domain {
                ...entityDomain
            }
            deprecation {
                ...deprecationFields
            }
            dataPlatformInstance {
                ...dataPlatformInstanceFields
            }
            status {
                removed
            }
        }
    }
    ${PlatformFieldsFragmentDoc}
    ${OwnershipFieldsFragmentDoc}
    ${GlobalTagsFieldsFragmentDoc}
    ${InstitutionalMemoryFieldsFragmentDoc}
    ${GlossaryTermsFragmentDoc}
    ${EntityContainerFragmentDoc}
    ${ParentContainersFieldsFragmentDoc}
    ${EntityDomainFragmentDoc}
    ${DeprecationFieldsFragmentDoc}
    ${DataPlatformInstanceFieldsFragmentDoc}
`;

/**
 * __useGetContainerQuery__
 *
 * To run a query within a React component, call `useGetContainerQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetContainerQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetContainerQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetContainerQuery(
    baseOptions: Apollo.QueryHookOptions<GetContainerQuery, GetContainerQueryVariables>,
) {
    return Apollo.useQuery<GetContainerQuery, GetContainerQueryVariables>(GetContainerDocument, baseOptions);
}
export function useGetContainerLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetContainerQuery, GetContainerQueryVariables>,
) {
    return Apollo.useLazyQuery<GetContainerQuery, GetContainerQueryVariables>(GetContainerDocument, baseOptions);
}
export type GetContainerQueryHookResult = ReturnType<typeof useGetContainerQuery>;
export type GetContainerLazyQueryHookResult = ReturnType<typeof useGetContainerLazyQuery>;
export type GetContainerQueryResult = Apollo.QueryResult<GetContainerQuery, GetContainerQueryVariables>;
