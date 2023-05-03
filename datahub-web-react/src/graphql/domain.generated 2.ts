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
export type GetDomainQueryVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type GetDomainQuery = { __typename?: 'Query' } & {
    domain?: Types.Maybe<
        { __typename?: 'Domain' } & Pick<Types.Domain, 'urn' | 'id'> & {
                properties?: Types.Maybe<
                    { __typename?: 'DomainProperties' } & Pick<Types.DomainProperties, 'name' | 'description'>
                >;
                ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                institutionalMemory?: Types.Maybe<
                    { __typename?: 'InstitutionalMemory' } & {
                        elements: Array<
                            { __typename?: 'InstitutionalMemoryMetadata' } & Pick<
                                Types.InstitutionalMemoryMetadata,
                                'url' | 'description'
                            > & {
                                    author: { __typename?: 'CorpUser' } & Pick<Types.CorpUser, 'urn' | 'username'>;
                                    created: { __typename?: 'AuditStamp' } & Pick<Types.AuditStamp, 'actor' | 'time'>;
                                }
                        >;
                    }
                >;
            }
    >;
};

export type ListDomainsQueryVariables = Types.Exact<{
    input: Types.ListDomainsInput;
}>;

export type ListDomainsQuery = { __typename?: 'Query' } & {
    listDomains?: Types.Maybe<
        { __typename?: 'ListDomainsResult' } & Pick<Types.ListDomainsResult, 'start' | 'count' | 'total'> & {
                domains: Array<
                    { __typename?: 'Domain' } & Pick<Types.Domain, 'urn'> & {
                            properties?: Types.Maybe<
                                { __typename?: 'DomainProperties' } & Pick<
                                    Types.DomainProperties,
                                    'name' | 'description'
                                >
                            >;
                            ownership?: Types.Maybe<{ __typename?: 'Ownership' } & OwnershipFieldsFragment>;
                            entities?: Types.Maybe<
                                { __typename?: 'SearchResults' } & Pick<Types.SearchResults, 'total'>
                            >;
                        }
                >;
            }
    >;
};

export type CreateDomainMutationVariables = Types.Exact<{
    input: Types.CreateDomainInput;
}>;

export type CreateDomainMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'createDomain'>;

export type DeleteDomainMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
}>;

export type DeleteDomainMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'deleteDomain'>;

export const GetDomainDocument = gql`
    query getDomain($urn: String!) {
        domain(urn: $urn) {
            urn
            id
            properties {
                name
                description
            }
            ownership {
                ...ownershipFields
            }
            institutionalMemory {
                elements {
                    url
                    author {
                        urn
                        username
                    }
                    description
                    created {
                        actor
                        time
                    }
                }
            }
        }
    }
    ${OwnershipFieldsFragmentDoc}
`;

/**
 * __useGetDomainQuery__
 *
 * To run a query within a React component, call `useGetDomainQuery` and pass it any options that fit your needs.
 * When your component renders, `useGetDomainQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useGetDomainQuery({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useGetDomainQuery(baseOptions: Apollo.QueryHookOptions<GetDomainQuery, GetDomainQueryVariables>) {
    return Apollo.useQuery<GetDomainQuery, GetDomainQueryVariables>(GetDomainDocument, baseOptions);
}
export function useGetDomainLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<GetDomainQuery, GetDomainQueryVariables>,
) {
    return Apollo.useLazyQuery<GetDomainQuery, GetDomainQueryVariables>(GetDomainDocument, baseOptions);
}
export type GetDomainQueryHookResult = ReturnType<typeof useGetDomainQuery>;
export type GetDomainLazyQueryHookResult = ReturnType<typeof useGetDomainLazyQuery>;
export type GetDomainQueryResult = Apollo.QueryResult<GetDomainQuery, GetDomainQueryVariables>;
export const ListDomainsDocument = gql`
    query listDomains($input: ListDomainsInput!) {
        listDomains(input: $input) {
            start
            count
            total
            domains {
                urn
                properties {
                    name
                    description
                }
                ownership {
                    ...ownershipFields
                }
                entities(input: { start: 0, count: 1 }) {
                    total
                }
            }
        }
    }
    ${OwnershipFieldsFragmentDoc}
`;

/**
 * __useListDomainsQuery__
 *
 * To run a query within a React component, call `useListDomainsQuery` and pass it any options that fit your needs.
 * When your component renders, `useListDomainsQuery` returns an object from Apollo Client that contains loading, error, and data properties
 * you can use to render your UI.
 *
 * @param baseOptions options that will be passed into the query, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options;
 *
 * @example
 * const { data, loading, error } = useListDomainsQuery({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useListDomainsQuery(baseOptions: Apollo.QueryHookOptions<ListDomainsQuery, ListDomainsQueryVariables>) {
    return Apollo.useQuery<ListDomainsQuery, ListDomainsQueryVariables>(ListDomainsDocument, baseOptions);
}
export function useListDomainsLazyQuery(
    baseOptions?: Apollo.LazyQueryHookOptions<ListDomainsQuery, ListDomainsQueryVariables>,
) {
    return Apollo.useLazyQuery<ListDomainsQuery, ListDomainsQueryVariables>(ListDomainsDocument, baseOptions);
}
export type ListDomainsQueryHookResult = ReturnType<typeof useListDomainsQuery>;
export type ListDomainsLazyQueryHookResult = ReturnType<typeof useListDomainsLazyQuery>;
export type ListDomainsQueryResult = Apollo.QueryResult<ListDomainsQuery, ListDomainsQueryVariables>;
export const CreateDomainDocument = gql`
    mutation createDomain($input: CreateDomainInput!) {
        createDomain(input: $input)
    }
`;
export type CreateDomainMutationFn = Apollo.MutationFunction<CreateDomainMutation, CreateDomainMutationVariables>;

/**
 * __useCreateDomainMutation__
 *
 * To run a mutation, you first call `useCreateDomainMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateDomainMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createDomainMutation, { data, loading, error }] = useCreateDomainMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateDomainMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateDomainMutation, CreateDomainMutationVariables>,
) {
    return Apollo.useMutation<CreateDomainMutation, CreateDomainMutationVariables>(CreateDomainDocument, baseOptions);
}
export type CreateDomainMutationHookResult = ReturnType<typeof useCreateDomainMutation>;
export type CreateDomainMutationResult = Apollo.MutationResult<CreateDomainMutation>;
export type CreateDomainMutationOptions = Apollo.BaseMutationOptions<
    CreateDomainMutation,
    CreateDomainMutationVariables
>;
export const DeleteDomainDocument = gql`
    mutation deleteDomain($urn: String!) {
        deleteDomain(urn: $urn)
    }
`;
export type DeleteDomainMutationFn = Apollo.MutationFunction<DeleteDomainMutation, DeleteDomainMutationVariables>;

/**
 * __useDeleteDomainMutation__
 *
 * To run a mutation, you first call `useDeleteDomainMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useDeleteDomainMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [deleteDomainMutation, { data, loading, error }] = useDeleteDomainMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *   },
 * });
 */
export function useDeleteDomainMutation(
    baseOptions?: Apollo.MutationHookOptions<DeleteDomainMutation, DeleteDomainMutationVariables>,
) {
    return Apollo.useMutation<DeleteDomainMutation, DeleteDomainMutationVariables>(DeleteDomainDocument, baseOptions);
}
export type DeleteDomainMutationHookResult = ReturnType<typeof useDeleteDomainMutation>;
export type DeleteDomainMutationResult = Apollo.MutationResult<DeleteDomainMutation>;
export type DeleteDomainMutationOptions = Apollo.BaseMutationOptions<
    DeleteDomainMutation,
    DeleteDomainMutationVariables
>;
