/* eslint-disable */
import * as Types from '../types.generated';

import { gql } from '@apollo/client';
import * as Apollo from '@apollo/client';
export type RemoveTagMutationVariables = Types.Exact<{
    input: Types.TagAssociationInput;
}>;

export type RemoveTagMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'removeTag'>;

export type BatchRemoveTagsMutationVariables = Types.Exact<{
    input: Types.BatchRemoveTagsInput;
}>;

export type BatchRemoveTagsMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'batchRemoveTags'>;

export type AddTagMutationVariables = Types.Exact<{
    input: Types.TagAssociationInput;
}>;

export type AddTagMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'addTag'>;

export type BatchAddTagsMutationVariables = Types.Exact<{
    input: Types.BatchAddTagsInput;
}>;

export type BatchAddTagsMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'batchAddTags'>;

export type RemoveTermMutationVariables = Types.Exact<{
    input: Types.TermAssociationInput;
}>;

export type RemoveTermMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'removeTerm'>;

export type BatchRemoveTermsMutationVariables = Types.Exact<{
    input: Types.BatchRemoveTermsInput;
}>;

export type BatchRemoveTermsMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'batchRemoveTerms'>;

export type AddTermMutationVariables = Types.Exact<{
    input: Types.TermAssociationInput;
}>;

export type AddTermMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'addTerm'>;

export type BatchAddTermsMutationVariables = Types.Exact<{
    input: Types.BatchAddTermsInput;
}>;

export type BatchAddTermsMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'batchAddTerms'>;

export type AddLinkMutationVariables = Types.Exact<{
    input: Types.AddLinkInput;
}>;

export type AddLinkMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'addLink'>;

export type RemoveLinkMutationVariables = Types.Exact<{
    input: Types.RemoveLinkInput;
}>;

export type RemoveLinkMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'removeLink'>;

export type AddOwnerMutationVariables = Types.Exact<{
    input: Types.AddOwnerInput;
}>;

export type AddOwnerMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'addOwner'>;

export type BatchAddOwnersMutationVariables = Types.Exact<{
    input: Types.BatchAddOwnersInput;
}>;

export type BatchAddOwnersMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'batchAddOwners'>;

export type RemoveOwnerMutationVariables = Types.Exact<{
    input: Types.RemoveOwnerInput;
}>;

export type RemoveOwnerMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'removeOwner'>;

export type BatchRemoveOwnersMutationVariables = Types.Exact<{
    input: Types.BatchRemoveOwnersInput;
}>;

export type BatchRemoveOwnersMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'batchRemoveOwners'>;

export type UpdateDescriptionMutationVariables = Types.Exact<{
    input: Types.DescriptionUpdateInput;
}>;

export type UpdateDescriptionMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updateDescription'>;

export type SetDomainMutationVariables = Types.Exact<{
    entityUrn: Types.Scalars['String'];
    domainUrn: Types.Scalars['String'];
}>;

export type SetDomainMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'setDomain'>;

export type UnsetDomainMutationVariables = Types.Exact<{
    entityUrn: Types.Scalars['String'];
}>;

export type UnsetDomainMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'unsetDomain'>;

export type SetTagColorMutationVariables = Types.Exact<{
    urn: Types.Scalars['String'];
    colorHex: Types.Scalars['String'];
}>;

export type SetTagColorMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'setTagColor'>;

export type UpdateDeprecationMutationVariables = Types.Exact<{
    input: Types.UpdateDeprecationInput;
}>;

export type UpdateDeprecationMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updateDeprecation'>;

export type AddOwnersMutationVariables = Types.Exact<{
    input: Types.AddOwnersInput;
}>;

export type AddOwnersMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'addOwners'>;

export type AddTagsMutationVariables = Types.Exact<{
    input: Types.AddTagsInput;
}>;

export type AddTagsMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'addTags'>;

export type AddTermsMutationVariables = Types.Exact<{
    input: Types.AddTermsInput;
}>;

export type AddTermsMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'addTerms'>;

export type UpdateNameMutationVariables = Types.Exact<{
    input: Types.UpdateNameInput;
}>;

export type UpdateNameMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updateName'>;

export type BatchSetDomainMutationVariables = Types.Exact<{
    input: Types.BatchSetDomainInput;
}>;

export type BatchSetDomainMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'batchSetDomain'>;

export type BatchUpdateDeprecationMutationVariables = Types.Exact<{
    input: Types.BatchUpdateDeprecationInput;
}>;

export type BatchUpdateDeprecationMutation = { __typename?: 'Mutation' } & Pick<
    Types.Mutation,
    'batchUpdateDeprecation'
>;

export type BatchUpdateSoftDeletedMutationVariables = Types.Exact<{
    input: Types.BatchUpdateSoftDeletedInput;
}>;

export type BatchUpdateSoftDeletedMutation = { __typename?: 'Mutation' } & Pick<
    Types.Mutation,
    'batchUpdateSoftDeleted'
>;

export type BatchAssignRoleMutationVariables = Types.Exact<{
    input: Types.BatchAssignRoleInput;
}>;

export type BatchAssignRoleMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'batchAssignRole'>;

export type CreateInviteTokenMutationVariables = Types.Exact<{
    input: Types.CreateInviteTokenInput;
}>;

export type CreateInviteTokenMutation = { __typename?: 'Mutation' } & {
    createInviteToken?: Types.Maybe<{ __typename?: 'InviteToken' } & Pick<Types.InviteToken, 'inviteToken'>>;
};

export type AcceptRoleMutationVariables = Types.Exact<{
    input: Types.AcceptRoleInput;
}>;

export type AcceptRoleMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'acceptRole'>;

export type CreatePostMutationVariables = Types.Exact<{
    input: Types.CreatePostInput;
}>;

export type CreatePostMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'createPost'>;

export type UpdateLineageMutationVariables = Types.Exact<{
    input: Types.UpdateLineageInput;
}>;

export type UpdateLineageMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updateLineage'>;

export type UpdateEmbedMutationVariables = Types.Exact<{
    input: Types.UpdateEmbedInput;
}>;

export type UpdateEmbedMutation = { __typename?: 'Mutation' } & Pick<Types.Mutation, 'updateEmbed'>;

export const RemoveTagDocument = gql`
    mutation removeTag($input: TagAssociationInput!) {
        removeTag(input: $input)
    }
`;
export type RemoveTagMutationFn = Apollo.MutationFunction<RemoveTagMutation, RemoveTagMutationVariables>;

/**
 * __useRemoveTagMutation__
 *
 * To run a mutation, you first call `useRemoveTagMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useRemoveTagMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [removeTagMutation, { data, loading, error }] = useRemoveTagMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useRemoveTagMutation(
    baseOptions?: Apollo.MutationHookOptions<RemoveTagMutation, RemoveTagMutationVariables>,
) {
    return Apollo.useMutation<RemoveTagMutation, RemoveTagMutationVariables>(RemoveTagDocument, baseOptions);
}
export type RemoveTagMutationHookResult = ReturnType<typeof useRemoveTagMutation>;
export type RemoveTagMutationResult = Apollo.MutationResult<RemoveTagMutation>;
export type RemoveTagMutationOptions = Apollo.BaseMutationOptions<RemoveTagMutation, RemoveTagMutationVariables>;
export const BatchRemoveTagsDocument = gql`
    mutation batchRemoveTags($input: BatchRemoveTagsInput!) {
        batchRemoveTags(input: $input)
    }
`;
export type BatchRemoveTagsMutationFn = Apollo.MutationFunction<
    BatchRemoveTagsMutation,
    BatchRemoveTagsMutationVariables
>;

/**
 * __useBatchRemoveTagsMutation__
 *
 * To run a mutation, you first call `useBatchRemoveTagsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchRemoveTagsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchRemoveTagsMutation, { data, loading, error }] = useBatchRemoveTagsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchRemoveTagsMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchRemoveTagsMutation, BatchRemoveTagsMutationVariables>,
) {
    return Apollo.useMutation<BatchRemoveTagsMutation, BatchRemoveTagsMutationVariables>(
        BatchRemoveTagsDocument,
        baseOptions,
    );
}
export type BatchRemoveTagsMutationHookResult = ReturnType<typeof useBatchRemoveTagsMutation>;
export type BatchRemoveTagsMutationResult = Apollo.MutationResult<BatchRemoveTagsMutation>;
export type BatchRemoveTagsMutationOptions = Apollo.BaseMutationOptions<
    BatchRemoveTagsMutation,
    BatchRemoveTagsMutationVariables
>;
export const AddTagDocument = gql`
    mutation addTag($input: TagAssociationInput!) {
        addTag(input: $input)
    }
`;
export type AddTagMutationFn = Apollo.MutationFunction<AddTagMutation, AddTagMutationVariables>;

/**
 * __useAddTagMutation__
 *
 * To run a mutation, you first call `useAddTagMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useAddTagMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [addTagMutation, { data, loading, error }] = useAddTagMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useAddTagMutation(baseOptions?: Apollo.MutationHookOptions<AddTagMutation, AddTagMutationVariables>) {
    return Apollo.useMutation<AddTagMutation, AddTagMutationVariables>(AddTagDocument, baseOptions);
}
export type AddTagMutationHookResult = ReturnType<typeof useAddTagMutation>;
export type AddTagMutationResult = Apollo.MutationResult<AddTagMutation>;
export type AddTagMutationOptions = Apollo.BaseMutationOptions<AddTagMutation, AddTagMutationVariables>;
export const BatchAddTagsDocument = gql`
    mutation batchAddTags($input: BatchAddTagsInput!) {
        batchAddTags(input: $input)
    }
`;
export type BatchAddTagsMutationFn = Apollo.MutationFunction<BatchAddTagsMutation, BatchAddTagsMutationVariables>;

/**
 * __useBatchAddTagsMutation__
 *
 * To run a mutation, you first call `useBatchAddTagsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchAddTagsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchAddTagsMutation, { data, loading, error }] = useBatchAddTagsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchAddTagsMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchAddTagsMutation, BatchAddTagsMutationVariables>,
) {
    return Apollo.useMutation<BatchAddTagsMutation, BatchAddTagsMutationVariables>(BatchAddTagsDocument, baseOptions);
}
export type BatchAddTagsMutationHookResult = ReturnType<typeof useBatchAddTagsMutation>;
export type BatchAddTagsMutationResult = Apollo.MutationResult<BatchAddTagsMutation>;
export type BatchAddTagsMutationOptions = Apollo.BaseMutationOptions<
    BatchAddTagsMutation,
    BatchAddTagsMutationVariables
>;
export const RemoveTermDocument = gql`
    mutation removeTerm($input: TermAssociationInput!) {
        removeTerm(input: $input)
    }
`;
export type RemoveTermMutationFn = Apollo.MutationFunction<RemoveTermMutation, RemoveTermMutationVariables>;

/**
 * __useRemoveTermMutation__
 *
 * To run a mutation, you first call `useRemoveTermMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useRemoveTermMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [removeTermMutation, { data, loading, error }] = useRemoveTermMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useRemoveTermMutation(
    baseOptions?: Apollo.MutationHookOptions<RemoveTermMutation, RemoveTermMutationVariables>,
) {
    return Apollo.useMutation<RemoveTermMutation, RemoveTermMutationVariables>(RemoveTermDocument, baseOptions);
}
export type RemoveTermMutationHookResult = ReturnType<typeof useRemoveTermMutation>;
export type RemoveTermMutationResult = Apollo.MutationResult<RemoveTermMutation>;
export type RemoveTermMutationOptions = Apollo.BaseMutationOptions<RemoveTermMutation, RemoveTermMutationVariables>;
export const BatchRemoveTermsDocument = gql`
    mutation batchRemoveTerms($input: BatchRemoveTermsInput!) {
        batchRemoveTerms(input: $input)
    }
`;
export type BatchRemoveTermsMutationFn = Apollo.MutationFunction<
    BatchRemoveTermsMutation,
    BatchRemoveTermsMutationVariables
>;

/**
 * __useBatchRemoveTermsMutation__
 *
 * To run a mutation, you first call `useBatchRemoveTermsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchRemoveTermsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchRemoveTermsMutation, { data, loading, error }] = useBatchRemoveTermsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchRemoveTermsMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchRemoveTermsMutation, BatchRemoveTermsMutationVariables>,
) {
    return Apollo.useMutation<BatchRemoveTermsMutation, BatchRemoveTermsMutationVariables>(
        BatchRemoveTermsDocument,
        baseOptions,
    );
}
export type BatchRemoveTermsMutationHookResult = ReturnType<typeof useBatchRemoveTermsMutation>;
export type BatchRemoveTermsMutationResult = Apollo.MutationResult<BatchRemoveTermsMutation>;
export type BatchRemoveTermsMutationOptions = Apollo.BaseMutationOptions<
    BatchRemoveTermsMutation,
    BatchRemoveTermsMutationVariables
>;
export const AddTermDocument = gql`
    mutation addTerm($input: TermAssociationInput!) {
        addTerm(input: $input)
    }
`;
export type AddTermMutationFn = Apollo.MutationFunction<AddTermMutation, AddTermMutationVariables>;

/**
 * __useAddTermMutation__
 *
 * To run a mutation, you first call `useAddTermMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useAddTermMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [addTermMutation, { data, loading, error }] = useAddTermMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useAddTermMutation(
    baseOptions?: Apollo.MutationHookOptions<AddTermMutation, AddTermMutationVariables>,
) {
    return Apollo.useMutation<AddTermMutation, AddTermMutationVariables>(AddTermDocument, baseOptions);
}
export type AddTermMutationHookResult = ReturnType<typeof useAddTermMutation>;
export type AddTermMutationResult = Apollo.MutationResult<AddTermMutation>;
export type AddTermMutationOptions = Apollo.BaseMutationOptions<AddTermMutation, AddTermMutationVariables>;
export const BatchAddTermsDocument = gql`
    mutation batchAddTerms($input: BatchAddTermsInput!) {
        batchAddTerms(input: $input)
    }
`;
export type BatchAddTermsMutationFn = Apollo.MutationFunction<BatchAddTermsMutation, BatchAddTermsMutationVariables>;

/**
 * __useBatchAddTermsMutation__
 *
 * To run a mutation, you first call `useBatchAddTermsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchAddTermsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchAddTermsMutation, { data, loading, error }] = useBatchAddTermsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchAddTermsMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchAddTermsMutation, BatchAddTermsMutationVariables>,
) {
    return Apollo.useMutation<BatchAddTermsMutation, BatchAddTermsMutationVariables>(
        BatchAddTermsDocument,
        baseOptions,
    );
}
export type BatchAddTermsMutationHookResult = ReturnType<typeof useBatchAddTermsMutation>;
export type BatchAddTermsMutationResult = Apollo.MutationResult<BatchAddTermsMutation>;
export type BatchAddTermsMutationOptions = Apollo.BaseMutationOptions<
    BatchAddTermsMutation,
    BatchAddTermsMutationVariables
>;
export const AddLinkDocument = gql`
    mutation addLink($input: AddLinkInput!) {
        addLink(input: $input)
    }
`;
export type AddLinkMutationFn = Apollo.MutationFunction<AddLinkMutation, AddLinkMutationVariables>;

/**
 * __useAddLinkMutation__
 *
 * To run a mutation, you first call `useAddLinkMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useAddLinkMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [addLinkMutation, { data, loading, error }] = useAddLinkMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useAddLinkMutation(
    baseOptions?: Apollo.MutationHookOptions<AddLinkMutation, AddLinkMutationVariables>,
) {
    return Apollo.useMutation<AddLinkMutation, AddLinkMutationVariables>(AddLinkDocument, baseOptions);
}
export type AddLinkMutationHookResult = ReturnType<typeof useAddLinkMutation>;
export type AddLinkMutationResult = Apollo.MutationResult<AddLinkMutation>;
export type AddLinkMutationOptions = Apollo.BaseMutationOptions<AddLinkMutation, AddLinkMutationVariables>;
export const RemoveLinkDocument = gql`
    mutation removeLink($input: RemoveLinkInput!) {
        removeLink(input: $input)
    }
`;
export type RemoveLinkMutationFn = Apollo.MutationFunction<RemoveLinkMutation, RemoveLinkMutationVariables>;

/**
 * __useRemoveLinkMutation__
 *
 * To run a mutation, you first call `useRemoveLinkMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useRemoveLinkMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [removeLinkMutation, { data, loading, error }] = useRemoveLinkMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useRemoveLinkMutation(
    baseOptions?: Apollo.MutationHookOptions<RemoveLinkMutation, RemoveLinkMutationVariables>,
) {
    return Apollo.useMutation<RemoveLinkMutation, RemoveLinkMutationVariables>(RemoveLinkDocument, baseOptions);
}
export type RemoveLinkMutationHookResult = ReturnType<typeof useRemoveLinkMutation>;
export type RemoveLinkMutationResult = Apollo.MutationResult<RemoveLinkMutation>;
export type RemoveLinkMutationOptions = Apollo.BaseMutationOptions<RemoveLinkMutation, RemoveLinkMutationVariables>;
export const AddOwnerDocument = gql`
    mutation addOwner($input: AddOwnerInput!) {
        addOwner(input: $input)
    }
`;
export type AddOwnerMutationFn = Apollo.MutationFunction<AddOwnerMutation, AddOwnerMutationVariables>;

/**
 * __useAddOwnerMutation__
 *
 * To run a mutation, you first call `useAddOwnerMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useAddOwnerMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [addOwnerMutation, { data, loading, error }] = useAddOwnerMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useAddOwnerMutation(
    baseOptions?: Apollo.MutationHookOptions<AddOwnerMutation, AddOwnerMutationVariables>,
) {
    return Apollo.useMutation<AddOwnerMutation, AddOwnerMutationVariables>(AddOwnerDocument, baseOptions);
}
export type AddOwnerMutationHookResult = ReturnType<typeof useAddOwnerMutation>;
export type AddOwnerMutationResult = Apollo.MutationResult<AddOwnerMutation>;
export type AddOwnerMutationOptions = Apollo.BaseMutationOptions<AddOwnerMutation, AddOwnerMutationVariables>;
export const BatchAddOwnersDocument = gql`
    mutation batchAddOwners($input: BatchAddOwnersInput!) {
        batchAddOwners(input: $input)
    }
`;
export type BatchAddOwnersMutationFn = Apollo.MutationFunction<BatchAddOwnersMutation, BatchAddOwnersMutationVariables>;

/**
 * __useBatchAddOwnersMutation__
 *
 * To run a mutation, you first call `useBatchAddOwnersMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchAddOwnersMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchAddOwnersMutation, { data, loading, error }] = useBatchAddOwnersMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchAddOwnersMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchAddOwnersMutation, BatchAddOwnersMutationVariables>,
) {
    return Apollo.useMutation<BatchAddOwnersMutation, BatchAddOwnersMutationVariables>(
        BatchAddOwnersDocument,
        baseOptions,
    );
}
export type BatchAddOwnersMutationHookResult = ReturnType<typeof useBatchAddOwnersMutation>;
export type BatchAddOwnersMutationResult = Apollo.MutationResult<BatchAddOwnersMutation>;
export type BatchAddOwnersMutationOptions = Apollo.BaseMutationOptions<
    BatchAddOwnersMutation,
    BatchAddOwnersMutationVariables
>;
export const RemoveOwnerDocument = gql`
    mutation removeOwner($input: RemoveOwnerInput!) {
        removeOwner(input: $input)
    }
`;
export type RemoveOwnerMutationFn = Apollo.MutationFunction<RemoveOwnerMutation, RemoveOwnerMutationVariables>;

/**
 * __useRemoveOwnerMutation__
 *
 * To run a mutation, you first call `useRemoveOwnerMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useRemoveOwnerMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [removeOwnerMutation, { data, loading, error }] = useRemoveOwnerMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useRemoveOwnerMutation(
    baseOptions?: Apollo.MutationHookOptions<RemoveOwnerMutation, RemoveOwnerMutationVariables>,
) {
    return Apollo.useMutation<RemoveOwnerMutation, RemoveOwnerMutationVariables>(RemoveOwnerDocument, baseOptions);
}
export type RemoveOwnerMutationHookResult = ReturnType<typeof useRemoveOwnerMutation>;
export type RemoveOwnerMutationResult = Apollo.MutationResult<RemoveOwnerMutation>;
export type RemoveOwnerMutationOptions = Apollo.BaseMutationOptions<RemoveOwnerMutation, RemoveOwnerMutationVariables>;
export const BatchRemoveOwnersDocument = gql`
    mutation batchRemoveOwners($input: BatchRemoveOwnersInput!) {
        batchRemoveOwners(input: $input)
    }
`;
export type BatchRemoveOwnersMutationFn = Apollo.MutationFunction<
    BatchRemoveOwnersMutation,
    BatchRemoveOwnersMutationVariables
>;

/**
 * __useBatchRemoveOwnersMutation__
 *
 * To run a mutation, you first call `useBatchRemoveOwnersMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchRemoveOwnersMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchRemoveOwnersMutation, { data, loading, error }] = useBatchRemoveOwnersMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchRemoveOwnersMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchRemoveOwnersMutation, BatchRemoveOwnersMutationVariables>,
) {
    return Apollo.useMutation<BatchRemoveOwnersMutation, BatchRemoveOwnersMutationVariables>(
        BatchRemoveOwnersDocument,
        baseOptions,
    );
}
export type BatchRemoveOwnersMutationHookResult = ReturnType<typeof useBatchRemoveOwnersMutation>;
export type BatchRemoveOwnersMutationResult = Apollo.MutationResult<BatchRemoveOwnersMutation>;
export type BatchRemoveOwnersMutationOptions = Apollo.BaseMutationOptions<
    BatchRemoveOwnersMutation,
    BatchRemoveOwnersMutationVariables
>;
export const UpdateDescriptionDocument = gql`
    mutation updateDescription($input: DescriptionUpdateInput!) {
        updateDescription(input: $input)
    }
`;
export type UpdateDescriptionMutationFn = Apollo.MutationFunction<
    UpdateDescriptionMutation,
    UpdateDescriptionMutationVariables
>;

/**
 * __useUpdateDescriptionMutation__
 *
 * To run a mutation, you first call `useUpdateDescriptionMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateDescriptionMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateDescriptionMutation, { data, loading, error }] = useUpdateDescriptionMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateDescriptionMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateDescriptionMutation, UpdateDescriptionMutationVariables>,
) {
    return Apollo.useMutation<UpdateDescriptionMutation, UpdateDescriptionMutationVariables>(
        UpdateDescriptionDocument,
        baseOptions,
    );
}
export type UpdateDescriptionMutationHookResult = ReturnType<typeof useUpdateDescriptionMutation>;
export type UpdateDescriptionMutationResult = Apollo.MutationResult<UpdateDescriptionMutation>;
export type UpdateDescriptionMutationOptions = Apollo.BaseMutationOptions<
    UpdateDescriptionMutation,
    UpdateDescriptionMutationVariables
>;
export const SetDomainDocument = gql`
    mutation setDomain($entityUrn: String!, $domainUrn: String!) {
        setDomain(entityUrn: $entityUrn, domainUrn: $domainUrn)
    }
`;
export type SetDomainMutationFn = Apollo.MutationFunction<SetDomainMutation, SetDomainMutationVariables>;

/**
 * __useSetDomainMutation__
 *
 * To run a mutation, you first call `useSetDomainMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useSetDomainMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [setDomainMutation, { data, loading, error }] = useSetDomainMutation({
 *   variables: {
 *      entityUrn: // value for 'entityUrn'
 *      domainUrn: // value for 'domainUrn'
 *   },
 * });
 */
export function useSetDomainMutation(
    baseOptions?: Apollo.MutationHookOptions<SetDomainMutation, SetDomainMutationVariables>,
) {
    return Apollo.useMutation<SetDomainMutation, SetDomainMutationVariables>(SetDomainDocument, baseOptions);
}
export type SetDomainMutationHookResult = ReturnType<typeof useSetDomainMutation>;
export type SetDomainMutationResult = Apollo.MutationResult<SetDomainMutation>;
export type SetDomainMutationOptions = Apollo.BaseMutationOptions<SetDomainMutation, SetDomainMutationVariables>;
export const UnsetDomainDocument = gql`
    mutation unsetDomain($entityUrn: String!) {
        unsetDomain(entityUrn: $entityUrn)
    }
`;
export type UnsetDomainMutationFn = Apollo.MutationFunction<UnsetDomainMutation, UnsetDomainMutationVariables>;

/**
 * __useUnsetDomainMutation__
 *
 * To run a mutation, you first call `useUnsetDomainMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUnsetDomainMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [unsetDomainMutation, { data, loading, error }] = useUnsetDomainMutation({
 *   variables: {
 *      entityUrn: // value for 'entityUrn'
 *   },
 * });
 */
export function useUnsetDomainMutation(
    baseOptions?: Apollo.MutationHookOptions<UnsetDomainMutation, UnsetDomainMutationVariables>,
) {
    return Apollo.useMutation<UnsetDomainMutation, UnsetDomainMutationVariables>(UnsetDomainDocument, baseOptions);
}
export type UnsetDomainMutationHookResult = ReturnType<typeof useUnsetDomainMutation>;
export type UnsetDomainMutationResult = Apollo.MutationResult<UnsetDomainMutation>;
export type UnsetDomainMutationOptions = Apollo.BaseMutationOptions<UnsetDomainMutation, UnsetDomainMutationVariables>;
export const SetTagColorDocument = gql`
    mutation setTagColor($urn: String!, $colorHex: String!) {
        setTagColor(urn: $urn, colorHex: $colorHex)
    }
`;
export type SetTagColorMutationFn = Apollo.MutationFunction<SetTagColorMutation, SetTagColorMutationVariables>;

/**
 * __useSetTagColorMutation__
 *
 * To run a mutation, you first call `useSetTagColorMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useSetTagColorMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [setTagColorMutation, { data, loading, error }] = useSetTagColorMutation({
 *   variables: {
 *      urn: // value for 'urn'
 *      colorHex: // value for 'colorHex'
 *   },
 * });
 */
export function useSetTagColorMutation(
    baseOptions?: Apollo.MutationHookOptions<SetTagColorMutation, SetTagColorMutationVariables>,
) {
    return Apollo.useMutation<SetTagColorMutation, SetTagColorMutationVariables>(SetTagColorDocument, baseOptions);
}
export type SetTagColorMutationHookResult = ReturnType<typeof useSetTagColorMutation>;
export type SetTagColorMutationResult = Apollo.MutationResult<SetTagColorMutation>;
export type SetTagColorMutationOptions = Apollo.BaseMutationOptions<SetTagColorMutation, SetTagColorMutationVariables>;
export const UpdateDeprecationDocument = gql`
    mutation updateDeprecation($input: UpdateDeprecationInput!) {
        updateDeprecation(input: $input)
    }
`;
export type UpdateDeprecationMutationFn = Apollo.MutationFunction<
    UpdateDeprecationMutation,
    UpdateDeprecationMutationVariables
>;

/**
 * __useUpdateDeprecationMutation__
 *
 * To run a mutation, you first call `useUpdateDeprecationMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateDeprecationMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateDeprecationMutation, { data, loading, error }] = useUpdateDeprecationMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateDeprecationMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateDeprecationMutation, UpdateDeprecationMutationVariables>,
) {
    return Apollo.useMutation<UpdateDeprecationMutation, UpdateDeprecationMutationVariables>(
        UpdateDeprecationDocument,
        baseOptions,
    );
}
export type UpdateDeprecationMutationHookResult = ReturnType<typeof useUpdateDeprecationMutation>;
export type UpdateDeprecationMutationResult = Apollo.MutationResult<UpdateDeprecationMutation>;
export type UpdateDeprecationMutationOptions = Apollo.BaseMutationOptions<
    UpdateDeprecationMutation,
    UpdateDeprecationMutationVariables
>;
export const AddOwnersDocument = gql`
    mutation addOwners($input: AddOwnersInput!) {
        addOwners(input: $input)
    }
`;
export type AddOwnersMutationFn = Apollo.MutationFunction<AddOwnersMutation, AddOwnersMutationVariables>;

/**
 * __useAddOwnersMutation__
 *
 * To run a mutation, you first call `useAddOwnersMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useAddOwnersMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [addOwnersMutation, { data, loading, error }] = useAddOwnersMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useAddOwnersMutation(
    baseOptions?: Apollo.MutationHookOptions<AddOwnersMutation, AddOwnersMutationVariables>,
) {
    return Apollo.useMutation<AddOwnersMutation, AddOwnersMutationVariables>(AddOwnersDocument, baseOptions);
}
export type AddOwnersMutationHookResult = ReturnType<typeof useAddOwnersMutation>;
export type AddOwnersMutationResult = Apollo.MutationResult<AddOwnersMutation>;
export type AddOwnersMutationOptions = Apollo.BaseMutationOptions<AddOwnersMutation, AddOwnersMutationVariables>;
export const AddTagsDocument = gql`
    mutation addTags($input: AddTagsInput!) {
        addTags(input: $input)
    }
`;
export type AddTagsMutationFn = Apollo.MutationFunction<AddTagsMutation, AddTagsMutationVariables>;

/**
 * __useAddTagsMutation__
 *
 * To run a mutation, you first call `useAddTagsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useAddTagsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [addTagsMutation, { data, loading, error }] = useAddTagsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useAddTagsMutation(
    baseOptions?: Apollo.MutationHookOptions<AddTagsMutation, AddTagsMutationVariables>,
) {
    return Apollo.useMutation<AddTagsMutation, AddTagsMutationVariables>(AddTagsDocument, baseOptions);
}
export type AddTagsMutationHookResult = ReturnType<typeof useAddTagsMutation>;
export type AddTagsMutationResult = Apollo.MutationResult<AddTagsMutation>;
export type AddTagsMutationOptions = Apollo.BaseMutationOptions<AddTagsMutation, AddTagsMutationVariables>;
export const AddTermsDocument = gql`
    mutation addTerms($input: AddTermsInput!) {
        addTerms(input: $input)
    }
`;
export type AddTermsMutationFn = Apollo.MutationFunction<AddTermsMutation, AddTermsMutationVariables>;

/**
 * __useAddTermsMutation__
 *
 * To run a mutation, you first call `useAddTermsMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useAddTermsMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [addTermsMutation, { data, loading, error }] = useAddTermsMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useAddTermsMutation(
    baseOptions?: Apollo.MutationHookOptions<AddTermsMutation, AddTermsMutationVariables>,
) {
    return Apollo.useMutation<AddTermsMutation, AddTermsMutationVariables>(AddTermsDocument, baseOptions);
}
export type AddTermsMutationHookResult = ReturnType<typeof useAddTermsMutation>;
export type AddTermsMutationResult = Apollo.MutationResult<AddTermsMutation>;
export type AddTermsMutationOptions = Apollo.BaseMutationOptions<AddTermsMutation, AddTermsMutationVariables>;
export const UpdateNameDocument = gql`
    mutation updateName($input: UpdateNameInput!) {
        updateName(input: $input)
    }
`;
export type UpdateNameMutationFn = Apollo.MutationFunction<UpdateNameMutation, UpdateNameMutationVariables>;

/**
 * __useUpdateNameMutation__
 *
 * To run a mutation, you first call `useUpdateNameMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateNameMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateNameMutation, { data, loading, error }] = useUpdateNameMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateNameMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateNameMutation, UpdateNameMutationVariables>,
) {
    return Apollo.useMutation<UpdateNameMutation, UpdateNameMutationVariables>(UpdateNameDocument, baseOptions);
}
export type UpdateNameMutationHookResult = ReturnType<typeof useUpdateNameMutation>;
export type UpdateNameMutationResult = Apollo.MutationResult<UpdateNameMutation>;
export type UpdateNameMutationOptions = Apollo.BaseMutationOptions<UpdateNameMutation, UpdateNameMutationVariables>;
export const BatchSetDomainDocument = gql`
    mutation batchSetDomain($input: BatchSetDomainInput!) {
        batchSetDomain(input: $input)
    }
`;
export type BatchSetDomainMutationFn = Apollo.MutationFunction<BatchSetDomainMutation, BatchSetDomainMutationVariables>;

/**
 * __useBatchSetDomainMutation__
 *
 * To run a mutation, you first call `useBatchSetDomainMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchSetDomainMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchSetDomainMutation, { data, loading, error }] = useBatchSetDomainMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchSetDomainMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchSetDomainMutation, BatchSetDomainMutationVariables>,
) {
    return Apollo.useMutation<BatchSetDomainMutation, BatchSetDomainMutationVariables>(
        BatchSetDomainDocument,
        baseOptions,
    );
}
export type BatchSetDomainMutationHookResult = ReturnType<typeof useBatchSetDomainMutation>;
export type BatchSetDomainMutationResult = Apollo.MutationResult<BatchSetDomainMutation>;
export type BatchSetDomainMutationOptions = Apollo.BaseMutationOptions<
    BatchSetDomainMutation,
    BatchSetDomainMutationVariables
>;
export const BatchUpdateDeprecationDocument = gql`
    mutation batchUpdateDeprecation($input: BatchUpdateDeprecationInput!) {
        batchUpdateDeprecation(input: $input)
    }
`;
export type BatchUpdateDeprecationMutationFn = Apollo.MutationFunction<
    BatchUpdateDeprecationMutation,
    BatchUpdateDeprecationMutationVariables
>;

/**
 * __useBatchUpdateDeprecationMutation__
 *
 * To run a mutation, you first call `useBatchUpdateDeprecationMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchUpdateDeprecationMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchUpdateDeprecationMutation, { data, loading, error }] = useBatchUpdateDeprecationMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchUpdateDeprecationMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchUpdateDeprecationMutation, BatchUpdateDeprecationMutationVariables>,
) {
    return Apollo.useMutation<BatchUpdateDeprecationMutation, BatchUpdateDeprecationMutationVariables>(
        BatchUpdateDeprecationDocument,
        baseOptions,
    );
}
export type BatchUpdateDeprecationMutationHookResult = ReturnType<typeof useBatchUpdateDeprecationMutation>;
export type BatchUpdateDeprecationMutationResult = Apollo.MutationResult<BatchUpdateDeprecationMutation>;
export type BatchUpdateDeprecationMutationOptions = Apollo.BaseMutationOptions<
    BatchUpdateDeprecationMutation,
    BatchUpdateDeprecationMutationVariables
>;
export const BatchUpdateSoftDeletedDocument = gql`
    mutation batchUpdateSoftDeleted($input: BatchUpdateSoftDeletedInput!) {
        batchUpdateSoftDeleted(input: $input)
    }
`;
export type BatchUpdateSoftDeletedMutationFn = Apollo.MutationFunction<
    BatchUpdateSoftDeletedMutation,
    BatchUpdateSoftDeletedMutationVariables
>;

/**
 * __useBatchUpdateSoftDeletedMutation__
 *
 * To run a mutation, you first call `useBatchUpdateSoftDeletedMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchUpdateSoftDeletedMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchUpdateSoftDeletedMutation, { data, loading, error }] = useBatchUpdateSoftDeletedMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchUpdateSoftDeletedMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchUpdateSoftDeletedMutation, BatchUpdateSoftDeletedMutationVariables>,
) {
    return Apollo.useMutation<BatchUpdateSoftDeletedMutation, BatchUpdateSoftDeletedMutationVariables>(
        BatchUpdateSoftDeletedDocument,
        baseOptions,
    );
}
export type BatchUpdateSoftDeletedMutationHookResult = ReturnType<typeof useBatchUpdateSoftDeletedMutation>;
export type BatchUpdateSoftDeletedMutationResult = Apollo.MutationResult<BatchUpdateSoftDeletedMutation>;
export type BatchUpdateSoftDeletedMutationOptions = Apollo.BaseMutationOptions<
    BatchUpdateSoftDeletedMutation,
    BatchUpdateSoftDeletedMutationVariables
>;
export const BatchAssignRoleDocument = gql`
    mutation batchAssignRole($input: BatchAssignRoleInput!) {
        batchAssignRole(input: $input)
    }
`;
export type BatchAssignRoleMutationFn = Apollo.MutationFunction<
    BatchAssignRoleMutation,
    BatchAssignRoleMutationVariables
>;

/**
 * __useBatchAssignRoleMutation__
 *
 * To run a mutation, you first call `useBatchAssignRoleMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useBatchAssignRoleMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [batchAssignRoleMutation, { data, loading, error }] = useBatchAssignRoleMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useBatchAssignRoleMutation(
    baseOptions?: Apollo.MutationHookOptions<BatchAssignRoleMutation, BatchAssignRoleMutationVariables>,
) {
    return Apollo.useMutation<BatchAssignRoleMutation, BatchAssignRoleMutationVariables>(
        BatchAssignRoleDocument,
        baseOptions,
    );
}
export type BatchAssignRoleMutationHookResult = ReturnType<typeof useBatchAssignRoleMutation>;
export type BatchAssignRoleMutationResult = Apollo.MutationResult<BatchAssignRoleMutation>;
export type BatchAssignRoleMutationOptions = Apollo.BaseMutationOptions<
    BatchAssignRoleMutation,
    BatchAssignRoleMutationVariables
>;
export const CreateInviteTokenDocument = gql`
    mutation createInviteToken($input: CreateInviteTokenInput!) {
        createInviteToken(input: $input) {
            inviteToken
        }
    }
`;
export type CreateInviteTokenMutationFn = Apollo.MutationFunction<
    CreateInviteTokenMutation,
    CreateInviteTokenMutationVariables
>;

/**
 * __useCreateInviteTokenMutation__
 *
 * To run a mutation, you first call `useCreateInviteTokenMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreateInviteTokenMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createInviteTokenMutation, { data, loading, error }] = useCreateInviteTokenMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreateInviteTokenMutation(
    baseOptions?: Apollo.MutationHookOptions<CreateInviteTokenMutation, CreateInviteTokenMutationVariables>,
) {
    return Apollo.useMutation<CreateInviteTokenMutation, CreateInviteTokenMutationVariables>(
        CreateInviteTokenDocument,
        baseOptions,
    );
}
export type CreateInviteTokenMutationHookResult = ReturnType<typeof useCreateInviteTokenMutation>;
export type CreateInviteTokenMutationResult = Apollo.MutationResult<CreateInviteTokenMutation>;
export type CreateInviteTokenMutationOptions = Apollo.BaseMutationOptions<
    CreateInviteTokenMutation,
    CreateInviteTokenMutationVariables
>;
export const AcceptRoleDocument = gql`
    mutation acceptRole($input: AcceptRoleInput!) {
        acceptRole(input: $input)
    }
`;
export type AcceptRoleMutationFn = Apollo.MutationFunction<AcceptRoleMutation, AcceptRoleMutationVariables>;

/**
 * __useAcceptRoleMutation__
 *
 * To run a mutation, you first call `useAcceptRoleMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useAcceptRoleMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [acceptRoleMutation, { data, loading, error }] = useAcceptRoleMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useAcceptRoleMutation(
    baseOptions?: Apollo.MutationHookOptions<AcceptRoleMutation, AcceptRoleMutationVariables>,
) {
    return Apollo.useMutation<AcceptRoleMutation, AcceptRoleMutationVariables>(AcceptRoleDocument, baseOptions);
}
export type AcceptRoleMutationHookResult = ReturnType<typeof useAcceptRoleMutation>;
export type AcceptRoleMutationResult = Apollo.MutationResult<AcceptRoleMutation>;
export type AcceptRoleMutationOptions = Apollo.BaseMutationOptions<AcceptRoleMutation, AcceptRoleMutationVariables>;
export const CreatePostDocument = gql`
    mutation createPost($input: CreatePostInput!) {
        createPost(input: $input)
    }
`;
export type CreatePostMutationFn = Apollo.MutationFunction<CreatePostMutation, CreatePostMutationVariables>;

/**
 * __useCreatePostMutation__
 *
 * To run a mutation, you first call `useCreatePostMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useCreatePostMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [createPostMutation, { data, loading, error }] = useCreatePostMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useCreatePostMutation(
    baseOptions?: Apollo.MutationHookOptions<CreatePostMutation, CreatePostMutationVariables>,
) {
    return Apollo.useMutation<CreatePostMutation, CreatePostMutationVariables>(CreatePostDocument, baseOptions);
}
export type CreatePostMutationHookResult = ReturnType<typeof useCreatePostMutation>;
export type CreatePostMutationResult = Apollo.MutationResult<CreatePostMutation>;
export type CreatePostMutationOptions = Apollo.BaseMutationOptions<CreatePostMutation, CreatePostMutationVariables>;
export const UpdateLineageDocument = gql`
    mutation updateLineage($input: UpdateLineageInput!) {
        updateLineage(input: $input)
    }
`;
export type UpdateLineageMutationFn = Apollo.MutationFunction<UpdateLineageMutation, UpdateLineageMutationVariables>;

/**
 * __useUpdateLineageMutation__
 *
 * To run a mutation, you first call `useUpdateLineageMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateLineageMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateLineageMutation, { data, loading, error }] = useUpdateLineageMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateLineageMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateLineageMutation, UpdateLineageMutationVariables>,
) {
    return Apollo.useMutation<UpdateLineageMutation, UpdateLineageMutationVariables>(
        UpdateLineageDocument,
        baseOptions,
    );
}
export type UpdateLineageMutationHookResult = ReturnType<typeof useUpdateLineageMutation>;
export type UpdateLineageMutationResult = Apollo.MutationResult<UpdateLineageMutation>;
export type UpdateLineageMutationOptions = Apollo.BaseMutationOptions<
    UpdateLineageMutation,
    UpdateLineageMutationVariables
>;
export const UpdateEmbedDocument = gql`
    mutation updateEmbed($input: UpdateEmbedInput!) {
        updateEmbed(input: $input)
    }
`;
export type UpdateEmbedMutationFn = Apollo.MutationFunction<UpdateEmbedMutation, UpdateEmbedMutationVariables>;

/**
 * __useUpdateEmbedMutation__
 *
 * To run a mutation, you first call `useUpdateEmbedMutation` within a React component and pass it any options that fit your needs.
 * When your component renders, `useUpdateEmbedMutation` returns a tuple that includes:
 * - A mutate function that you can call at any time to execute the mutation
 * - An object with fields that represent the current status of the mutation's execution
 *
 * @param baseOptions options that will be passed into the mutation, supported options are listed on: https://www.apollographql.com/docs/react/api/react-hooks/#options-2;
 *
 * @example
 * const [updateEmbedMutation, { data, loading, error }] = useUpdateEmbedMutation({
 *   variables: {
 *      input: // value for 'input'
 *   },
 * });
 */
export function useUpdateEmbedMutation(
    baseOptions?: Apollo.MutationHookOptions<UpdateEmbedMutation, UpdateEmbedMutationVariables>,
) {
    return Apollo.useMutation<UpdateEmbedMutation, UpdateEmbedMutationVariables>(UpdateEmbedDocument, baseOptions);
}
export type UpdateEmbedMutationHookResult = ReturnType<typeof useUpdateEmbedMutation>;
export type UpdateEmbedMutationResult = Apollo.MutationResult<UpdateEmbedMutation>;
export type UpdateEmbedMutationOptions = Apollo.BaseMutationOptions<UpdateEmbedMutation, UpdateEmbedMutationVariables>;
