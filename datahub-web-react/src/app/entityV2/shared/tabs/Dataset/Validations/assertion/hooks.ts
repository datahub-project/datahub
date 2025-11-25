import type { MutationHookOptions } from '@apollo/client';

import { useDeleteAssertionMutation } from '@graphql/assertion.generated';
import type { DeleteAssertionMutation, DeleteAssertionMutationVariables } from '@graphql/assertion.generated';

/**
 * @description A custom hook for deleting an assertion with cache eviction.
 * Use this instead of the default useDeleteAssertionMutation hook.
 */
export const useDeleteAssertionMutationWithCache = (
    baseOptions?: MutationHookOptions<DeleteAssertionMutation, DeleteAssertionMutationVariables>,
) => {
    const [deleteAssertion, result] = useDeleteAssertionMutation({
        ...baseOptions,
        update(cache, response) {
            baseOptions?.update?.(cache, response);
            const assertionUrn = baseOptions?.variables?.urn;
            if (response.data?.deleteAssertion && assertionUrn) {
                cache.evict({
                    id: cache.identify({
                        __typename: 'Assertion',
                        urn: assertionUrn,
                    }),
                });
                cache.gc();
            }
        },
    });

    return [deleteAssertion, result] as const; // Add 'as const' for tuple type
};
