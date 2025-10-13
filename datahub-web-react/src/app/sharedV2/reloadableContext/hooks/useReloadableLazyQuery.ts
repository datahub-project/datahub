import { LazyQueryHookOptions, QueryTuple } from '@apollo/client';
import { useCallback, useEffect } from 'react';

import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';

export function useReloadableLazyQuery<T, K>(
    lazyQueryHook: (options: LazyQueryHookOptions<T, K>) => QueryTuple<T, K>,
    key: { type: string; id?: string },
    options: LazyQueryHookOptions<T, K>,
): QueryTuple<T, K> {
    const { shouldBeReloaded, markAsReloaded } = useReloadableContext();
    const [execute, result] = lazyQueryHook(options);

    const wrappedExecute = useCallback(
        (overrideOptions?: LazyQueryHookOptions<T, K>) => {
            const needsReload = shouldBeReloaded(key.type, key.id);
            const finalOptions = {
                ...overrideOptions,
                fetchPolicy: needsReload ? 'cache-and-network' : overrideOptions?.fetchPolicy || options.fetchPolicy,
            };
            return execute(finalOptions);
        },
        [execute, shouldBeReloaded, key, options],
    );

    useEffect(() => {
        if (!result.loading && !result.error) {
            markAsReloaded(key.type, key.id);
        }
    }, [result.loading, result.error, markAsReloaded, key.type, key.id]);

    return [wrappedExecute, result];
}
