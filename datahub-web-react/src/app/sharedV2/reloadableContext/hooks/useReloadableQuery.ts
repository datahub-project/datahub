import { QueryHookOptions, QueryResult } from '@apollo/client';
import { useEffect } from 'react';

import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';

export function useReloadableQuery<T, K>(
    queryHook: (options: QueryHookOptions<T, K>) => QueryResult<T, K>,
    key: { type: string; id?: string },
    options: QueryHookOptions<T, K>,
): QueryResult<T, K> {
    const { shouldBeReloaded, reloaded } = useReloadableContext();
    const needsReload = shouldBeReloaded(key.type, key.id);
    const result = queryHook({
        ...options,
        fetchPolicy: needsReload ? 'cache-and-network' : options.fetchPolicy,
    });

    useEffect(() => {
        if (!result.loading && !result.error) {
            reloaded(key.type, key.id);
        }
    }, [result.loading, result.error, reloaded, key.type, key.id]);

    return result;
}
