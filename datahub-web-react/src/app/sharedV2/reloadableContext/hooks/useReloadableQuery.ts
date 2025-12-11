/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { QueryHookOptions, QueryResult } from '@apollo/client';
import { useEffect } from 'react';

import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';

export function useReloadableQuery<T, K>(
    queryHook: (options: QueryHookOptions<T, K>) => QueryResult<T, K>,
    key: { type: string; id?: string },
    options: QueryHookOptions<T, K>,
): QueryResult<T, K> {
    const { shouldBeReloaded, markAsReloaded } = useReloadableContext();
    const needsReload = shouldBeReloaded(key.type, key.id);
    const result = queryHook({
        ...options,
        fetchPolicy: needsReload ? 'cache-and-network' : options.fetchPolicy,
    });

    useEffect(() => {
        if (!result.loading && !result.error) {
            markAsReloaded(key.type, key.id);
        }
    }, [result.loading, result.error, markAsReloaded, key.type, key.id]);

    return result;
}
