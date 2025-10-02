import { LazyQueryHookOptions, QueryHookOptions, QueryResult, QueryTuple } from '@apollo/client';
import React, { useCallback, useEffect, useState } from 'react';

interface ReloadableContextType {
    reloadByKeyType: (keysTypes: string[], delayMs?: number) => void;
    reloaded: (keyType: string, keyId?: string, delayMs?: number) => void;
    shouldBeReloaded: (keyType: string, keyId?: string) => boolean;
}

const DEFAULT_CONTEXT: ReloadableContextType = {
    reloadByKeyType: () => {},
    reloaded: () => {},
    shouldBeReloaded: () => false,
};

const ReloadableContext = React.createContext<ReloadableContextType>(DEFAULT_CONTEXT);

interface Props {
    children: React.ReactNode;
}

export function useReloadableContext() {
    return React.useContext<ReloadableContextType>(ReloadableContext);
}

const KEY_SEPARATOR = '|';

export const getReloadableKey = (keyType: string, entryId?: string) => `${keyType}${KEY_SEPARATOR}${entryId ?? ''}`;

export function ReloadableProvider({ children }: Props) {
    const [reloadedKeys, setReloadedKeys] = useState<Set<string>>(new Set());

    const reloadByKeyType = useCallback((keyTypes: string[], delayMs?: number) => {
        const removeKeys = () => {
            setReloadedKeys((prev) => {
                const filteredKeys = Array.from(prev).filter(
                    (key) => !keyTypes.some((type) => key.startsWith(`${type}${KEY_SEPARATOR}`)),
                );
                return new Set(filteredKeys);
            });
        };

        if (!delayMs) {
            removeKeys();
        } else {
            setTimeout(() => removeKeys(), delayMs);
        }
    }, []);

    const reloaded = useCallback((keyType: string, entryId?: string) => {
        const key = getReloadableKey(keyType, entryId);
        setReloadedKeys((prev) => new Set(prev).add(key));
    }, []);

    const shouldBeReloaded = useCallback(
        (keyType: string, entryId?: string) => {
            const key = getReloadableKey(keyType, entryId);
            return !reloadedKeys.has(key);
        },
        [reloadedKeys],
    );

    return (
        <ReloadableContext.Provider value={{ reloadByKeyType, reloaded, shouldBeReloaded }}>
            {children}
        </ReloadableContext.Provider>
    );
}

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

export function useReloadableLazyQuery<T, K>(
    lazyQueryHook: (options: LazyQueryHookOptions<T, K>) => QueryTuple<T, K>,
    key: { type: string; id?: string },
    options: LazyQueryHookOptions<T, K>,
): QueryTuple<T, K> {
    const { shouldBeReloaded, reloaded } = useReloadableContext();
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
            reloaded(key.type, key.id);
        }
    }, [result.loading, result.error, reloaded, key.type, key.id]);

    return [wrappedExecute, result];
}
