import React, { useCallback, useState } from 'react';

import { ReloadableContextType } from '@app/sharedV2/reloadableContext/types';
import { KEY_SEPARATOR, getReloadableKey } from '@app/sharedV2/reloadableContext/utils';

const DEFAULT_CONTEXT: ReloadableContextType = {
    reloadByKeyType: () => {},
    markAsReloaded: () => {},
    shouldBeReloaded: () => false,
    bypassCacheForUrn: () => {},
    shouldBypassCache: () => false,
    clearCacheBypass: () => {},
};

export const ReloadableContext = React.createContext<ReloadableContextType>(DEFAULT_CONTEXT);

interface Props {
    children: React.ReactNode;
}

export function ReloadableProvider({ children }: Props) {
    const [reloadedKeys, setReloadedKeys] = useState<Set<string>>(new Set());
    const [cacheBypassUrns, setCacheBypassUrns] = useState<Set<string>>(new Set());

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

    const markAsReloaded = useCallback((keyType: string, entryId?: string) => {
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

    const bypassCacheForUrn = useCallback((urn: string) => {
        setCacheBypassUrns((prev) => new Set(prev).add(urn));
    }, []);

    const shouldBypassCache = useCallback(
        (urn: string) => {
            return cacheBypassUrns.has(urn);
        },
        [cacheBypassUrns],
    );

    const clearCacheBypass = useCallback((urn: string) => {
        setCacheBypassUrns((prev) => {
            const next = new Set(prev);
            next.delete(urn);
            return next;
        });
    }, []);

    return (
        <ReloadableContext.Provider
            value={{
                reloadByKeyType,
                markAsReloaded,
                shouldBeReloaded,
                bypassCacheForUrn,
                shouldBypassCache,
                clearCacheBypass,
            }}
        >
            {children}
        </ReloadableContext.Provider>
    );
}
