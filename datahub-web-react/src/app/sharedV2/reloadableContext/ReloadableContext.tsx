import React, { useCallback, useState } from 'react';

import { ReloadableContextType } from '@app/sharedV2/reloadableContext/types';
import { KEY_SEPARATOR, getReloadableKey } from '@app/sharedV2/reloadableContext/utils';

const DEFAULT_CONTEXT: ReloadableContextType = {
    reloadByKeyType: () => {},
    markAsReloaded: () => {},
    shouldBeReloaded: () => false,
};

export const ReloadableContext = React.createContext<ReloadableContextType>(DEFAULT_CONTEXT);

interface Props {
    children: React.ReactNode;
}

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

    return (
        <ReloadableContext.Provider value={{ reloadByKeyType, markAsReloaded, shouldBeReloaded }}>
            {children}
        </ReloadableContext.Provider>
    );
}
