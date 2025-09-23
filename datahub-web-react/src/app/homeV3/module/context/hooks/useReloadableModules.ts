import { useCallback, useState } from 'react';

import { DataHubPageModuleType } from '@types';

const getModuleKey = (moduleType: DataHubPageModuleType, moduleUrn: string) => {
    return `${moduleType}|${moduleUrn}`;
};

export function useReloadableModules() {
    const [loadedModules, setLoadedModules] = useState<Set<string>>(() => new Set());

    const reloadModules = useCallback((moduleTypes: DataHubPageModuleType[], intervalMs = 0) => {
        const reload = () => {
            setLoadedModules((prev) => {
                const filteredKeys = Array.from(prev).filter(
                    (key) => !moduleTypes.some((type) => key.startsWith(`${type}|`)),
                );
                return new Set(filteredKeys);
            });
        };

        if (!intervalMs) {
            reload();
        } else {
            setTimeout(() => reload(), intervalMs);
        }
    }, []);

    const shouldModuleBeReloaded = useCallback(
        (moduleType: DataHubPageModuleType, moduleUrn: string) => {
            const key = getModuleKey(moduleType, moduleUrn);
            return !loadedModules.has(key);
        },
        [loadedModules],
    );

    const markModulesAsReloaded = useCallback((moduleType: DataHubPageModuleType, moduleUrn: string) => {
        const key = getModuleKey(moduleType, moduleUrn);
        setLoadedModules((prev) => new Set(prev).add(key));
    }, []);

    return {
        reloadModules,
        shouldModuleBeReloaded,
        markModulesAsReloaded,
    };
}
