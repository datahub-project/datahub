import { useCallback, useState } from 'react';

import { LoadedModulesEntry } from '@app/homeV3/module/types';

import { DataHubPageModuleType } from '@types';

const getLoadModulesEntry = (moduleType: DataHubPageModuleType, moduleUrn?: string) => {
    const newEntry: LoadedModulesEntry = {
        urn: moduleUrn || '',
        type: moduleType,
    };

    return newEntry;
};

export function useReloadableModules() {
    const [loadedModules, setLoadedModules] = useState<LoadedModulesEntry[]>([]);

    const reloadModules = useCallback((moduleTypes: DataHubPageModuleType[], intervalMs = 0) => {
        const reload = () => {
            setLoadedModules((prev) => prev.filter((entry) => !moduleTypes.includes(entry.type)));
        };

        if (!intervalMs) {
            reload();
        } else {
            setTimeout(() => reload(), intervalMs);
        }
    }, []);

    const shouldModuleBeReloaded = useCallback(
        (moduleType: DataHubPageModuleType, moduleUrn?: string) => {
            return !loadedModules.some((entry) => entry.urn === (moduleUrn || '') && entry.type === moduleType);
        },
        [loadedModules],
    );

    const markModulesAsReloaded = useCallback((moduleType: DataHubPageModuleType, moduleUrn?: string) => {
        const newEntry = getLoadModulesEntry(moduleType, moduleUrn);
        setLoadedModules((prev) => [...prev, newEntry]);
    }, []);

    return {
        reloadModules,
        shouldModuleBeReloaded,
        markModulesAsReloaded,
    };
}
