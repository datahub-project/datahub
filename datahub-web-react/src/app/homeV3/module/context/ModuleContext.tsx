import React, { useCallback, useMemo } from 'react';

import { ModuleContextType, ModuleProps, ModuleSize } from '@app/homeV3/module/types';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';

const ModuleContext = React.createContext<ModuleContextType>({
    isReloading: false,
    onReloadingFinished: () => {},
});

export function ModuleProvider({ children, module, position }: React.PropsWithChildren<ModuleProps>) {
    const { shouldBeReloaded, markAsReloaded } = useReloadableContext();

    const moduleUrn = module.urn;
    const moduleType = module.properties.type;

    const isReloading = useMemo(
        () => shouldBeReloaded(getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, moduleType), moduleUrn),
        [shouldBeReloaded, moduleUrn, moduleType],
    );

    const onReloadingFinished = useCallback(() => {
        if (isReloading) markAsReloaded(getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, moduleType), moduleUrn);
    }, [markAsReloaded, moduleUrn, moduleType, isReloading]);

    const size = useMemo(() => {
        if (position.numberOfModulesInRow === 1) {
            return ModuleSize.FULL;
        }

        if (position.numberOfModulesInRow === 2) {
            return ModuleSize.HALF;
        }

        if (position.numberOfModulesInRow === 3) {
            return ModuleSize.THIRD;
        }

        return ModuleSize.THIRD; // default case
    }, [position.numberOfModulesInRow]);

    return (
        <ModuleContext.Provider value={{ isReloading, onReloadingFinished, size }}>{children}</ModuleContext.Provider>
    );
}

export function useModuleContext() {
    return React.useContext(ModuleContext);
}
