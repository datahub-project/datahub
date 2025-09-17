import React, { useCallback, useMemo } from 'react';

import { useModulesContext } from '@app/homeV3/module/context/ModulesContext';
import { ModuleContextType, ModuleProps } from '@app/homeV3/module/types';

const ModuleContext = React.createContext<ModuleContextType>({
    isReloading: false,
    onReloadingFinished: () => {},
});

export function ModuleProvider({ children, module }: React.PropsWithChildren<ModuleProps>) {
    const { shouldModuleBeReloaded, markModulesAsReloaded } = useModulesContext();

    const moduleUrn = module.urn;
    const moduleType = module.properties.type;

    const isReloading = useMemo(
        () => shouldModuleBeReloaded(moduleType, moduleUrn),
        [shouldModuleBeReloaded, moduleUrn, moduleType],
    );

    const onReloadingFinished = useCallback(() => {
        if (isReloading) markModulesAsReloaded(moduleType, moduleUrn);
    }, [markModulesAsReloaded, moduleUrn, moduleType, isReloading]);

    return <ModuleContext.Provider value={{ isReloading, onReloadingFinished }}>{children}</ModuleContext.Provider>;
}

export function useModuleContext() {
    return React.useContext(ModuleContext);
}
