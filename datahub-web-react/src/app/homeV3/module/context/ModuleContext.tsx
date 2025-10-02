import React, { useCallback, useMemo } from 'react';

import { ModuleContextType, ModuleProps } from '@app/homeV3/module/types';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';

const ModuleContext = React.createContext<ModuleContextType>({
    isReloading: false,
    onReloadingFinished: () => {},
});

export function ModuleProvider({ children, module }: React.PropsWithChildren<ModuleProps>) {
    const { shouldBeReloaded, reloaded } = useReloadableContext();

    const moduleUrn = module.urn;
    const moduleType = module.properties.type;

    const isReloading = useMemo(
        () => shouldBeReloaded(moduleType, moduleUrn),
        [shouldBeReloaded, moduleUrn, moduleType],
    );

    const onReloadingFinished = useCallback(() => {
        if (isReloading) reloaded(moduleType, moduleUrn);
    }, [reloaded, moduleUrn, moduleType, isReloading]);

    return <ModuleContext.Provider value={{ isReloading, onReloadingFinished }}>{children}</ModuleContext.Provider>;
}

export function useModuleContext() {
    return React.useContext(ModuleContext);
}
