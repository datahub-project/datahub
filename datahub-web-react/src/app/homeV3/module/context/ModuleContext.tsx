import React, { useCallback, useMemo } from 'react';

import { ModuleContextType, ModuleProps } from '@app/homeV3/module/types';
import { getReloadableModuleKey } from '@app/homeV3/modules/utils';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';

const ModuleContext = React.createContext<ModuleContextType>({
    isReloading: false,
    onReloadingFinished: () => {},
});

export function ModuleProvider({ children, module }: React.PropsWithChildren<ModuleProps>) {
    const { shouldBeReloaded, markAsReloaded } = useReloadableContext();

    const moduleUrn = module.urn;
    const moduleType = module.properties.type;

    const isReloading = useMemo(
        () => shouldBeReloaded(getReloadableModuleKey(moduleType), moduleUrn),
        [shouldBeReloaded, moduleUrn, moduleType],
    );

    const onReloadingFinished = useCallback(() => {
        if (isReloading) markAsReloaded(getReloadableModuleKey(moduleType), moduleUrn);
    }, [markAsReloaded, moduleUrn, moduleType, isReloading]);

    return <ModuleContext.Provider value={{ isReloading, onReloadingFinished }}>{children}</ModuleContext.Provider>;
}

export function useModuleContext() {
    return React.useContext(ModuleContext);
}
