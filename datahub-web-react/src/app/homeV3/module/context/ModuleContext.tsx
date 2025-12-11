/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useCallback, useMemo } from 'react';

import { ModuleContextType, ModuleProps } from '@app/homeV3/module/types';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';

const ModuleContext = React.createContext<ModuleContextType>({
    isReloading: false,
    onReloadingFinished: () => {},
});

export function ModuleProvider({ children, module }: React.PropsWithChildren<ModuleProps>) {
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

    return <ModuleContext.Provider value={{ isReloading, onReloadingFinished }}>{children}</ModuleContext.Provider>;
}

export function useModuleContext() {
    return React.useContext(ModuleContext);
}
