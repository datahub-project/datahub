import React, { ReactNode } from 'react';

import { useReloadableModules } from '@app/homeV3/module/context/hooks/useReloadableModules';
import { ModulesContextType } from '@app/homeV3/module/types';

const ModulesContext = React.createContext<ModulesContextType>({
    reloadModules: () => {},
    shouldModuleBeReloaded: () => false,
    markModulesAsReloaded: () => {},
});

interface Props {
    children: ReactNode;
}

export function ModulesProvider({ children }: Props) {
    const { reloadModules, shouldModuleBeReloaded, markModulesAsReloaded } = useReloadableModules();

    return (
        <ModulesContext.Provider
            value={{
                reloadModules,
                shouldModuleBeReloaded,
                markModulesAsReloaded,
            }}
        >
            {children}
        </ModulesContext.Provider>
    );
}

export function useModulesContext() {
    return React.useContext(ModulesContext);
}
