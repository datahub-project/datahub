import React, { useContext } from 'react';

import { GlobalSettings } from '@types';

export type GlobalSettingsContextType = {
    globalSettings?: GlobalSettings;
    refetch: () => void;
    loading: boolean;
};

export const GlobalSettingsContext = React.createContext<GlobalSettingsContextType>({
    refetch: () => null,
    loading: true,
});

export function useGlobalSettingsContext() {
    return useContext(GlobalSettingsContext);
}
