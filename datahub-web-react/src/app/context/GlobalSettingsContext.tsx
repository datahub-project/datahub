import React, { useContext } from 'react';

import { GetHomePageSettingsQuery } from '@graphql/app.generated';

export const DEFAULT_GLOBAL_SETTINGS = {
    globalHomePageSettings: {
        defaultTemplate: null,
    },
};

export const GlobalSettingsContext = React.createContext<{
    settings: GetHomePageSettingsQuery;
    loaded: boolean;
}>({ settings: DEFAULT_GLOBAL_SETTINGS, loaded: false });

export function useGlobalSettings() {
    return useContext(GlobalSettingsContext);
}
