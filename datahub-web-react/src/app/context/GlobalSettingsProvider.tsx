import React from 'react';

import { DEFAULT_GLOBAL_SETTINGS, GlobalSettingsContext } from '@app/context/GlobalSettingsContext';

import { useGetHomePageSettingsQuery } from '@graphql/app.generated';

export default function GlobalSettingsProvider({ children }: { children: React.ReactNode }) {
    const { data } = useGetHomePageSettingsQuery();

    return (
        <GlobalSettingsContext.Provider
            value={{
                settings: data || DEFAULT_GLOBAL_SETTINGS,
                loaded: !!data,
            }}
        >
            {children}
        </GlobalSettingsContext.Provider>
    );
}
