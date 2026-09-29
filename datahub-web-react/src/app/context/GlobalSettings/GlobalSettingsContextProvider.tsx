import React from 'react';

import { GlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';

import { useGetGlobalSettingsQuery } from '@graphql/settings.generated';

export default function GlobalSettingsContextProvider({ children }: { children: React.ReactNode }) {
    const { data, refetch, loading } = useGetGlobalSettingsQuery();

    return (
        <GlobalSettingsContext.Provider
            value={{
                globalSettings: data?.globalSettings || undefined,
                refetch,
                loading,
            }}
        >
            {children}
        </GlobalSettingsContext.Provider>
    );
}
