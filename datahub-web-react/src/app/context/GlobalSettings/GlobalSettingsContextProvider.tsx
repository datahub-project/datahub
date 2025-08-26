import React from 'react';

import { GlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';
import useHelpLinkState from '@app/context/GlobalSettings/useHelpLinkState';

import { useGetGlobalSettingsQuery } from '@graphql/settings.generated';

export default function GlobalSettingsContextProvider({ children }: { children: React.ReactNode }) {
    const { data, refetch, loading } = useGetGlobalSettingsQuery();
    const helpLinkState = useHelpLinkState(data?.globalSettings);

    return (
        <GlobalSettingsContext.Provider
            value={{
                globalSettings: data?.globalSettings || undefined,
                helpLinkState,
                refetch,
                loading,
            }}
        >
            {children}
        </GlobalSettingsContext.Provider>
    );
}
