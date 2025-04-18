import React from 'react';
import { useGetGlobalSettingsQuery } from '../../../graphql/settings.generated';
import { GlobalSettingsContext } from './GlobalSettingsContext';
import useHelpLinkState from './useHelpLinkState';

export default function GlobalSettingsContextProvider({ children }: { children: React.ReactNode }) {
    const { data, refetch } = useGetGlobalSettingsQuery();
    const helpLinkState = useHelpLinkState(data?.globalSettings);

    return (
        <GlobalSettingsContext.Provider
            value={{
                globalSettings: data?.globalSettings || undefined,
                helpLinkState,
                refetch,
            }}
        >
            {children}
        </GlobalSettingsContext.Provider>
    );
}
