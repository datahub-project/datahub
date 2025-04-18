import React from 'react';

import EntityRegistryProvider from '@app/EntityRegistryProvider';
import GlobalSettingsContextProvider from '@app/context/GlobalSettings/GlobalSettingsContextProvider';
import UserContextProvider from '@app/context/UserContextProvider';
import { NavBarProvider } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import SearchContextProvider from '@app/search/context/SearchContextProvider';
import { EducationStepsProvider } from '@providers/EducationStepsProvider';
import QuickFiltersProvider from '@providers/QuickFiltersProvider';
import AppConfigProvider from '@src/AppConfigProvider';

interface Props {
    children: React.ReactNode;
}

export default function AppProviders({ children }: Props) {
    return (
        <GlobalSettingsContextProvider>
            <AppConfigProvider>
                <UserContextProvider>
                    <EntityRegistryProvider>
                        <EducationStepsProvider>
                            <QuickFiltersProvider>
                                <SearchContextProvider>
                                    <NavBarProvider>{children}</NavBarProvider>
                                </SearchContextProvider>
                            </QuickFiltersProvider>
                        </EducationStepsProvider>
                    </EntityRegistryProvider>
                </UserContextProvider>
            </AppConfigProvider>
        </GlobalSettingsContextProvider>
    );
}
