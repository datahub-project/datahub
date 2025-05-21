import React from 'react';

import EntityRegistryProvider from '@app/EntityRegistryProvider';
import UserContextProvider from '@app/context/UserContextProvider';
import { NavBarProvider } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import SearchContextProvider from '@app/search/context/SearchContextProvider';
import { BrowserTitleProvider } from '@app/shared/BrowserTabTitleContext';
import { EducationStepsProvider } from '@providers/EducationStepsProvider';
import QuickFiltersProvider from '@providers/QuickFiltersProvider';
import AppConfigProvider from '@src/AppConfigProvider';

interface Props {
    children: React.ReactNode;
}

export default function AppProviders({ children }: Props) {
    return (
        <AppConfigProvider>
            <UserContextProvider>
                <EntityRegistryProvider>
                    <BrowserTitleProvider>
                        <EducationStepsProvider>
                            <QuickFiltersProvider>
                                <SearchContextProvider>
                                    <NavBarProvider>{children}</NavBarProvider>
                                </SearchContextProvider>
                            </QuickFiltersProvider>
                        </EducationStepsProvider>
                    </BrowserTitleProvider>
                </EntityRegistryProvider>
            </UserContextProvider>
        </AppConfigProvider>
    );
}
