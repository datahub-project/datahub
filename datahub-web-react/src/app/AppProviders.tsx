import React from 'react';
import AppConfigProvider from '../AppConfigProvider';
import { EducationStepsProvider } from '../providers/EducationStepsProvider';
import UserContextProvider from './context/UserContextProvider';
import QuickFiltersProvider from '../providers/QuickFiltersProvider';
import SearchContextProvider from './search/context/SearchContextProvider';
import EntityRegistryProvider from './EntityRegistryProvider';
import { BrowserTitleProvider } from './shared/BrowserTabTitleContext';
import { NavBarProvider } from './homeV2/layout/navBarRedesign/NavBarContext';

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
