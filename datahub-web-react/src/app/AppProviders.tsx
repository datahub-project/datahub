import React from 'react';
import AppConfigProvider from '../AppConfigProvider';
import { EducationStepsProvider } from '../providers/EducationStepsProvider';
import UserContextProvider from './context/UserContextProvider';
import QuickFiltersProvider from '../providers/QuickFiltersProvider';
import SearchContextProvider from './search/context/SearchContextProvider';
import EntityRegistryProvider from './EntityRegistryProvider';

interface Props {
    children: React.ReactNode;
}

export default function AppProviders({ children }: Props) {
    return (
        <AppConfigProvider>
            <UserContextProvider>
                <EntityRegistryProvider>
                    <EducationStepsProvider>
                        <QuickFiltersProvider>
                            <SearchContextProvider>{children}</SearchContextProvider>
                        </QuickFiltersProvider>
                    </EducationStepsProvider>
                </EntityRegistryProvider>
            </UserContextProvider>
        </AppConfigProvider>
    );
}
