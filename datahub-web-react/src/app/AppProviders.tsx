import React from 'react';
import AppConfigProvider from '../AppConfigProvider';
import { EducationStepsProvider } from '../providers/EducationStepsProvider';
import UserContextProvider from './context/UserContextProvider';
import QuickFiltersProvider from '../providers/QuickFiltersProvider';
import SearchContextProvider from './search/context/SearchContextProvider';

interface Props {
    children: React.ReactNode;
}

export default function AppProviders({ children }: Props) {
    return (
        <AppConfigProvider>
            <UserContextProvider>
                <EducationStepsProvider>
                    <QuickFiltersProvider>
                        <SearchContextProvider>{children}</SearchContextProvider>
                    </QuickFiltersProvider>
                </EducationStepsProvider>
            </UserContextProvider>
        </AppConfigProvider>
    );
}
