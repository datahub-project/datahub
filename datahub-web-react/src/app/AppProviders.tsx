import React from 'react';
import AppConfigProvider from '../AppConfigProvider';
import { EducationStepsProvider } from '../providers/EducationStepsProvider';
import UserContextProvider from './context/UserContextProvider';
import QuickFiltersProvider from '../providers/QuickFiltersProvider';
import SearchContextProvider from './search/context/SearchContextProvider';
import { BrowserTitleProvider } from './shared/BrowserTabTitleContext';

interface Props {
    children: React.ReactNode;
}

export default function AppProviders({ children }: Props) {
    return (
        <AppConfigProvider>
            <UserContextProvider>
                <BrowserTitleProvider>
                    <EducationStepsProvider>
                        <QuickFiltersProvider>
                            <SearchContextProvider>{children}</SearchContextProvider>
                        </QuickFiltersProvider>
                    </EducationStepsProvider>
                </BrowserTitleProvider>
            </UserContextProvider>
        </AppConfigProvider>
    );
}
