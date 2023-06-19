import React from 'react';
import AppConfigProvider from '../AppConfigProvider';
import { EducationStepsProvider } from '../providers/EducationStepsProvider';
import UserContextProvider from './context/UserContextProvider';
import QuickFiltersProvider from '../providers/QuickFiltersProvider';

interface Props {
    children: React.ReactNode;
}

export default function AppProviders({ children }: Props) {
    return (
        <AppConfigProvider>
            <UserContextProvider>
                <EducationStepsProvider>
                    <QuickFiltersProvider>{children}</QuickFiltersProvider>
                </EducationStepsProvider>
            </UserContextProvider>
        </AppConfigProvider>
    );
}
