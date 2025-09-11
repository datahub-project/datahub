import React from 'react';

import EntityRegistryProvider from '@app/EntityRegistryProvider';
import GlobalSettingsContextProvider from '@app/context/GlobalSettings/GlobalSettingsContextProvider';
import GlobalSettingsProvider from '@app/context/GlobalSettingsProvider';
import UserContextProvider from '@app/context/UserContextProvider';
import { NavBarProvider } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import HomePageProvider from '@app/homeV3/context/HomePageProvider';
import { ModulesProvider } from '@app/homeV3/module/context/ModulesContext';
import OnboardingTourProvider from '@app/onboarding/OnboardingTourContextProvider';
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
                <GlobalSettingsProvider>
                    <UserContextProvider>
                        <EntityRegistryProvider>
                            <EducationStepsProvider>
                                <OnboardingTourProvider>
                                    <QuickFiltersProvider>
                                        <SearchContextProvider>
                                            <ModulesProvider>
                                                <HomePageProvider>
                                                    <NavBarProvider>{children}</NavBarProvider>
                                                </HomePageProvider>
                                            </ModulesProvider>
                                        </SearchContextProvider>
                                    </QuickFiltersProvider>
                                </OnboardingTourProvider>
                            </EducationStepsProvider>
                        </EntityRegistryProvider>
                    </UserContextProvider>
                </GlobalSettingsProvider>
            </AppConfigProvider>
        </GlobalSettingsContextProvider>
    );
}
