import React from 'react';

import EntityRegistryProvider from '@app/EntityRegistryProvider';
import GlobalSettingsProvider from '@app/context/GlobalSettingsProvider';
import UserContextProvider from '@app/context/UserContextProvider';
import { NavBarProvider } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import HomePageProvider from '@app/homeV3/context/HomePageProvider';
import OnboardingTourProvider from '@app/onboarding/OnboardingTourContextProvider';
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
            <GlobalSettingsProvider>
                <UserContextProvider>
                    <EntityRegistryProvider>
                        <BrowserTitleProvider>
                            <EducationStepsProvider>
                                <QuickFiltersProvider>
                                    <OnboardingTourProvider>
                                        <SearchContextProvider>
                                            <HomePageProvider>
                                                <NavBarProvider>{children}</NavBarProvider>
                                            </HomePageProvider>
                                        </SearchContextProvider>
                                    </OnboardingTourProvider>
                                </QuickFiltersProvider>
                            </EducationStepsProvider>
                        </BrowserTitleProvider>
                    </EntityRegistryProvider>
                </UserContextProvider>
            </GlobalSettingsProvider>
        </AppConfigProvider>
    );
}
