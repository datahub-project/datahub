import React from 'react';

import EntityRegistryProvider from '@app/EntityRegistryProvider';
import GlobalSettingsProvider from '@app/context/GlobalSettingsProvider';
import UserContextProvider from '@app/context/UserContextProvider';
import { DocumentsProvider } from '@app/documentV2/DocumentsContext';
import { NavBarProvider } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import HomePageProvider from '@app/homeV3/context/HomePageProvider';
import OnboardingTourProvider from '@app/onboarding/OnboardingTourContextProvider';
import SearchContextProvider from '@app/search/context/SearchContextProvider';
import { BrowserTitleProvider } from '@app/shared/BrowserTabTitleContext';
import { ReloadableProvider } from '@app/sharedV2/reloadableContext/ReloadableContext';
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
                        <DocumentsProvider>
                            <BrowserTitleProvider>
                                <EducationStepsProvider>
                                    <OnboardingTourProvider>
                                        <QuickFiltersProvider>
                                            <SearchContextProvider>
                                                <ReloadableProvider>
                                                    <HomePageProvider>
                                                        <NavBarProvider>{children}</NavBarProvider>
                                                    </HomePageProvider>
                                                </ReloadableProvider>
                                            </SearchContextProvider>
                                        </QuickFiltersProvider>
                                    </OnboardingTourProvider>
                                </EducationStepsProvider>
                            </BrowserTitleProvider>
                        </DocumentsProvider>
                    </EntityRegistryProvider>
                </UserContextProvider>
            </GlobalSettingsProvider>
        </AppConfigProvider>
    );
}
