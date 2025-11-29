import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

import EntityRegistryProvider from '@app/EntityRegistryProvider';
import GlobalSettingsProvider from '@app/context/GlobalSettingsProvider';
import UserContextProvider from '@app/context/UserContextProvider';
import { DocumentTreeProvider } from '@app/document/DocumentTreeContext';
import { NavBarProvider } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import HomePageProvider from '@app/homeV3/context/HomePageProvider';
import OnboardingTourProvider from '@app/onboarding/OnboardingTourContextProvider';
import SearchContextProvider from '@app/search/context/SearchContextProvider';
import { BrowserTitleProvider } from '@app/shared/BrowserTabTitleContext';
import { ReloadableProvider } from '@app/sharedV2/reloadableContext/ReloadableContext';
import { EducationStepsProvider } from '@providers/EducationStepsProvider';
import QuickFiltersProvider from '@providers/QuickFiltersProvider';
import AppConfigProvider from '@src/AppConfigProvider';

const queryClient = new QueryClient();

interface Props {
    children: React.ReactNode;
}

export default function AppProviders({ children }: Props) {
    return (
        <QueryClientProvider client={queryClient}>
            <AppConfigProvider>
                <GlobalSettingsProvider>
                    <UserContextProvider>
                        <EntityRegistryProvider>
                            <DocumentTreeProvider>
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
                            </DocumentTreeProvider>
                        </EntityRegistryProvider>
                    </UserContextProvider>
                </GlobalSettingsProvider>
            </AppConfigProvider>
        </QueryClientProvider>
    );
}
