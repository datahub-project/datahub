/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import EntityRegistryProvider from '@app/EntityRegistryProvider';
import GlobalSettingsProvider from '@app/context/GlobalSettingsProvider';
import UserContextProvider from '@app/context/UserContextProvider';
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
                    </EntityRegistryProvider>
                </UserContextProvider>
            </GlobalSettingsProvider>
        </AppConfigProvider>
    );
}
