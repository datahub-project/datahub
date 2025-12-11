/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import PersonalizationLoadingModal from '@app/homeV2/persona/PersonalizationLoadingModal';
import HomePageContent from '@app/homeV3/HomePageContent';
import Header from '@app/homeV3/header/Header';
import { HomePageContainer, PageWrapper, StyledVectorBackground } from '@app/homeV3/styledComponents';
import { WelcomeToDataHubModal } from '@app/onboarding/WelcomeToDataHubModal';
import { SearchablePage } from '@app/searchV2/SearchablePage';

export const HomePage = () => {
    return (
        <>
            <SearchablePage hideSearchBar>
                <HomePageContainer>
                    <StyledVectorBackground />
                    <PageWrapper>
                        <Header />
                        <HomePageContent />
                    </PageWrapper>
                </HomePageContainer>
            </SearchablePage>
            <PersonalizationLoadingModal />
            <WelcomeToDataHubModal />
        </>
    );
};
