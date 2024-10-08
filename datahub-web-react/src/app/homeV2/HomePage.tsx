import React, { useEffect } from 'react';
import styled from 'styled-components';
import analytics, { EventType } from '../analytics';
import {
    GLOBAL_WELCOME_TO_ACRYL_ID,
    V2_HOME_PAGE_MOST_POPULAR_ID,
    V2_SEARCH_BAR_ID,
    V2_SEARCH_BAR_VIEWS,
    V2_HOME_PAGE_DISCOVER_ID,
    V2_HOME_PAGE_PERSONAL_SIDEBAR_ID,
    V2_HOME_PAGE_PENDING_TASKS_ID,
    V2_HOME_PAGE_ANNOUNCEMENTS_ID,
} from '../onboarding/configV2/HomePageOnboardingConfig';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import { HOME_PAGE_DOMAINS_ID, HOME_PAGE_PLATFORMS_ID } from '../onboarding/config/HomePageOnboardingConfig';
import { LeftSidebar } from './layout/LeftSidebar';
import { CenterContent } from './layout/CenterContent';
import { RightSidebar } from './layout/RightSidebar';
import { useRedirectToIntroduceYourself } from './introduce/useRedirectToIntroduceYourself';
import { SearchablePage } from '../searchV2/SearchablePage';
import PersonalizationLoadingModal from './persona/PersonalizationLoadingModal';

const Container = styled.div`
    flex: 1;
    display: flex;
    overflow: hidden;
`;

export const HomePage = () => {
    useRedirectToIntroduceYourself();

    useEffect(() => {
        analytics.event({ type: EventType.HomePageViewEvent });
    }, []);

    return (
        <>
            <OnboardingTour
                stepIds={[
                    GLOBAL_WELCOME_TO_ACRYL_ID,
                    V2_HOME_PAGE_PERSONAL_SIDEBAR_ID,
                    V2_SEARCH_BAR_ID,
                    V2_SEARCH_BAR_VIEWS,
                    V2_HOME_PAGE_DISCOVER_ID,
                    V2_HOME_PAGE_ANNOUNCEMENTS_ID,
                    HOME_PAGE_DOMAINS_ID,
                    V2_HOME_PAGE_MOST_POPULAR_ID,
                    HOME_PAGE_PLATFORMS_ID,
                    V2_HOME_PAGE_PENDING_TASKS_ID,
                ]}
            />
            <SearchablePage>
                <Container data-testid="home-page-content-container">
                    <LeftSidebar />
                    <CenterContent />
                    <RightSidebar />
                </Container>
            </SearchablePage>
            <PersonalizationLoadingModal />
        </>
    );
};
