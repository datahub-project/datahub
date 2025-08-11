import React, { useEffect } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useRedirectToIntroduceYourself } from '@app/homeV2/introduce/useRedirectToIntroduceYourself';
import { CenterContent } from '@app/homeV2/layout/CenterContent';
import { LeftSidebar } from '@app/homeV2/layout/LeftSidebar';
import { RightSidebar } from '@app/homeV2/layout/RightSidebar';
import { NavBarStateType, useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import PersonalizationLoadingModal from '@app/homeV2/persona/PersonalizationLoadingModal';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import { HOME_PAGE_DOMAINS_ID, HOME_PAGE_PLATFORMS_ID } from '@app/onboarding/config/HomePageOnboardingConfig';
import {
    GLOBAL_WELCOME_TO_ACRYL_ID,
    V2_HOME_PAGE_ANNOUNCEMENTS_ID,
    V2_HOME_PAGE_DISCOVER_ID,
    V2_HOME_PAGE_MOST_POPULAR_ID,
    V2_HOME_PAGE_PENDING_TASKS_ID,
    V2_HOME_PAGE_PERSONAL_SIDEBAR_ID,
    V2_SEARCH_BAR_ID,
    V2_SEARCH_BAR_VIEWS,
} from '@app/onboarding/configV2/HomePageOnboardingConfig';
import { SearchablePage } from '@app/searchV2/SearchablePage';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    flex: 1;
    display: flex;
    overflow: hidden;
    ${(props) => props.$isShowNavBarRedesign && 'gap: 6px;'}
`;

export const HomePage = () => {
    useRedirectToIntroduceYourself();

    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { setDefaultNavBarState } = useNavBarContext();

    useEffect(() => {
        analytics.event({ type: EventType.HomePageViewEvent });
    }, []);

    useEffect(() => {
        setDefaultNavBarState(NavBarStateType.Opened);
        return () => setDefaultNavBarState(NavBarStateType.Collapsed);
    });

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
                <Container data-testid="home-page-content-container" $isShowNavBarRedesign={isShowNavBarRedesign}>
                    <LeftSidebar />
                    <CenterContent />
                    <RightSidebar />
                </Container>
            </SearchablePage>
            <PersonalizationLoadingModal />
        </>
    );
};
