import React, { useEffect } from 'react';
import styled from 'styled-components';
import analytics, { EventType } from '../analytics';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import {
    GLOBAL_WELCOME_TO_DATAHUB_ID,
    HOME_PAGE_INGESTION_ID,
    HOME_PAGE_DOMAINS_ID,
    HOME_PAGE_PLATFORMS_ID,
    HOME_PAGE_SEARCH_BAR_ID,
} from '../onboarding/config/HomePageOnboardingConfig';
import { LeftSidebar } from './layout/LeftSidebar';
import { CenterContent } from './layout/CenterContent';
import { RightSidebar } from './layout/RightSidebar';
import { useRedirectToIntroduceYourself } from './introduce/useRedirectToIntroduceYourself';
import { SearchablePage } from '../searchV2/SearchablePage';

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
                    GLOBAL_WELCOME_TO_DATAHUB_ID,
                    HOME_PAGE_INGESTION_ID,
                    HOME_PAGE_DOMAINS_ID,
                    HOME_PAGE_PLATFORMS_ID,
                    HOME_PAGE_SEARCH_BAR_ID,
                ]}
            />
            <SearchablePage>
                <Container>
                    <LeftSidebar />
                    <CenterContent />
                    <RightSidebar />
                </Container>
            </SearchablePage>
        </>
    );
};
