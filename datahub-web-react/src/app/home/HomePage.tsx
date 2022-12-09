import React from 'react';
import { HomePageHeader } from './HomePageHeader';
import { HomePageBody } from './HomePageBody';
import analytics, { EventType } from '../analytics';
import { OnboardingTour } from '../onboarding/OnboardingTour';
import {
    GLOBAL_WELCOME_TO_DATAHUB_ID,
    HOME_PAGE_INGESTION_ID,
    HOME_PAGE_DOMAINS_ID,
    HOME_PAGE_MOST_POPULAR_ID,
    HOME_PAGE_PLATFORMS_ID,
    HOME_PAGE_SEARCH_BAR_ID,
} from '../onboarding/config/HomePageOnboardingConfig';

export const HomePage = () => {
    analytics.event({ type: EventType.HomePageViewEvent });
    return (
        <>
            <OnboardingTour
                stepIds={[
                    GLOBAL_WELCOME_TO_DATAHUB_ID,
                    HOME_PAGE_INGESTION_ID,
                    HOME_PAGE_DOMAINS_ID,
                    HOME_PAGE_PLATFORMS_ID,
                    HOME_PAGE_MOST_POPULAR_ID,
                    HOME_PAGE_SEARCH_BAR_ID,
                ]}
            />
            <HomePageHeader />
            <HomePageBody />
        </>
    );
};
