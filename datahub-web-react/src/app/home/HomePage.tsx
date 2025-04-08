import React, { useEffect } from 'react';

import analytics, { EventType } from '@app/analytics';
import { HomePageBody } from '@app/home/HomePageBody';
import { HomePageHeader } from '@app/home/HomePageHeader';
import { OnboardingTour } from '@app/onboarding/OnboardingTour';
import {
    GLOBAL_WELCOME_TO_DATAHUB_ID,
    HOME_PAGE_DOMAINS_ID,
    HOME_PAGE_INGESTION_ID,
    HOME_PAGE_MOST_POPULAR_ID,
    HOME_PAGE_PLATFORMS_ID,
    HOME_PAGE_SEARCH_BAR_ID,
} from '@app/onboarding/config/HomePageOnboardingConfig';

export const HomePage = () => {
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
                    HOME_PAGE_MOST_POPULAR_ID,
                    HOME_PAGE_SEARCH_BAR_ID,
                ]}
            />
            <HomePageHeader />
            <HomePageBody />
        </>
    );
};
