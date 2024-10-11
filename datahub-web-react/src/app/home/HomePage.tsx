import React, { useEffect } from 'react';
import { Helmet } from 'react-helmet-async';
import { useTheme } from 'styled-components';
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
    const themeConfig = useTheme();

    useEffect(() => {
        analytics.event({ type: EventType.HomePageViewEvent });
    }, []);

    return (
        <>
            <Helmet>
                <title>{themeConfig.content.title}</title>
            </Helmet>
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
