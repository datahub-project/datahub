import { Icon } from '@components';
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
import PageBanner from '@app/sharedV2/PageBanner';

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
            <PageBanner
                localStorageKey="v1UIDeprecationAnnouncement"
                icon={<Icon icon="ExclamationMark" color="red" weight="fill" source="phosphor" />}
                content="V1 UI is officially deprecated and will no longer be available beginning in the next release (v1.3)"
            />
            <HomePageHeader />
            <HomePageBody />
        </>
    );
};
