import React from 'react';
import { HomePageHeader } from './HomePageHeader';
import { HomePageBody } from './HomePageBody';
import analytics, { EventType } from '../analytics';
import { CIPBanner } from '../shared/CIPShared';

export const HomePage = () => {
    analytics.event({ type: EventType.HomePageViewEvent });
    return (
        <>
            <CIPBanner />
            <HomePageHeader />
            <HomePageBody />
        </>
    );
};
