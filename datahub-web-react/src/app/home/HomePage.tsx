import React from 'react';
import { HomePageHeader } from './HomePageHeader';
import { HomePageBody } from './HomePageBody';
import { BannerSplash } from '../announce/BannerSplash';
import analytics, { EventType } from '../analytics';

export const HomePage = () => {
    analytics.event({ type: EventType.HomePageViewEvent });
    return (
        <>
            <BannerSplash />
            <HomePageHeader />
            <HomePageBody />
        </>
    );
};
