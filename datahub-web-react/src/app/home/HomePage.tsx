import React from 'react';
import { HomePageHeader } from './HomePageHeader';
import { HomePageBody } from './HomePageBody';
import { useTrackPageView } from '../analytics';

export const HomePage = () => {
    useTrackPageView();
    return (
        <>
            <HomePageHeader />
            <HomePageBody />
        </>
    );
};
