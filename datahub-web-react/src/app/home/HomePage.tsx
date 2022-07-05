import React from 'react';
import { HomePageHeader } from './HomePageHeader';
import { HomePageBody } from './HomePageBody';
import { CIPBanner } from '../shared/CIPShared';

export const HomePage = () => {
    return (
        <>
            <CIPBanner />
            <HomePageHeader />
            <HomePageBody />
        </>
    );
};
