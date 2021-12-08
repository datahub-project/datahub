import React from 'react';
import { HomePageHeader } from './HomePageHeader';
import { HomePageBody } from './HomePageBody';
// import { BannerSplash } from '../announce/BannerSplash';

export const HomePage = () => {
    return (
        <>
            {/* <BannerSplash /> */}
            <HomePageHeader />
            <HomePageBody />
        </>
    );
};
