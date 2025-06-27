import React from 'react';

import PersonalizationLoadingModal from '@app/homeV2/persona/PersonalizationLoadingModal';
import HomePageContent from '@app/homepageV2/HomePageContent';
import HomePageHeader from '@app/homepageV2/HomePageHeader';
import { PageWrapper } from '@app/homepageV2/styledComponents';
import { SearchablePage } from '@app/searchV2/SearchablePage';

export const HomePage = () => {
    return (
        <>
            <SearchablePage hideSearchBar>
                <PageWrapper>
                    <HomePageHeader />
                    <HomePageContent />
                </PageWrapper>
            </SearchablePage>
            <PersonalizationLoadingModal />
        </>
    );
};
