import React from 'react';

import PersonalizationLoadingModal from '@app/homeV2/persona/PersonalizationLoadingModal';
import HomePageContent from '@app/homeV3/HomePageContent';
import Header from '@app/homeV3/header/Header';
import { PageWrapper } from '@app/homeV3/styledComponents';
import { SearchablePage } from '@app/searchV2/SearchablePage';

export const HomePage = () => {
    return (
        <>
            <SearchablePage hideSearchBar>
                <PageWrapper>
                    <Header />
                    <HomePageContent />
                </PageWrapper>
            </SearchablePage>
            <PersonalizationLoadingModal />
        </>
    );
};
