import React from 'react';

import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';

import { AssetsSection } from './AssetsSections';

export const ApplicationSummaryTab = () => {
    return (
        <SummaryTabWrapper>
            <AssetsSection />
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
};
