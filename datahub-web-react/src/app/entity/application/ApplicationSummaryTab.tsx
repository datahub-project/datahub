import React from 'react';

import { AssetsSection } from '@app/entityV2/application/AssetsSections';
import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';

export const ApplicationSummaryTab = () => {
    return (
        <SummaryTabWrapper>
            <AssetsSection />
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
};
