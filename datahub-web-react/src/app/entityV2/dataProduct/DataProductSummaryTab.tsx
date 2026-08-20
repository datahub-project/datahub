import React from 'react';

import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';

export const DataProductSummaryTab = () => {
    return (
        <SummaryTabWrapper>
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
};
