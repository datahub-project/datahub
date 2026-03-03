import React from 'react';

import { AssetsSection } from '@app/entityV2/dataProduct/AssetsSections';
import { OutputPortsSection } from '@app/entityV2/dataProduct/OutputPortsSection';
import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';

export const DataProductSummaryTab = () => {
    return (
        <SummaryTabWrapper>
            <OutputPortsSection />
            <AssetsSection />
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
};
