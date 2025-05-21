import React from 'react';

import { ContentsSection } from '@app/entityV2/domain/summary/ContentsSection';
import { DataProductsSection } from '@app/entityV2/domain/summary/DataProductsSection';
import OwnersSection from '@app/entityV2/domain/summary/OwnersSection';
import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';

export const DomainSummaryTab = () => {
    return (
        <SummaryTabWrapper>
            <OwnersSection />
            <DataProductsSection />
            <ContentsSection />
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
};
