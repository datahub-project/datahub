import React from 'react';
import { ContentsSection } from './ContentsSection';
import { DataProductsSection } from './DataProductsSection';
import SummaryAboutSection from '../../shared/summary/SummaryAboutSection';
import { SummaryTabWrapper } from '../../shared/summary/HeaderComponents';
import OwnersSection from './OwnersSection';

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
