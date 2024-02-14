import React from 'react';
import { ContentsSection } from './ContentsSection';
import { DataProductsRow } from './DataProductsRow';
import SummaryAboutSection from '../../shared/summary/SummaryAboutSection';
import { SummaryTabWrapper } from '../../shared/summary/HeaderComponents';

export const DomainSummaryTab = () => {
    return (
        <SummaryTabWrapper>
            <DataProductsRow />
            <ContentsSection />
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
};
