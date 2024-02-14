import React from 'react';
import { SummaryTabWrapper } from '../shared/summary/HeaderComponents';
import SummaryAboutSection from '../shared/summary/SummaryAboutSection';
import { AssetsSection } from './AssetsSections';
import { OutputPortsSection } from './OutputPortsSection';

export const DataProductSummaryTab = () => {
    return (
        <SummaryTabWrapper>
            <OutputPortsSection />
            <AssetsSection />
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
};
