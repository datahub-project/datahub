import React from 'react';
import SummaryAboutSection from '../../shared/summary/SummaryAboutSection';
import { SummaryTabWrapper } from '../../shared/summary/HeaderComponents';
import TableauViewsSection from './TableauViewsSection';
import TableauDataSourcesSection from './TableauDataSourcesSection';

export default function TableauWorkbookSummaryTab() {
    return (
        <SummaryTabWrapper>
            <TableauViewsSection />
            <TableauDataSourcesSection />
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
}
