import React from 'react';

import TableauDataSourcesSection from '@app/entityV2/container/tableau/TableauDataSourcesSection';
import TableauViewsSection from '@app/entityV2/container/tableau/TableauViewsSection';
import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';

export default function TableauWorkbookSummaryTab() {
    return (
        <SummaryTabWrapper>
            <TableauViewsSection />
            <TableauDataSourcesSection />
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
}
