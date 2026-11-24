import React from 'react';

import WorkbookDashboardsSection from '@app/entityV2/container/workbook/WorkbookDashboardsSection';
import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';

export default function WorkbookSummaryTab() {
    return (
        <SummaryTabWrapper>
            <WorkbookDashboardsSection />
            <SummaryAboutSection />
        </SummaryTabWrapper>
    );
}
