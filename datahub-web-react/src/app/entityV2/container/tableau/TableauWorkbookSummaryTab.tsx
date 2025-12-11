/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
