/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
