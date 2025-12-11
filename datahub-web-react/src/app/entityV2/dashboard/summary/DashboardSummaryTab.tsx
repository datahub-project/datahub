/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EmbedPreview from '@app/entityV2/chart/summary/EmbedPreview';
import DashboardSummaryOverview from '@app/entityV2/dashboard/summary/DashboardSummaryOverview';
import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';

const StyledDivider = styled(Divider)`
    width: 100%;
    border-top-width: 2px;
    margin: 10px 0;
`;

export default function DashboardSummaryTab(): JSX.Element | null {
    const { entityData } = useEntityData();

    return (
        <SummaryTabWrapper>
            <DashboardSummaryOverview />
            <StyledDivider />
            <SummaryAboutSection />

            {entityData?.embed?.renderUrl && (
                <>
                    <StyledDivider />
                    <EmbedPreview embedUrl={entityData?.embed?.renderUrl} />
                </>
            )}
        </SummaryTabWrapper>
    );
}
