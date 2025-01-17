import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { SummaryTabWrapper } from '../../shared/summary/HeaderComponents';
import SummaryAboutSection from '../../shared/summary/SummaryAboutSection';
import EmbedPreview from '../../chart/summary/EmbedPreview';
import DashboardSummaryOverview from './DashboardSummaryOverview';

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
