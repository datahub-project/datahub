import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { SummaryTabWrapper } from '../../shared/summary/HeaderComponents';
import TableauEmbed from './TableauEmbed';
import ChartSummaryOverview from './ChartSummaryOverview';
import { TABLEAU_URN, LOOKER_URN } from '../../../ingest/source/builder/constants';
import SummaryAboutSection from '../../shared/summary/SummaryAboutSection';
import TableauFieldsSummary from './TableauFieldsSummary';
import LookerFieldsSummary from './LookerFieldsSummary';
import LookerEmbed from './LookerEmbed';
import { useEntityData } from '../../../entity/shared/EntityContext';

const StyledDivider = styled(Divider)`
    width: calc(100% + 40px);
    margin: 10px -20px;
    border-top-width: 8px;
`;

export default function ChartSummaryTab(): JSX.Element | null {
    const { entityData } = useEntityData();

    return (
        <SummaryTabWrapper>
            <ChartSummaryOverview />
            <StyledDivider />

            {entityData?.platform?.urn === TABLEAU_URN && <TableauFieldsSummary />}
            {entityData?.platform?.urn === LOOKER_URN && <LookerFieldsSummary />}
            <StyledDivider />

            <SummaryAboutSection />
            <StyledDivider />

            {entityData?.platform?.urn === TABLEAU_URN && entityData?.externalUrl && (
                <TableauEmbed externalUrl={entityData.externalUrl} />
            )}
            {entityData?.platform?.urn === LOOKER_URN && entityData?.externalUrl && (
                <LookerEmbed externalUrl={entityData.externalUrl} />
            )}
        </SummaryTabWrapper>
    );
}
