import React from 'react';
import { useEntityData } from '../../shared/EntityContext';
import { SummaryTabWrapper } from '../../shared/summary/HeaderComponents';
import TableauEmbed from './TableauEmbed';
import ChartSummaryOverview from './ChartSummaryOverview';
import { TABLEAU_URN } from '../../../ingest/source/builder/constants';
import SummaryAboutSection from '../../shared/summary/SummaryAboutSection';
import TableauFieldsSummary from './TableauFieldsSummary';

export default function ChartSummaryTab(): JSX.Element | null {
    const { entityData } = useEntityData();

    return (
        <SummaryTabWrapper>
            <ChartSummaryOverview />
            {entityData?.platform?.urn === TABLEAU_URN && <TableauFieldsSummary />}
            <SummaryAboutSection />
            {entityData?.platform?.urn === TABLEAU_URN && entityData?.externalUrl && (
                <TableauEmbed externalUrl={entityData.externalUrl} />
            )}
        </SummaryTabWrapper>
    );
}
