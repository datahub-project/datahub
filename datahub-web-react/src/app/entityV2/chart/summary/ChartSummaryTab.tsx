import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { SummaryTabWrapper } from '../../shared/summary/HeaderComponents';
import TableauEmbed from './TableauEmbed';
import ChartSummaryOverview from './ChartSummaryOverview';
import { TABLEAU_URN, LOOKER_URN, MODE_URN } from '../../../ingest/source/builder/constants';
import SummaryAboutSection from '../../shared/summary/SummaryAboutSection';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { useGetTagFields } from './useGetTagFields';
import { SummaryColumns } from '../../shared/summary/ListComponents';
import FieldTableByTag from './FieldTableByTag';
import EmbedPreview from './EmbedPreview';

const StyledDivider = styled(Divider)`
    width: 100%;
    border-top-width: 2px;
    margin: 10px 0;
`;

const MEASURE_TAG = 'urn:li:tag:Measure';
const DIMENSION_TAG = 'urn:li:tag:Dimension';
const TEMPORAL_TAG = 'urn:li:tag:Temporal';

export default function ChartSummaryTab(): JSX.Element | null {
    const { entityData } = useEntityData();

    const measureFields = useGetTagFields(MEASURE_TAG);
    const dimensionFields = useGetTagFields(DIMENSION_TAG);
    const temporalFields = useGetTagFields(TEMPORAL_TAG);

    const areTagFieldsPresent = !!(measureFields?.length || dimensionFields?.length || temporalFields?.length);

    return (
        <SummaryTabWrapper>
            <ChartSummaryOverview />

            {(entityData?.platform?.urn === TABLEAU_URN ||
                entityData?.platform?.urn === LOOKER_URN ||
                entityData?.platform?.urn === MODE_URN) &&
                areTagFieldsPresent && (
                    <>
                        <StyledDivider />

                        <SummaryColumns>
                            {measureFields?.length && <FieldTableByTag title="Measures" fields={measureFields} />}
                            {dimensionFields?.length && <FieldTableByTag title="Dimensions" fields={dimensionFields} />}
                            {temporalFields?.length && <FieldTableByTag title="Temporals" fields={temporalFields} />}
                        </SummaryColumns>
                    </>
                )}
            <StyledDivider />

            <SummaryAboutSection />

            {entityData?.platform?.urn === TABLEAU_URN && entityData?.externalUrl && (
                <>
                    <StyledDivider />
                    {entityData?.platform?.urn === TABLEAU_URN && <TableauEmbed externalUrl={entityData.externalUrl} />}
                </>
            )}
            {entityData?.platform?.urn === LOOKER_URN && entityData.embed?.renderUrl && (
                <EmbedPreview embedUrl={entityData.embed?.renderUrl} />
            )}
        </SummaryTabWrapper>
    );
}
