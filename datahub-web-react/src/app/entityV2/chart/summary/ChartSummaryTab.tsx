import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import ChartSummaryOverview from '@app/entityV2/chart/summary/ChartSummaryOverview';
import EmbedPreview from '@app/entityV2/chart/summary/EmbedPreview';
import FieldTableByTag from '@app/entityV2/chart/summary/FieldTableByTag';
import TableauEmbed from '@app/entityV2/chart/summary/TableauEmbed';
import { useGetTagFields } from '@app/entityV2/chart/summary/useGetTagFields';
import { SummaryTabWrapper } from '@app/entityV2/shared/summary/HeaderComponents';
import { SummaryColumns } from '@app/entityV2/shared/summary/ListComponents';
import SummaryAboutSection from '@app/entityV2/shared/summary/SummaryAboutSection';
import { LOOKER_URN, MODE_URN, TABLEAU_URN } from '@app/ingest/source/builder/constants';

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
