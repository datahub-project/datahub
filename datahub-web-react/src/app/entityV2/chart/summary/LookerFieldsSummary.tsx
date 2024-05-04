import React from 'react';
import styled from 'styled-components';
import ChartFieldsTable from './ChartFieldsTable';
import { SchemaField } from '../../../../types.generated';
import { SummaryColumns } from '../../shared/summary/ListComponents';
import { SummaryTabHeaderTitle } from '../../shared/summary/HeaderComponents';
import { useEntityData } from '../../../entity/shared/EntityContext';
import { useGetTagFields } from './useGetTagFields';

const MEASURE_TAG = 'urn:li:tag:Measure';
const DIMENSION_TAG = 'urn:li:tag:Dimension';
const TEMPORAL_TAG = 'urn:li:tag:Temporal';

export default function LookerFieldsSummary() {
    const measureFields = useGetTagFields(MEASURE_TAG);
    const dimensionFields = useGetTagFields(DIMENSION_TAG);
    const temporalFields = useGetTagFields(TEMPORAL_TAG);

    return (
        <SummaryColumns>
            {measureFields?.length && <FieldTableByTag title="Measures" fields={measureFields} />}
            {dimensionFields?.length && <FieldTableByTag title="Dimensions" fields={dimensionFields} />}
            {temporalFields?.length && <FieldTableByTag title="Temporals" fields={temporalFields} />}
        </SummaryColumns>
    );
}

const ColumnWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    title: string;
    fields: SchemaField[];
}

function FieldTableByTag({ title, fields }: Props) {
    const { urn } = useEntityData();

    if (!fields?.length) {
        return null;
    }

    return (
        <>
            <ColumnWrapper>
                <SummaryTabHeaderTitle title={title} />
                <ChartFieldsTable urn={urn} rows={fields} />
            </ColumnWrapper>
        </>
    );
}
