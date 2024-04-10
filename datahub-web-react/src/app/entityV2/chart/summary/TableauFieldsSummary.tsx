import React from 'react';
import styled from 'styled-components';
import { useBaseEntity, useEntityData } from '../../../entity/shared/EntityContext';
import { GetChartQuery } from '../../../../graphql/chart.generated';
import ChartFieldsTable from './ChartFieldsTable';
import { SchemaField } from '../../../../types.generated';
import { SummaryColumns } from '../../shared/summary/ListComponents';
import { SummaryTabHeaderTitle } from '../../shared/summary/HeaderComponents';

const MEASURE_TAG = 'urn:li:tag:MEASURE';
const DIMENSION_TAG = 'urn:li:tag:DIMENSION';

export default function TableauFieldsSummary() {
    return (
        <SummaryColumns>
            <FieldTableByTag title="Measures" tag={MEASURE_TAG} />
            <FieldTableByTag title="Dimensions" tag={DIMENSION_TAG} />
        </SummaryColumns>
    );
}

const ColumnWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    title: string;
    tag: string;
}

function FieldTableByTag({ title, tag }: Props) {
    const { urn } = useEntityData();
    const chart = useBaseEntity<GetChartQuery>()?.chart;

    const fields = chart?.inputFields?.fields
        ?.filter((f) => f?.schemaField?.globalTags?.tags?.map((t) => t.tag.urn).includes(tag))
        .map((f) => f?.schemaField)
        .filter((f): f is SchemaField => !!f);

    if (!fields?.length) {
        return null;
    }

    return (
        <ColumnWrapper>
            <SummaryTabHeaderTitle title={title} />
            <ChartFieldsTable urn={urn} rows={fields} />
        </ColumnWrapper>
    );
}
