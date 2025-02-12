import React from 'react';
import { SchemaFieldDataType } from '@src/types.generated';
import MetricLineChart from './components/MetricLineChart';
import useStatsTabContext from '../../../../hooks/useStatsTabContext';

export default function MedianValuesChart() {
    const { properties } = useStatsTabContext();
    const fieldType = properties?.expandedField?.type;

    // Only number type supported
    if (fieldType !== SchemaFieldDataType.Number) return null;

    return <MetricLineChart metric="median" title="Median Values" subTitle="Median values for this column over time" />;
}
