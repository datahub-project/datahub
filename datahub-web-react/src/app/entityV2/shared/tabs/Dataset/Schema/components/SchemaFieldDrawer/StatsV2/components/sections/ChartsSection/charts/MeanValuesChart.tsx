import React from 'react';
import { SchemaFieldDataType } from '@src/types.generated';
import MetricLineChart from './components/MetricLineChart';
import useStatsTabContext from '../../../../hooks/useStatsTabContext';

export default function MeanValuesChart() {
    const { properties } = useStatsTabContext();
    const fieldType = properties?.expandedField?.type;

    // Only number type supported
    if (fieldType !== SchemaFieldDataType.Number) return null;

    return <MetricLineChart metric="mean" title="Mean Values" subTitle="Mean values for this column over time" />;
}
