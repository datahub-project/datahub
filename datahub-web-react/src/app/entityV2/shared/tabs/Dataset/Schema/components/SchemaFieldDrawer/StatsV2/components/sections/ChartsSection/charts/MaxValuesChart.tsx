import React from 'react';
import { SchemaFieldDataType } from '@src/types.generated';
import MetricLineChart from './components/MetricLineChart';
import useStatsTabContext from '../../../../hooks/useStatsTabContext';

export default function MaxValuesChart() {
    const { properties } = useStatsTabContext();
    const fieldType = properties?.expandedField?.type;

    // Only number type supported
    if (fieldType !== SchemaFieldDataType.Number) return null;

    return <MetricLineChart metric="max" title="Max Values" subTitle="Max values for this column over time" />;
}
