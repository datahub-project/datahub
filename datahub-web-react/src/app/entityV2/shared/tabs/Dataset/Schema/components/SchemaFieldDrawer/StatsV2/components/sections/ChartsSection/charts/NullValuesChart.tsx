import React from 'react';
import MetricWithProportionLineChart from './components/MetricWithProportionLineChart';
import useStatsTabContext from '../../../../hooks/useStatsTabContext';

export default function NullValuesChart() {
    const { properties } = useStatsTabContext();

    // Show metric for nullable fields only
    if (!properties?.expandedField?.nullable) return null;

    return (
        <MetricWithProportionLineChart
            metric="nullCount"
            proportionMetric="nullProportion"
            title="Null Values"
            subTitle="Number of rows with a null value for this column"
        />
    );
}
