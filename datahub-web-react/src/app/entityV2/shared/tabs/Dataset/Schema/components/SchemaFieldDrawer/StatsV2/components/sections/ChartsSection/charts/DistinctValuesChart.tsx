import React from 'react';
import MetricWithProportionLineChart from './components/MetricWithProportionLineChart';

export default function DistinctValuesChart() {
    return (
        <MetricWithProportionLineChart
            metric="uniqueCount"
            proportionMetric="uniqueProportion"
            title="Distinct Values"
            subTitle="Number of rows with distinct values for this column"
        />
    );
}
