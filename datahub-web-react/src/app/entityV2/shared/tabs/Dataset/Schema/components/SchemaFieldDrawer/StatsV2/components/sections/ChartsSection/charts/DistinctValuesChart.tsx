import React from 'react';

import MetricWithProportionLineChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricWithProportionLineChart';

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
