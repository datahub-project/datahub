import React from 'react';

import MetricWithProportionLineChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricWithProportionLineChart';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';

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
