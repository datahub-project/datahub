import React from 'react';

import MetricLineChart from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricLineChart';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { SchemaFieldDataType } from '@src/types.generated';

export default function MedianValuesChart() {
    const { properties } = useStatsTabContext();
    const fieldType = properties?.expandedField?.type;

    // Only number type supported
    if (fieldType !== SchemaFieldDataType.Number) return null;

    return <MetricLineChart metric="median" title="Median Values" subTitle="Median values for this column over time" />;
}
