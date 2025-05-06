import React, { useMemo } from 'react';

import MetricLineChart, {
    MetricLineChartProps,
} from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricLineChart';
import { useExtractMetricStats } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/useExtractMetricStats';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { Datum } from '@src/alchemy-components/components/LineChart/types';
import { isValuePresent } from '@src/app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { decimalToPercentStr } from '@src/app/entityV2/shared/tabs/Dataset/Schema/utils/statsUtil';
import { TimeInterval, groupTimeData } from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { pluralize } from '@src/app/shared/textUtil';

interface MetricWithProportionLineChartProps extends MetricLineChartProps {
    proportionMetric: string;
}

export default function MetricWithProportionLineChart({
    proportionMetric,
    ...props
}: MetricWithProportionLineChartProps) {
    const { properties } = useStatsTabContext();
    const fieldPath = properties?.expandedField?.fieldPath;
    const profiles = properties?.profiles;
    const metricProportionData = useExtractMetricStats(profiles, fieldPath, proportionMetric);
    const { dataAggregationFunction } = props;

    const groupedMetrricProportionData = useMemo(
        () =>
            groupTimeData(
                metricProportionData,
                TimeInterval.DAY,
                (datum) => datum.x,
                (datum) => datum.y,
                dataAggregationFunction,
            ).map((datum) => ({
                x: datum.time,
                y: datum.value,
            })),
        [metricProportionData, dataAggregationFunction],
    );

    const renderPopoverDatumMetric = (datum: Datum) => {
        const proportion = Number(
            groupedMetrricProportionData.find((metricProportionDatum) => metricProportionDatum.x === datum.x)?.y,
        );
        const proportionPercent = isValuePresent(proportion) ? decimalToPercentStr(proportion) : null;

        return (
            <>
                {datum.y} {pluralize(datum.y, 'Row')} {proportionPercent !== null && <>| {proportionPercent}</>}
            </>
        );
    };

    return <MetricLineChart {...props} renderPopoverDatumMetric={renderPopoverDatumMetric} />;
}
