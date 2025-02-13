import React from 'react';
import { Datum } from '@src/alchemy-components/components/LineChart/types';
import { isValuePresent } from '@src/app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { decimalToPercentStr } from '@src/app/entityV2/shared/tabs/Dataset/Schema/utils/statsUtil';
import { pluralize } from '@src/app/shared/textUtil';
import useStatsTabContext from '../../../../../hooks/useStatsTabContext';
import MetricLineChart, { MetricLineChartProps } from './MetricLineChart';
import { useExtractMetricStats } from '../hooks/useExtractMetricStats';

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

    const renderPopoverDatumMetric = (datum: Datum) => {
        const proportion = Number(
            metricProportionData.find((metricProportionDatum) => metricProportionDatum.x === datum.x)?.y,
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
