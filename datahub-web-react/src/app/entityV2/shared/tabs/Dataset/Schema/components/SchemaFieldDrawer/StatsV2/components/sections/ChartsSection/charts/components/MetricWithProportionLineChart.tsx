import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import MetricLineChart, {
    MetricLineChartProps,
} from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricLineChart';
import { useExtractMetricStats } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/useExtractMetricStats';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { Datum } from '@src/alchemy-components/components/LineChart/types';
import { isValuePresent } from '@src/app/entityV2/shared/containers/profile/sidebar/shared/utils';
import { decimalToPercentStr } from '@src/app/entityV2/shared/tabs/Dataset/Schema/utils/statsUtil';
import { TimeInterval, groupTimeData } from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';

const PIPE_SEPARATOR = '|';

interface MetricWithProportionLineChartProps extends MetricLineChartProps {
    proportionMetric: string;
}

export default function MetricWithProportionLineChart({
    proportionMetric,
    ...props
}: MetricWithProportionLineChartProps) {
    const { t } = useTranslation('entity.profile.schema');
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
                {t('statsV2Charts.rowLabel', { count: datum.y })}{' '}
                {proportionPercent !== null && (
                    <>
                        {PIPE_SEPARATOR} {proportionPercent}
                    </>
                )}
            </>
        );
    };

    return <MetricLineChart {...props} renderPopoverDatumMetric={renderPopoverDatumMetric} />;
}
