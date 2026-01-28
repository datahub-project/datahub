import React, { useCallback, useMemo, useState } from 'react';

import MetricChartPopover from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/components/MetricChartPopover';
import useDefaultLookbackWindowType from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/useDefaultLookbackWindowType';
import { useExtractMetricStats } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/useExtractMetricStats';
import useGetBottomAxisPropsByLookbackWindowType from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/useGetBottomAxisPropsByLookbackWindowType';
import { usePrepareMetricStats } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/components/sections/ChartsSection/charts/hooks/usePrepareMetricStats';
import useStatsTabContext from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/StatsV2/hooks/useStatsTabContext';
import { GraphCard, LineChart } from '@src/alchemy-components';
import { Datum } from '@src/alchemy-components/components/LineChart/types';
import MoreInfoModalContent from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MoreInfoModalContent';
import TimeRangeSelect from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/TimeRangeSelect';
import {
    GRAPH_LOOKBACK_WINDOWS_OPTIONS,
    LookbackWindowType,
} from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import useGetTimeRangeOptionsByLookbackWindow from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptionsByLookbackWindow';
import { AggregationFunction } from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { capitalizeFirstLetterOnly } from '@src/app/shared/textUtil';

export interface MetricLineChartProps {
    metric: string;
    metricLabel?: string;
    title: string;
    subTitle: string;
    renderPopoverDatumMetric?: (datum: Datum) => React.ReactNode;
    dataAggregationFunction?: AggregationFunction;
}

export default function MetricLineChart({
    metric,
    metricLabel,
    title,
    subTitle,
    renderPopoverDatumMetric,
    dataAggregationFunction,
}: MetricLineChartProps) {
    const { properties } = useStatsTabContext();
    const loading = properties?.profilesDataLoading;
    const fieldPath = properties?.expandedField?.fieldPath;
    const profiles = properties?.profiles;
    const data = useExtractMetricStats(profiles, fieldPath, metric);
    const timeRangeOptions = useGetTimeRangeOptionsByLookbackWindow(GRAPH_LOOKBACK_WINDOWS_OPTIONS, data?.[0]?.x);
    const availableLookbackWindowTyles = useMemo(
        () => timeRangeOptions.map((option) => option.value),
        [timeRangeOptions],
    );
    const defaultLoockbackWindowType = useDefaultLookbackWindowType(data, availableLookbackWindowTyles);
    const [lookbackWindow, setLookbackWindow] = useState<LookbackWindowType | null | string>(
        defaultLoockbackWindowType,
    );
    const dataToShow = usePrepareMetricStats(data, lookbackWindow, dataAggregationFunction);
    const bottomAxisProps = useGetBottomAxisPropsByLookbackWindowType(lookbackWindow);

    const renderDatumMetric = useCallback(
        (datum: Datum) => {
            if (renderPopoverDatumMetric) return renderPopoverDatumMetric(datum);

            return (
                <>
                    {metricLabel ?? `${capitalizeFirstLetterOnly(metric)} Value`}: {datum.y}
                </>
            );
        },
        [renderPopoverDatumMetric, metric, metricLabel],
    );

    return (
        <GraphCard
            title={title}
            subTitle={subTitle}
            loading={loading}
            graphHeight="180px"
            isEmpty={dataToShow.length === 0}
            renderControls={() => (
                <>
                    <TimeRangeSelect
                        options={timeRangeOptions}
                        values={lookbackWindow ? [lookbackWindow] : []}
                        onUpdate={setLookbackWindow}
                        loading={loading}
                        chartName={title} // SaaS only prop
                    />
                </>
            )}
            renderGraph={() => (
                <LineChart
                    data={dataToShow}
                    isEmpty={dataToShow.length === 0}
                    leftAxisProps={{ hideZero: true }}
                    bottomAxisProps={bottomAxisProps}
                    popoverRenderer={(datum) => (
                        <MetricChartPopover datum={datum} renderDatumMetric={renderDatumMetric} />
                    )}
                />
            )}
            moreInfoModalContent={<MoreInfoModalContent />}
        />
    );
}
