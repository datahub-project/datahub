import { BarChart, GraphCard } from '@components';
import { pluralize } from '@src/app/shared/textUtil';
import { TimeRange } from '@src/types.generated';
import React, { useEffect, useState } from 'react';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import { useGetStatsData } from '../../useGetStatsData';
import { SectionKeys } from '../../utils';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import MoreInfoModalContent from '../components/MoreInfoModalContent';
import TimeRangeSelect from '../components/TimeRangeSelect';
import { AGGRAGATION_TIME_RANGE_OPTIONS } from '../constants';
import useGetTimeRangeOptionsByTimeRange from '../hooks/useGetTimeRangeOptionsByTimeRange';
import NoPermission from '../NoPermission';
import { getPopoverTimeFormat, getXAxisTickFormat } from '../utils';
import useQueryCountData from './useQueryCountData';

const QueryCountChart = () => {
    const {
        sections,
        setSectionState,
        dataInfo: { capabilitiesLoading, oldestDatasetUsageTime },
        statsEntityUrn,
        permissions: { canViewDatasetUsage },
    } = useStatsSectionsContext();

    const { usageStats } = useGetStatsData();
    const queryCountBuckets = canViewDatasetUsage ? usageStats?.buckets || undefined : [];

    const timeRangeOptions = useGetTimeRangeOptionsByTimeRange(AGGRAGATION_TIME_RANGE_OPTIONS, oldestDatasetUsageTime);
    const [timeRange, setTimeRange] = useState<TimeRange>(TimeRange.Month);

    const {
        chartData,
        loading: dataLoading,
        groupInterval,
    } = useQueryCountData(
        statsEntityUrn,
        timeRange,
        timeRange === TimeRange.Month ? queryCountBuckets || [] : undefined,
    );

    useEffect(() => {
        if (!sections.queries.hasData && chartData.length > 0) setSectionState(SectionKeys.QUERIES, true);
        else if (!!sections.queries.hasData && !chartData.length) setSectionState(SectionKeys.QUERIES, false);
    }, [chartData, setSectionState, sections.queries]);

    const handleFilterChange = (value: TimeRange) => {
        setTimeRange(value);
    };

    const loading = capabilitiesLoading || dataLoading;

    const renderBarChart = () => {
        return (
            <BarChart
                data={chartData}
                xAccessor={(item) => item.time}
                yAccessor={(item) => item.value}
                yScale={{ type: 'linear', nice: true, round: true, zero: true }}
                bottomAxisProps={{ tickFormat: (x) => getXAxisTickFormat(groupInterval, x) }}
                leftAxisProps={{ hideZero: true }}
                popoverRenderer={(datum) => (
                    <GraphPopover
                        header={getPopoverTimeFormat(groupInterval, datum.time)}
                        value={`${datum.value} ${pluralize(datum.value, 'Query')}`}
                        pills={<MonthOverMonthPill value={datum.mom} />}
                    />
                )}
            />
        );
    };

    return (
        <GraphCard
            title="Daily Query Count"
            renderGraph={renderBarChart}
            renderControls={() => (
                <>
                    <TimeRangeSelect
                        options={timeRangeOptions}
                        values={timeRange ? [timeRange] : []}
                        loading={loading}
                        onUpdate={(values) => handleFilterChange(values[0] as TimeRange)}
                    />
                </>
            )}
            loading={loading}
            isEmpty={chartData.length === 0 || !canViewDatasetUsage}
            emptyContent={!canViewDatasetUsage && <NoPermission statName="daily query count" />}
            moreInfoModalContent={<MoreInfoModalContent />}
        />
    );
};

export default QueryCountChart;
