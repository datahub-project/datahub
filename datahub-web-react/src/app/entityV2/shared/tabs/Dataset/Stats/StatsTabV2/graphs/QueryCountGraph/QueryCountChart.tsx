import { BarChart, GraphCard } from '@components';
import React, { useEffect, useState } from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import NoPermission from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/NoPermission';
import useQueryCountData, {
    ChartData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/QueryCountGraph/useQueryCountData';
import GraphPopover from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/GraphPopover';
import MonthOverMonthPill from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MonthOverMonthPill';
import MoreInfoModalContent from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MoreInfoModalContent';
import TimeRangeSelect from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/TimeRangeSelect';
import { AGGRAGATION_TIME_RANGE_OPTIONS } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import useGetTimeRangeOptionsByTimeRange from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptionsByTimeRange';
import {
    getPopoverTimeFormat,
    getXAxisTickFormat,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { pluralize } from '@src/app/shared/textUtil';
import { TimeRange } from '@src/types.generated';

const QueryCountChart = () => {
    const {
        dataInfo: { capabilitiesLoading, oldestDatasetUsageTime },
        statsEntityUrn,
        permissions: { canViewDatasetUsage },
        sections,
        setSectionState,
    } = useStatsSectionsContext();

    const timeRangeOptions = useGetTimeRangeOptionsByTimeRange(AGGRAGATION_TIME_RANGE_OPTIONS, oldestDatasetUsageTime);
    const [timeRange, setTimeRange] = useState<TimeRange>(TimeRange.Month);

    const { chartData, loading: dataLoading, groupInterval } = useQueryCountData(statsEntityUrn, timeRange);

    const loading = capabilitiesLoading || dataLoading;

    useEffect(() => {
        const currentSection = sections.queries;
        const hasData = canViewDatasetUsage && !loading && chartData.length > 0;

        if (currentSection.hasData !== hasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.QUERIES, hasData, loading);
        }
    }, [chartData, loading, sections.queries, setSectionState, canViewDatasetUsage]);

    const handleFilterChange = (value: TimeRange) => {
        setTimeRange(value);
    };

    const renderBarChart = () => {
        return (
            <BarChart
                data={chartData}
                dataTestId="query-count-chart"
                bottomAxisProps={{ tickFormat: (x) => getXAxisTickFormat(groupInterval, x) }}
                leftAxisProps={{ hideZero: true }}
                margin={{ left: 50 }}
                popoverRenderer={(datum: ChartData) => (
                    <GraphPopover
                        header={getPopoverTimeFormat(groupInterval, datum.x)}
                        value={`${formatNumberWithoutAbbreviation(datum.y)} ${pluralize(datum.y, 'Query')}`}
                        pills={<MonthOverMonthPill value={datum.mom} />}
                    />
                )}
            />
        );
    };

    const chartName = 'Daily Query Count';

    return (
        <GraphCard
            title={chartName}
            dataTestId="query-count-card"
            renderGraph={renderBarChart}
            renderControls={() => (
                <>
                    <TimeRangeSelect
                        options={timeRangeOptions}
                        values={timeRange ? [timeRange] : []}
                        loading={loading}
                        onUpdate={(value) => handleFilterChange(value as TimeRange)}
                        chartName={chartName}
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
