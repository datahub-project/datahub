import { BarChart, GraphCard } from '@components';
import { pluralize } from '@src/app/shared/textUtil';
import { TimeRange } from '@src/types.generated';
import React, { useEffect, useState } from 'react';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import MoreInfoModalContent from '../components/MoreInfoModalContent';
import TimeRangeSelect from '../components/TimeRangeSelect';
import { AGGRAGATION_TIME_RANGE_OPTIONS } from '../constants';
import useGetTimeRangeOptionsByTimeRange from '../hooks/useGetTimeRangeOptionsByTimeRange';
import NoPermission from '../NoPermission';
import { getPopoverTimeFormat, getXAxisTickFormat } from '../utils';
import useQueryCountData from './useQueryCountData';
import { SectionKeys } from '../../utils';

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
