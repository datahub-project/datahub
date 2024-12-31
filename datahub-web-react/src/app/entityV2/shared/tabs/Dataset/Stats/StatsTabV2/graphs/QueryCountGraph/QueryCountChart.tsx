import { BarChart, GraphCard } from '@components';
import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { pluralize } from '@src/app/shared/textUtil';
import { GetDatasetQuery } from '@src/graphql/dataset.generated';
import { Maybe, TimeRange, UsageAggregation } from '@src/types.generated';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import { AGGRAGATION_TIME_RANGE_OPTIONS } from '../constants';
import useQueryCountData from './useQueryCountData';
import TimeRangeSelect from '../components/TimeRangeSelect';
import useGetTimeRangeOptionsByTimeRange from '../hooks/useGetTimeRangeOptionsByTimeRange';

interface Props {
    queryCountBuckets?: Array<Maybe<UsageAggregation>>;
}

const QueryCountChart = ({ queryCountBuckets }: Props) => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const {
        setSectionState,
        dataInfo: { capabilitiesLoading, oldestDatasetUsageTime },
    } = useStatsSectionsContext();

    const timeRangeOptions = useGetTimeRangeOptionsByTimeRange(AGGRAGATION_TIME_RANGE_OPTIONS, oldestDatasetUsageTime);
    const [timeRange, setTimeRange] = useState<TimeRange>(TimeRange.Month);

    const { chartData, loading: dataLoading } = useQueryCountData(
        baseEntity?.dataset?.urn as string,
        timeRange,
        timeRange === TimeRange.Month ? queryCountBuckets || [] : undefined,
    );

    useEffect(() => {
        setSectionState('queries', chartData.length > 0);
    }, [chartData, setSectionState]);

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
                bottomAxisProps={{ tickFormat: (x) => dayjs(x).format('DD MMM') }}
                leftAxisProps={{ hideZero: true }}
                popoverRenderer={(datum) => (
                    <GraphPopover
                        header={dayjs(datum.time).format('dddd. MMM. D ’YY')}
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
            isEmpty={chartData.length === 0}
        />
    );
};

export default QueryCountChart;
