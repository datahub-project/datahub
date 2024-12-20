import { BarChart, SimpleSelect } from '@components';
import { GraphCard } from '@src/alchemy-components/components/GraphCard';
import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { pluralize } from '@src/app/shared/textUtil';
import { GetDatasetQuery } from '@src/graphql/dataset.generated';
import { Maybe, TimeRange, UsageAggregation } from '@src/types.generated';
import dayjs from 'dayjs';
import React, { useState } from 'react';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import { AGGRAGATION_TIME_RANGE_OPTIONS } from '../constants';
import useQueryCountData from './useQueryCountData';

interface Props {
    queryCountBuckets?: Array<Maybe<UsageAggregation>>;
}

const QueryCountChart = ({ queryCountBuckets }: Props) => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const [timeRange, setTimeRange] = useState<TimeRange>(TimeRange.Month);

    const { chartData, loading } = useQueryCountData(
        baseEntity?.dataset?.urn as string,
        timeRange,
        timeRange === TimeRange.Month ? queryCountBuckets || [] : undefined,
    );

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
                    <SimpleSelect
                        options={AGGRAGATION_TIME_RANGE_OPTIONS}
                        values={[timeRange]}
                        onUpdate={(values) => handleFilterChange(values[0] as TimeRange)}
                        showClear={false}
                        width="full"
                    />
                </>
            )}
            loading={loading}
            isEmpty={chartData.length === 0}
        />
    );
};

export default QueryCountChart;
