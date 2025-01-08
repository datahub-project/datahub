import { GraphCard, LineChart } from '@components';
import { pluralize } from '@src/app/shared/textUtil';
import { AssertionType, Maybe, TimeRange, UserUsageCounts } from '@src/types.generated';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import { LookbackWindow } from '../../../lookbackWindows';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import { SectionKeys } from '../../utils';
import AddAssertionButton from '../components/AddAssertionButton';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import TimeRangeSelect from '../components/TimeRangeSelect';
import { GRAPH_LOOPBACK_WINDOWS, GRAPH_LOOPBACK_WINDOWS_OPTIONS } from '../constants';
import useRowCountData from './useRowCountData';
import useGetTimeRangeOptionsByLookbackWindow from '../hooks/useGetTimeRangeOptionsByLookbackWindow';

type RowCountGraphProps = {
    users?: Array<Maybe<UserUsageCounts>>;
};

export default function RowCountGraph({ users }: RowCountGraphProps) {
    const {
        sections,
        setSectionState,
        dataInfo: { capabilitiesLoading, oldestDatasetProfileTime },
        statsEntityUrn,
    } = useStatsSectionsContext();
    const timeRangeOptions = useGetTimeRangeOptionsByLookbackWindow(
        GRAPH_LOOPBACK_WINDOWS_OPTIONS,
        oldestDatasetProfileTime,
    );
    const [lookbackWindow, setLookbackWindow] = useState<LookbackWindow>(GRAPH_LOOPBACK_WINDOWS.MONTH);
    const [rangeType, setRangeType] = useState<string | null>(TimeRange.Month);

    const { data, loading: dataLoading } = useRowCountData(statsEntityUrn, lookbackWindow);

    useEffect(() => {
        if (!sections.rowsAndUsers.hasData && data.length > 0) setSectionState(SectionKeys.ROWS_AND_USERS, true);
        else if (!!sections.rowsAndUsers.hasData && !data?.length && !users?.length)
            setSectionState(SectionKeys.ROWS_AND_USERS, false);
    }, [data, setSectionState, sections.rowsAndUsers, users]);

    useEffect(() => {
        if (rangeType) setLookbackWindow(GRAPH_LOOPBACK_WINDOWS[rangeType]);
    }, [rangeType, setLookbackWindow]);

    const loading = capabilitiesLoading || dataLoading;

    return (
        <GraphCard
            title="Row Count"
            isEmpty={data.length === 0}
            loading={loading}
            graphHeight="290px"
            width="70%"
            renderControls={() => (
                <>
                    <AddAssertionButton assertionType={AssertionType.Volume} />

                    <TimeRangeSelect
                        options={timeRangeOptions}
                        values={rangeType ? [rangeType] : []}
                        onUpdate={(values) => setRangeType(values[0])}
                        loading={loading}
                    />
                </>
            )}
            renderGraph={() => (
                <LineChart
                    data={data}
                    xAccessor={(d) => d.time}
                    yAccessor={(d) => d.value}
                    yScale={{ type: 'linear', nice: true, round: true, zero: true }}
                    bottomAxisProps={{ tickFormat: (x) => dayjs(x).format('DD MMM') }}
                    leftAxisProps={{ hideZero: true }}
                    popoverRenderer={(datum) => (
                        <GraphPopover
                            header={dayjs(datum.time).format('dddd. MMM. D ’YY')}
                            value={`${datum.value} ${pluralize(datum.value, 'Row')}`}
                            pills={<MonthOverMonthPill value={datum.mom} />}
                        />
                    )}
                />
            )}
        />
    );
}
