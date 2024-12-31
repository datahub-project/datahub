import { LineChart, GraphCard } from '@components';
import { pluralize } from '@src/app/shared/textUtil';
import { AssertionType, TimeRange } from '@src/types.generated';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import { LookbackWindow } from '../../../lookbackWindows';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import AddAssertionButton from '../components/AddAssertionButton';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import { GRAPH_LOOPBACK_WINDOWS, GRAPH_LOOPBACK_WINDOWS_OPTIONS } from '../constants';
import useRowCountData from './useRowCountData';
import TimeRangeSelect from '../components/TimeRangeSelect';
import useGetTimeRangeOptionsByLookbackWindow from '../hooks/useGetTimeRangeOptionsByLookbackWindow';

type RowCountGraphProps = {
    urn?: string;
};

export default function RowCountGraph({ urn }: RowCountGraphProps) {
    const {
        sections,
        setSectionState,
        dataInfo: { capabilitiesLoading, oldestDatasetProfileTime },
    } = useStatsSectionsContext();
    const timeRangeOptions = useGetTimeRangeOptionsByLookbackWindow(
        GRAPH_LOOPBACK_WINDOWS_OPTIONS,
        oldestDatasetProfileTime,
    );
    const [lookbackWindow, setLookbackWindow] = useState<LookbackWindow>(GRAPH_LOOPBACK_WINDOWS.MONTH);
    const [rangeType, setRangeType] = useState<string | null>(TimeRange.Month);

    const { data, loading: dataLoading } = useRowCountData(urn, lookbackWindow);

    useEffect(() => {
        if (!sections.rowsAndUsers.hasData && data.length > 0) setSectionState('rowsAndUsers', true);
    }, [data, setSectionState, sections.rowsAndUsers]);

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
