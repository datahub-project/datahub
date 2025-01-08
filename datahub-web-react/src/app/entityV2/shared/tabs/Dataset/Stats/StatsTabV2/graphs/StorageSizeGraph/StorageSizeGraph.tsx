import { GraphCard, LineChart } from '@components';
import { formatBytes } from '@src/app/shared/formatNumber';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import { TimeRange } from '@src/types.generated';
import { LookbackWindow } from '../../../lookbackWindows';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import { SectionKeys } from '../../utils';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import TimeRangeSelect from '../components/TimeRangeSelect';
import { GRAPH_LOOKBACK_WINDOWS, GRAPH_LOOKBACK_WINDOWS_OPTIONS } from '../constants';
import useStorageSizeData from './useStorageSizeData';
import useGetTimeRangeOptionsByLookbackWindow from '../hooks/useGetTimeRangeOptionsByLookbackWindow';

export default function StorageSizeGraph() {
    const {
        sections,
        setSectionState,
        dataInfo: { capabilitiesLoading, oldestDatasetProfileTime },
        statsEntityUrn,
    } = useStatsSectionsContext();

    const timeRangeOptions = useGetTimeRangeOptionsByLookbackWindow(
        GRAPH_LOOKBACK_WINDOWS_OPTIONS,
        oldestDatasetProfileTime,
    );
    const [lookbackWindow, setLookbackWindow] = useState<LookbackWindow>(GRAPH_LOOKBACK_WINDOWS.MONTH);
    const [rangeType, setRangeType] = useState<string | null>(TimeRange.Month);

    const { data, loading: dataLoading } = useStorageSizeData(statsEntityUrn, lookbackWindow);

    useEffect(() => {
        if (!sections.storage.hasData && data.length > 0) setSectionState(SectionKeys.STORAGE, true);
        else if (!!sections.storage.hasData && !data.length) setSectionState(SectionKeys.STORAGE, false);
    }, [data, setSectionState, sections.storage]);

    useEffect(() => {
        if (rangeType) setLookbackWindow(GRAPH_LOOKBACK_WINDOWS[rangeType]);
    }, [rangeType, setLookbackWindow]);

    const bytesFormatter = (num: number) => {
        const formattedBytes = formatBytes(num);
        return `${formattedBytes.number} ${formattedBytes.unit}`;
    };

    const loading = capabilitiesLoading || dataLoading;

    return (
        <GraphCard
            title="Storage Size"
            isEmpty={data.length === 0}
            loading={loading}
            graphHeight="290px"
            renderControls={() => (
                <>
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
                    leftAxisProps={{ hideZero: true, tickFormat: bytesFormatter }}
                    popoverRenderer={(datum) => (
                        <GraphPopover
                            header={dayjs(datum.time).format('dddd. MMM. D ’YY')}
                            value={bytesFormatter(datum.value)}
                            pills={<MonthOverMonthPill value={datum.mom} />}
                        />
                    )}
                />
            )}
        />
    );
}
