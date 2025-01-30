import { GraphCard, LineChart } from '@components';
import { formatBytes, formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { TimeRange } from '@src/types.generated';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import { LookbackWindow } from '../../../lookbackWindows';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import MoreInfoModalContent from '../components/MoreInfoModalContent';
import TimeRangeSelect from '../components/TimeRangeSelect';
import { GRAPH_LOOKBACK_WINDOWS, GRAPH_LOOKBACK_WINDOWS_OPTIONS } from '../constants';
import useGetTimeRangeOptionsByLookbackWindow from '../hooks/useGetTimeRangeOptionsByLookbackWindow';
import NoPermission from '../NoPermission';
import useStorageSizeData from './useStorageSizeData';
import { SectionKeys } from '../../utils';

export default function StorageSizeGraph() {
    const {
        dataInfo: { capabilitiesLoading, oldestDatasetProfileTime },
        statsEntityUrn,
        permissions: { canViewDatasetProfile },
        sections,
        setSectionState,
    } = useStatsSectionsContext();

    const timeRangeOptions = useGetTimeRangeOptionsByLookbackWindow(
        GRAPH_LOOKBACK_WINDOWS_OPTIONS,
        oldestDatasetProfileTime,
    );
    const [lookbackWindow, setLookbackWindow] = useState<LookbackWindow>(GRAPH_LOOKBACK_WINDOWS.MONTH);
    const [rangeType, setRangeType] = useState<string | null>(TimeRange.Month);

    const { data, loading: dataLoading } = useStorageSizeData(statsEntityUrn, lookbackWindow);

    const loading = capabilitiesLoading || dataLoading;

    useEffect(() => {
        const currentSection = sections.storage;
        const hasData = canViewDatasetProfile && !loading && data.length > 0;
        if (currentSection.hasData !== hasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.STORAGE, hasData, loading);
        }
    }, [data, loading, sections.storage, setSectionState, canViewDatasetProfile]);

    useEffect(() => {
        if (rangeType) setLookbackWindow(GRAPH_LOOKBACK_WINDOWS[rangeType]);
    }, [rangeType, setLookbackWindow]);

    const bytesFormatter = (num: number) => {
        const formattedBytes = formatBytes(num);
        return `${formatNumberWithoutAbbreviation(formattedBytes.number)} ${formattedBytes.unit}`;
    };

    return (
        <GraphCard
            title="Storage Size"
            isEmpty={data.length === 0 || !canViewDatasetProfile}
            emptyContent={!canViewDatasetProfile && <NoPermission statName="storage size" />}
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
            moreInfoModalContent={<MoreInfoModalContent />}
        />
    );
}
