import { GraphCard, LineChart } from '@components';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import NoPermission from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/NoPermission';
import useStorageSizeData, {
    StorageSizeData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/StorageSizeGraph/useStorageSizeData';
import GraphPopover from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/GraphPopover';
import MonthOverMonthPill from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MonthOverMonthPill';
import MoreInfoModalContent from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MoreInfoModalContent';
import TimeRangeSelect from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/TimeRangeSelect';
import {
    GRAPH_LOOKBACK_WINDOWS,
    GRAPH_LOOKBACK_WINDOWS_OPTIONS,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import useGetTimeRangeOptionsByLookbackWindow from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptionsByLookbackWindow';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { LookbackWindow } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { formatBytes, formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { TimeRange } from '@src/types.generated';

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
        const formattedBytes = formatBytes(num, 2, 'B');
        return `${formatNumberWithoutAbbreviation(formattedBytes.number)} ${formattedBytes.unit}`;
    };

    const chartName = 'Storage Size';

    return (
        <GraphCard
            title={chartName}
            isEmpty={data.length === 0 || !canViewDatasetProfile}
            emptyContent={!canViewDatasetProfile && <NoPermission statName="storage size" />}
            loading={loading}
            graphHeight="290px"
            renderControls={() => (
                <>
                    <TimeRangeSelect
                        options={timeRangeOptions}
                        values={rangeType ? [rangeType] : []}
                        onUpdate={setRangeType}
                        loading={loading}
                        chartName={chartName}
                    />
                </>
            )}
            renderGraph={() => (
                <LineChart
                    data={data}
                    bottomAxisProps={{ tickFormat: (x) => dayjs(x).format('DD MMM') }}
                    leftAxisProps={{ hideZero: true, tickFormat: bytesFormatter }}
                    margin={{ left: 50 }}
                    popoverRenderer={(datum: StorageSizeData) => (
                        <GraphPopover
                            header={dayjs(datum.x).format('dddd. MMM. D ’YY')}
                            value={bytesFormatter(datum.y)}
                            pills={<MonthOverMonthPill value={datum.mom} />}
                        />
                    )}
                />
            )}
            moreInfoModalContent={<MoreInfoModalContent />}
        />
    );
}
