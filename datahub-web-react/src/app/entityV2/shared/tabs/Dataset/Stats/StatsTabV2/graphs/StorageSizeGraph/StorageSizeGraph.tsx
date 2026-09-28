import { GraphCard, LineChart } from '@components';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

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
    getGraphLookbackWindowsOptions,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import useGetTimeRangeOptionsByLookbackWindow from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptionsByLookbackWindow';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { LookbackWindow } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { formatBytes, formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { TimeRange } from '@src/types.generated';
import dayjs from '@utils/dayjs';

// dayjs format tokens (localized date strings), not user-visible text.
const AXIS_TICK_FORMAT = 'DD MMM';
const POPOVER_HEADER_FORMAT = 'dddd. MMM. D ’YY';

export default function StorageSizeGraph() {
    const { t } = useTranslation('entity.profile.stats');
    const {
        dataInfo: { capabilitiesLoading, oldestDatasetProfileTime },
        statsEntityUrn,
        permissions: { canViewDatasetProfile },
        sections,
        setSectionState,
    } = useStatsSectionsContext();

    const graphLookbackWindowsOptions = useMemo(() => getGraphLookbackWindowsOptions(), []);
    const timeRangeOptions = useGetTimeRangeOptionsByLookbackWindow(
        graphLookbackWindowsOptions,
        oldestDatasetProfileTime,
    );
    const [lookbackWindow, setLookbackWindow] = useState<LookbackWindow>(GRAPH_LOOKBACK_WINDOWS.MONTH);
    const [rangeType, setRangeType] = useState<string | null>(TimeRange.Month);

    const { data, loading: dataLoading } = useStorageSizeData(statsEntityUrn ?? undefined, lookbackWindow);

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

    const chartName = t('storageSizeGraph.title');

    return (
        <GraphCard
            title={chartName}
            dataTestId="storage-size-card"
            isEmpty={data.length === 0 || !canViewDatasetProfile}
            emptyContent={!canViewDatasetProfile && <NoPermission statName={t('storageSizeGraph.statName')} />}
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
                    dataTestId="storage-size-chart"
                    data={data}
                    bottomAxisProps={{ tickFormat: (x) => dayjs(x).format(AXIS_TICK_FORMAT) }}
                    leftAxisProps={{ hideZero: true, tickFormat: bytesFormatter }}
                    margin={{ left: 50 }}
                    popoverRenderer={(datum: StorageSizeData) => (
                        <GraphPopover
                            header={dayjs(datum.x).format(POPOVER_HEADER_FORMAT)}
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
