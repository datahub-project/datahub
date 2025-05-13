import React, { useEffect, useState } from 'react';

import RowCountGraph from '@app/entityV2/shared/graphs/RowCountGraph';
import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import MoreInfoModalContent from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MoreInfoModalContent';
import TimeRangeSelect from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/TimeRangeSelect';
import {
    GRAPH_LOOKBACK_WINDOWS,
    GRAPH_LOOKBACK_WINDOWS_OPTIONS,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import useGetTimeRangeOptionsByLookbackWindow from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptionsByLookbackWindow';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import { LookbackWindow } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import useRowCountData from '@app/entityV2/shared/useRowCountData';
import { TimeRange } from '@src/types.generated';

const DEFAULT_GRAPH_NAME = 'Row Count';

export default function StatsTabRowCountGraph(): JSX.Element {
    const {
        sections,
        setSectionState,
        dataInfo: { capabilitiesLoading, oldestDatasetProfileTime },
        statsEntityUrn,
        permissions: { canViewDatasetProfile },
    } = useStatsSectionsContext();
    const timeRangeOptions = useGetTimeRangeOptionsByLookbackWindow(
        GRAPH_LOOKBACK_WINDOWS_OPTIONS,
        oldestDatasetProfileTime,
    );
    const [lookbackWindow, setLookbackWindow] = useState<LookbackWindow>(GRAPH_LOOKBACK_WINDOWS.MONTH);
    const [rangeType, setRangeType] = useState<string | null>(TimeRange.Month);

    const { data, loading: dataLoading } = useRowCountData(statsEntityUrn, lookbackWindow, canViewDatasetProfile);

    const loading = capabilitiesLoading || dataLoading;

    useEffect(() => {
        const currentSection = sections.rows;
        const hasData = canViewDatasetProfile && !loading && data.length > 0;

        if (currentSection.hasData !== hasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.ROWS, hasData, loading);
        }
    }, [data, loading, sections.rows, setSectionState, canViewDatasetProfile]);

    useEffect(() => {
        if (rangeType) setLookbackWindow(GRAPH_LOOKBACK_WINDOWS[rangeType]);
    }, [rangeType, setLookbackWindow]);

    return (
        <RowCountGraph
            data={data}
            loading={loading}
            canViewDatasetProfile={canViewDatasetProfile}
            renderControls={() => (
                <>
                    <TimeRangeSelect
                        options={timeRangeOptions}
                        values={rangeType ? [rangeType] : []}
                        onUpdate={setRangeType}
                        loading={loading}
                        chartName={DEFAULT_GRAPH_NAME}
                    />
                </>
            )}
            moreInfoModalContent={<MoreInfoModalContent />}
            dataTestId="row-count"
        />
    );
}
