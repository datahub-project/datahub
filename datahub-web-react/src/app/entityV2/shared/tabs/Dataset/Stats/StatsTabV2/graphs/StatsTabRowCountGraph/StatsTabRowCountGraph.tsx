import { AssertionType, TimeRange } from '@src/types.generated';
import React, { useEffect, useState } from 'react';
import { LookbackWindow } from '../../../lookbackWindows';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import { SectionKeys } from '../../utils';
import { GRAPH_LOOKBACK_WINDOWS, GRAPH_LOOKBACK_WINDOWS_OPTIONS } from '../constants';
import useGetTimeRangeOptionsByLookbackWindow from '../hooks/useGetTimeRangeOptionsByLookbackWindow';
import useRowCountData from '../../../../../../useRowCountData';
import RowCountGraph from '../../../../../../graphs/RowCountGraph';
import AddAssertionButton from '../components/AddAssertionButton';
import TimeRangeSelect from '../components/TimeRangeSelect';
import MoreInfoModalContent from '../components/MoreInfoModalContent';

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
                    <AddAssertionButton assertionType={AssertionType.Volume} chartName={DEFAULT_GRAPH_NAME} />
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
        />
    );
}
