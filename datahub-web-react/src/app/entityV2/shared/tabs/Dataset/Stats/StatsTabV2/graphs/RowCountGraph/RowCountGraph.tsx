import { GraphCard, LineChart } from '@components';
import { pluralize } from '@src/app/shared/textUtil';
import { AssertionType, TimeRange } from '@src/types.generated';
import dayjs from 'dayjs';
import React, { useEffect, useState } from 'react';
import { formatNumberWithoutAbbreviation } from '@src/app/shared/formatNumber';
import { LookbackWindow } from '../../../lookbackWindows';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import { SectionKeys } from '../../utils';
import AddAssertionButton from '../components/AddAssertionButton';
import GraphPopover from '../components/GraphPopover';
import MonthOverMonthPill from '../components/MonthOverMonthPill';
import MoreInfoModalContent from '../components/MoreInfoModalContent';
import TimeRangeSelect from '../components/TimeRangeSelect';
import { GRAPH_LOOKBACK_WINDOWS, GRAPH_LOOKBACK_WINDOWS_OPTIONS } from '../constants';
import useGetTimeRangeOptionsByLookbackWindow from '../hooks/useGetTimeRangeOptionsByLookbackWindow';
import NoPermission from '../NoPermission';
import useRowCountData, { RowCountData } from './useRowCountData';

export default function RowCountGraph() {
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

    const { data, loading: dataLoading } = useRowCountData(statsEntityUrn, lookbackWindow);

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

    const chartName = 'Row Count';

    return (
        <GraphCard
            title={chartName}
            isEmpty={data.length === 0 || !canViewDatasetProfile}
            emptyContent={!canViewDatasetProfile && <NoPermission statName="row count" />}
            loading={loading}
            graphHeight="290px"
            renderControls={() => (
                <>
                    <AddAssertionButton assertionType={AssertionType.Volume} chartName={chartName} />

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
                    leftAxisProps={{ hideZero: true }}
                    margin={{ left: 50 }}
                    popoverRenderer={(datum: RowCountData) => (
                        <GraphPopover
                            header={dayjs(datum.x).format('dddd. MMM. D ’YY')}
                            value={`${formatNumberWithoutAbbreviation(datum.y)} ${pluralize(datum.y, 'Row')}`}
                            pills={<MonthOverMonthPill value={datum.mom} />}
                        />
                    )}
                />
            )}
            moreInfoModalContent={<MoreInfoModalContent />}
        />
    );
}
