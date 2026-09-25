import React, { useEffect, useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import RowCountGraph from '@app/entityV2/shared/graphs/RowCountGraph';
import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import MoreInfoModalContent from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/MoreInfoModalContent';
import TimeRangeSelect from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/components/TimeRangeSelect';
import { getGraphLookbackWindowsOptions } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import useGetTimeRangeOptionsByLookbackWindow from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptionsByLookbackWindow';
import useProfileGraphLookback from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useProfileGraphLookback';
import { SectionKeys } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/utils';
import useRowCountData from '@app/entityV2/shared/useRowCountData';

export default function StatsTabRowCountGraph(): JSX.Element {
    const { t } = useTranslation('entity.profile.stats');
    const DEFAULT_GRAPH_NAME = t('rowCountGraph.title');
    const {
        sections,
        setSectionState,
        dataInfo: { capabilitiesLoading, oldestDatasetProfileTime },
        statsEntityUrn,
        permissions: { canViewDatasetProfile },
    } = useStatsSectionsContext();
    const graphLookbackWindowsOptions = useMemo(() => getGraphLookbackWindowsOptions(), []);
    const timeRangeOptions = useGetTimeRangeOptionsByLookbackWindow(
        graphLookbackWindowsOptions,
        oldestDatasetProfileTime,
    );
    const { lookbackWindow, rangeType, selectRangeType } = useProfileGraphLookback();

    const { data, loading: dataLoading } = useRowCountData(
        statsEntityUrn ?? undefined,
        lookbackWindow,
        canViewDatasetProfile,
    );

    const loading = capabilitiesLoading || dataLoading;

    useEffect(() => {
        const currentSection = sections.rows;
        const hasData = canViewDatasetProfile && !loading && data.length > 0;

        if (currentSection.hasData !== hasData || currentSection.isLoading !== loading) {
            setSectionState(SectionKeys.ROWS, hasData, loading);
        }
    }, [data, loading, sections.rows, setSectionState, canViewDatasetProfile]);

    return (
        <RowCountGraph
            data={data}
            loading={loading}
            canViewDatasetProfile={canViewDatasetProfile}
            emptyMessage={t('graph.emptyInSelectedRange')}
            renderControls={() => (
                <>
                    <TimeRangeSelect
                        options={timeRangeOptions}
                        values={rangeType ? [rangeType] : []}
                        onUpdate={selectRangeType}
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
