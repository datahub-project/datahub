import React from 'react';

import RowCountGraph from '@app/entityV2/shared/graphs/RowCountGraph';
import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import { GRAPH_LOOKBACK_WINDOWS } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import { useQualityTabContext } from '@app/entityV2/shared/tabs/Dataset/Validations/QualityTabContextProvider';
import AssertionDataPreviewMoreInfoModalContent from '@app/entityV2/shared/tabs/Dataset/Validations/shared/AssertionDataPreviewMoreInfoModalContent';
import useRowCountData from '@app/entityV2/shared/useRowCountData';

// Hardcode the lookback window to 1 month by default
const DEFAULT_GRAPH_LOOKBACK_WINDOW = GRAPH_LOOKBACK_WINDOWS.MONTH;

export default function QualityTabRowCountGraph(): JSX.Element {
    const { qualityEntityUrn, canViewDatasetProfile: canViewDatasetProfileFromQualityTabContext } =
        useQualityTabContext();

    const {
        dataInfo: { capabilitiesLoading },
        statsEntityUrn,
        permissions: { canViewDatasetProfile: canViewDatasetProfileFromStatsContext },
    } = useStatsSectionsContext();

    // This graph can be visible in both Quality and Stats tabs, so we need to check both contexts for info
    const canViewDatasetProfile = canViewDatasetProfileFromQualityTabContext || canViewDatasetProfileFromStatsContext;
    const entityUrn = qualityEntityUrn || statsEntityUrn;

    const { data, loading: dataLoading } = useRowCountData(
        entityUrn,
        DEFAULT_GRAPH_LOOKBACK_WINDOW,
        canViewDatasetProfile,
    );

    const loading = capabilitiesLoading || dataLoading;

    return (
        <RowCountGraph
            chartHeight="150px"
            showHeader={false}
            data={data}
            loading={loading}
            canViewDatasetProfile={canViewDatasetProfile}
            renderControls={() => null} // No additional controls in the Quality tab including add assertion button
            showEmptyMessageHeader={false}
            emptyMessage="No preview of historical stats available at the moment."
            moreInfoModalContent={<AssertionDataPreviewMoreInfoModalContent />}
        />
    );
}
