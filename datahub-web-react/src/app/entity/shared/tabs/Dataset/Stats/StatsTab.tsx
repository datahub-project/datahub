import React, { useState } from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { DatasetProfile, UsageQueryResult } from '../../../../../../types.generated';
import { useBaseEntity } from '../../../EntityContext';
import HistoricalStats from './historical/HistoricalStats';
import { LOOKBACK_WINDOWS } from './lookbackWindows';
import ColumnStats from './snapshot/ColumnStats';
import TableStats from './snapshot/TableStats';
import StatsHeader from './StatsHeader';
import { ViewType } from './viewType';

const toLocalDateString = (time: number) => {
    const date = new Date(time);
    return date.toLocaleDateString();
};

const toLocalTimeString = (time: number) => {
    const date = new Date(time);
    return date.toLocaleTimeString();
};

export default function StatsTab() {
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const [viewType, setViewType] = useState(ViewType.LATEST);
    const [lookbackWindow, setLookbackWindow] = useState(LOOKBACK_WINDOWS.WEEK);

    const hasUsageStats = baseEntity?.dataset?.usageStats !== undefined;
    const hasDatasetProfiles = baseEntity?.dataset?.datasetProfiles !== undefined;

    const usageStats = (hasUsageStats && (baseEntity?.dataset?.usageStats as UsageQueryResult)) || undefined;
    const datasetProfiles =
        (hasDatasetProfiles && (baseEntity?.dataset?.datasetProfiles as Array<DatasetProfile>)) || undefined;

    // Used for rendering latest stats.
    const latestProfile = datasetProfiles && datasetProfiles[0]; // This is required for showing latest stats.
    const urn = baseEntity && baseEntity.dataset && baseEntity.dataset?.urn;

    // Okay so if we are disabled, we don't have both or the other. Let's render

    // const emptyView = <Empty description="TODO: Stats!" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
    // Action buttons.
    // Table Stats.
    // Column Stats

    const reportedAt =
        latestProfile &&
        `Reported on ${toLocalDateString(latestProfile?.timestampMillis)} at ${toLocalTimeString(
            latestProfile?.timestampMillis,
        )}`;

    const statsHeader = (
        <StatsHeader
            viewType={viewType}
            setViewType={setViewType}
            reportedAt={reportedAt || ''}
            lookbackWindow={lookbackWindow}
            setLookbackWindow={setLookbackWindow}
        />
    );

    const latestStats = (
        <>
            <TableStats
                rowCount={latestProfile?.rowCount || undefined}
                columnCount={latestProfile?.columnCount || undefined}
                queryCount={usageStats?.aggregations?.totalSqlQueries || undefined}
                users={usageStats?.aggregations?.users || undefined}
            />
            <ColumnStats columnStats={(latestProfile && latestProfile.fieldProfiles) || []} />
        </>
    );

    const historicalStats = <HistoricalStats urn={urn || ''} lookbackWindow={lookbackWindow} />;

    return (
        <>
            {statsHeader}
            {viewType === ViewType.LATEST ? latestStats : historicalStats}
        </>
    );
}
