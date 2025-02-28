import React, { useState } from 'react';
import styled from 'styled-components';
import { GetDatasetQuery, useGetLastMonthUsageAggregationsQuery } from '../../../../../../graphql/dataset.generated';
import { Operation, UsageQueryResult } from '../../../../../../types.generated';
import { useBaseEntity } from '../../../../../entity/shared/EntityContext';
import { toLocalDateString, toLocalDateTimeString, toLocalTimeString } from '../../../../../shared/time/timeUtils';
import StatsHeader from './StatsHeader';
import HistoricalStats from './historical/HistoricalStats';
import { LOOKBACK_WINDOWS } from './lookbackWindows';
import ColumnStats from './snapshot/ColumnStats';
import TableStats from './snapshot/TableStats';
import { ViewType } from './viewType';

const SectionWrapper = styled.div`
    overflow-y: auto;
`;

export default function StatsTab() {
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const [viewType, setViewType] = useState(ViewType.LATEST);
    const [lookbackWindow, setLookbackWindow] = useState(LOOKBACK_WINDOWS.QUARTER);

    const { data: usageStatsData } = useGetLastMonthUsageAggregationsQuery({
        variables: { urn: baseEntity?.dataset?.urn as string },
        skip: !baseEntity?.dataset?.urn,
    });

    const hasUsageStats = usageStatsData?.dataset?.usageStats !== undefined;

    const hasOperations = baseEntity?.dataset?.operations !== undefined;

    const usageStats = (hasUsageStats && (usageStatsData?.dataset?.usageStats as UsageQueryResult)) || undefined;

    const latestFullTableProfile = baseEntity?.dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = baseEntity?.dataset?.latestPartitionProfile?.[0];

    // Used for rendering latest stats.
    const urn = baseEntity && baseEntity.dataset && baseEntity.dataset?.urn;

    // Used for rendering operation info.
    const operations = (hasOperations && (baseEntity?.dataset?.operations as Array<Operation>)) || undefined;
    const latestOperation = operations && operations[0];
    const lastUpdatedTime = latestOperation && toLocalDateTimeString(latestOperation?.lastUpdatedTimestamp);
    const lastReportedTime = latestOperation && toLocalDateTimeString(latestOperation?.timestampMillis);

    // Okay so if we are disabled, we don't have both or the other. Let's render
    // Action buttons.
    // Table Stats.
    // Column Stats

    const fullTableReportedAt =
        latestFullTableProfile &&
        `Reported on ${toLocalDateString(latestFullTableProfile?.timestampMillis)} at ${toLocalTimeString(
            latestFullTableProfile?.timestampMillis,
        )}`;

    const partitionReportedAt =
        latestPartitionProfile &&
        `Reported on ${toLocalDateString(latestPartitionProfile?.timestampMillis)} at ${toLocalTimeString(
            latestPartitionProfile?.timestampMillis,
        )}`;

    const totalSqlQueries = usageStats?.aggregations?.totalSqlQueries;
    const queryCountLast30Days = baseEntity.dataset?.statsSummary?.queryCountLast30Days;

    const statsHeader = (
        <StatsHeader
            reportedAt={fullTableReportedAt || partitionReportedAt || ''}
            viewType={viewType}
            setViewType={setViewType}
            lookbackWindow={lookbackWindow}
            setLookbackWindow={setLookbackWindow}
        />
    );

    const latestProfile = latestFullTableProfile || latestPartitionProfile;

    const latestStats = (
        <SectionWrapper>
            <TableStats
                partitionSpec={latestProfile?.partitionSpec}
                rowCount={latestProfile?.rowCount || undefined}
                columnCount={latestProfile?.columnCount || undefined}
                queryCount={queryCountLast30Days || totalSqlQueries || undefined}
                users={usageStats?.aggregations?.users || undefined}
                lastUpdatedTime={lastUpdatedTime || undefined}
                lastReportedTime={lastReportedTime || undefined}
            />
            {latestProfile?.fieldProfiles && latestProfile?.fieldProfiles?.length > 0 && (
                <ColumnStats
                    partitionSpec={latestProfile?.partitionSpec}
                    columnStats={(latestProfile && latestProfile.fieldProfiles) || []}
                />
            )}
        </SectionWrapper>
    );

    const historicalStats = <HistoricalStats urn={urn || ''} lookbackWindow={lookbackWindow} />;

    return (
        <>
            {statsHeader}
            {viewType === ViewType.LATEST ? latestStats : historicalStats}
        </>
    );
}
