import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { GetDatasetQuery, useGetLastMonthUsageAggregationsQuery } from '@src/graphql/dataset.generated';
import { UsageQueryResult } from '@src/types.generated';
import React from 'react';
import StatsHighlights from '../snapshot/StatsHighlights';

const StatsTabV2 = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const { data: usageStatsData } = useGetLastMonthUsageAggregationsQuery({
        variables: { urn: baseEntity?.dataset?.urn as string },
        skip: !baseEntity?.dataset?.urn,
    });

    const hasUsageStats = usageStatsData?.dataset?.usageStats !== undefined;

    const usageStats = (hasUsageStats && (usageStatsData?.dataset?.usageStats as UsageQueryResult)) || undefined;

    const latestFullTableProfile = baseEntity?.dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = baseEntity?.dataset?.latestPartitionProfile?.[0];

    const totalSqlQueries = usageStats?.aggregations?.totalSqlQueries;
    const queryCountLast30Days = baseEntity.dataset?.statsSummary?.queryCountLast30Days;

    const latestProfile = latestFullTableProfile || latestPartitionProfile;

    return (
        <StatsHighlights
            rowCount={latestProfile?.rowCount || undefined}
            columnCount={latestProfile?.columnCount || undefined}
            queryCount={queryCountLast30Days || totalSqlQueries || undefined}
            users={usageStats?.aggregations?.users || undefined}
        />
    );
};

export default StatsTabV2;
