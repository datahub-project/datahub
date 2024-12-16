import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { GetDatasetQuery, useGetLastMonthUsageAggregationsQuery } from '@src/graphql/dataset.generated';
import { UsageQueryResult } from '@src/types.generated';
import React, { useRef } from 'react';
import styled from 'styled-components';
import HistoricalStats from './HistoricalStats';
import StatsHighlights from './StatsHighlights';
import ColumnStatsV2 from './ColumnStatsV2';

const TabContainer = styled.div`
    padding: 16px 24px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const StatsTabV2 = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();

    const { data: usageStatsData } = useGetLastMonthUsageAggregationsQuery({
        variables: { urn: baseEntity?.dataset?.urn as string },
        skip: !baseEntity?.dataset?.urn,
    });

    const columnStatsSectionRef = useRef<HTMLDivElement>(null);

    const hasUsageStats = usageStatsData?.dataset?.usageStats !== undefined;

    const usageStats = (hasUsageStats && (usageStatsData?.dataset?.usageStats as UsageQueryResult)) || undefined;

    const latestFullTableProfile = baseEntity?.dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = baseEntity?.dataset?.latestPartitionProfile?.[0];

    const totalSqlQueries = usageStats?.aggregations?.totalSqlQueries;
    const queryCountLast30Days = baseEntity.dataset?.statsSummary?.queryCountLast30Days;

    const latestProfile = latestFullTableProfile || latestPartitionProfile;

    const scrollToColumnStats = () => {
        columnStatsSectionRef.current?.scrollIntoView({ behavior: 'smooth' });
    };

    const users = usageStats?.aggregations?.users;

    return (
        <TabContainer>
            <StatsHighlights
                rowCount={latestProfile?.rowCount || undefined}
                columnCount={latestProfile?.columnCount || undefined}
                queryCount={queryCountLast30Days || totalSqlQueries || undefined}
                users={users || undefined}
                scrollToColumnStats={scrollToColumnStats}
            />
            <HistoricalStats users={users || undefined} />
            <div ref={columnStatsSectionRef}>
                <ColumnStatsV2 columnStats={(latestProfile && latestProfile.fieldProfiles) || []} />
            </div>
        </TabContainer>
    );
};

export default StatsTabV2;
