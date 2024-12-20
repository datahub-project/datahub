import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import EntitySidebarContext from '@src/app/sharedV2/EntitySidebarContext';
import { GetDatasetQuery, useGetLastMonthUsageAggregationsQuery } from '@src/graphql/dataset.generated';
import { UsageQueryResult } from '@src/types.generated';
import React, { useContext, useEffect, useRef } from 'react';
import styled from 'styled-components';
import ColumnStatsV2 from './ColumnStatsV2';
import HistoricalStats from './HistoricalStats';
import StatsHighlights from './StatsHighlights';

const TabContainer = styled.div`
    padding: 16px 24px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const StatsTabV2 = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

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
    const queryCountBuckets = usageStats?.buckets;
    const columnStats = (latestProfile && latestProfile.fieldProfiles) || [];
    const hasColumnStats = columnStats?.length > 0;

    useEffect(() => {
        if (!isClosed) setSidebarClosed(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return (
        <TabContainer>
            <StatsHighlights
                rowCount={latestProfile?.rowCount || undefined}
                columnCount={latestProfile?.columnCount || undefined}
                queryCount={queryCountLast30Days || totalSqlQueries || undefined}
                users={users || undefined}
                scrollToColumnStats={scrollToColumnStats}
                hasColumnStats={hasColumnStats}
            />
            <HistoricalStats
                users={users || undefined}
                queryCountBuckets={queryCountBuckets || undefined}
                urn={baseEntity?.dataset?.urn}
            />
            {hasColumnStats && (
                <div ref={columnStatsSectionRef}>
                    <ColumnStatsV2 columnStats={columnStats} />
                </div>
            )}
        </TabContainer>
    );
};

export default StatsTabV2;
