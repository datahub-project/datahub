import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import EntitySidebarContext from '@src/app/sharedV2/EntitySidebarContext';
import {
    GetDatasetQuery,
    useGetLastMonthUsageAggregationsQuery,
    useGetOperationsStatsQuery,
} from '@src/graphql/dataset.generated';
import { TimeRange, UsageQueryResult } from '@src/types.generated';
import React, { useContext, useEffect } from 'react';
import styled from 'styled-components';
import { useGetEntityWithSchema } from '../../Schema/useGetEntitySchema';
import ColumnStatsV2 from './ColumnStatsV2';
import ChangeHistoryGraph from './graphs/ChangeHistoryGraph/ChangeHistoryGraph';
import QueryCountChart from './graphs/QueryCountGraph/QueryCountChart';
import StorageSizeGraph from './graphs/StorageSizeGraph/StorageSizeGraph';
import HistoricalSectionHeader from './HistoricalSectionHeader';
import HistoricalStats from './HistoricalStats';
import RowsAndUsers from './RowsAndUsers';
import StatsHighlights from './StatsHighlights';
import { useStatsSectionsContext } from './StatsSectionsContext';
import { SectionKeys } from './utils';

const TabContainer = styled.div`
    padding: 16px 24px;
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const StatsTabV2 = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const { entityWithSchema } = useGetEntityWithSchema();
    const { isClosed, setSidebarClosed } = useContext(EntitySidebarContext);

    const { data: usageStatsData } = useGetLastMonthUsageAggregationsQuery({
        variables: { urn: baseEntity?.dataset?.urn as string },
        skip: !baseEntity?.dataset?.urn,
    });

    const { data: operationsStats } = useGetOperationsStatsQuery({
        variables: { urn: baseEntity?.dataset?.urn as string, range: TimeRange.Month },
        skip: !baseEntity?.dataset?.urn,
    });

    const { sections, setSectionState } = useStatsSectionsContext();

    const hasUsageStats = usageStatsData?.dataset?.usageStats !== undefined;
    const usageStats = (hasUsageStats && (usageStatsData?.dataset?.usageStats as UsageQueryResult)) || undefined;

    const latestFullTableProfile = baseEntity?.dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = baseEntity?.dataset?.latestPartitionProfile?.[0];

    const totalSqlQueries = usageStats?.aggregations?.totalSqlQueries;
    const queryCountLast30Days = baseEntity.dataset?.statsSummary?.queryCountLast30Days;
    const totalOperations = operationsStats?.dataset?.operationsStats?.aggregations?.totalOperations;

    const latestProfile = latestFullTableProfile || latestPartitionProfile;

    const scrollToSection = (sectionKey: string) => {
        sections[sectionKey]?.ref?.current?.scrollIntoView({
            behavior: 'smooth',
        });
    };

    const urn = baseEntity?.dataset?.urn;
    const users = usageStats?.aggregations?.users;
    const queryCountBuckets = usageStats?.buckets;
    const columnStats = (latestProfile && latestProfile.fieldProfiles) || [];
    const hasColumnStats = columnStats?.length > 0;

    const historicalSections: SectionKeys[] = ['rowsAndUsers', 'queries', 'storage', 'changes'];
    const hasHistoricalStats = historicalSections.some((key) => sections[key].hasData);

    useEffect(() => {
        if (!isClosed) setSidebarClosed(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        setSectionState('columnStats', hasColumnStats);
    }, [hasColumnStats, setSectionState]);

    useEffect(() => {
        if (!sections.rowsAndUsers.hasData && users && users.length > 0) setSectionState('rowsAndUsers', true);
    }, [users, setSectionState, sections.rowsAndUsers]);

    const sectionsList: Record<SectionKeys, React.ReactNode> = {
        rowsAndUsers: <RowsAndUsers hasHistoricalStats={hasHistoricalStats} urn={urn} users={users || undefined} />,
        queries: <QueryCountChart queryCountBuckets={queryCountBuckets || undefined} />,
        storage: <StorageSizeGraph urn={urn} />,
        changes: <ChangeHistoryGraph urn={urn} />,
        columnStats: <>{hasColumnStats && <ColumnStatsV2 columnStats={columnStats} />}</>,
    };
    const sortedSections = Object.entries(sections).sort(([, a], [, b]) => Number(b.hasData) - Number(a.hasData));

    return (
        <>
            <TabContainer>
                <StatsHighlights
                    rowCount={latestProfile?.rowCount ?? undefined}
                    columnCount={
                        latestProfile?.columnCount ?? entityWithSchema?.schemaMetadata?.fields?.length ?? undefined
                    }
                    queryCount={queryCountLast30Days ?? totalSqlQueries ?? undefined}
                    users={users || undefined}
                    totalOperations={totalOperations ?? undefined}
                    scrollToSection={scrollToSection}
                    hasColumnStats={hasColumnStats}
                />
                {hasHistoricalStats && <HistoricalSectionHeader />}
                <HistoricalStats sortedSections={sortedSections} sectionsList={sectionsList} />
            </TabContainer>
        </>
    );
};

export default StatsTabV2;
