import { useBaseEntity } from '@src/app/entity/shared/EntityContext';
import { useIsSeparateSiblingsMode } from '@src/app/entityV2/shared/useIsSeparateSiblingsMode';
import {
    GetDatasetQuery,
    useGetLastMonthUsageAggregationsQuery,
    useGetOperationsStatsQuery,
} from '@src/graphql/dataset.generated';
import { TimeRange, UsageQueryResult } from '@src/types.generated';
import { useGetEntityWithSchema } from '../../Schema/useGetEntitySchema';
import { useStatsSectionsContext } from './StatsSectionsContext';
import { getIsSiblingsMode } from './utils';

export const useGetStatsData = () => {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const { entityWithSchema } = useGetEntityWithSchema();

    const { statsEntity, statsEntityUrn } = useStatsSectionsContext();

    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    const isSiblingsMode = getIsSiblingsMode(baseEntity, isSeparateSiblingsMode);

    const { data: usageStatsData } = useGetLastMonthUsageAggregationsQuery({
        variables: { urn: statsEntityUrn as string },
        skip: !statsEntityUrn,
    });

    const { data: operationsStats } = useGetOperationsStatsQuery({
        variables: { urn: statsEntityUrn as string, range: TimeRange.Month },
        skip: !statsEntityUrn,
    });

    const latestFullTableProfile = (statsEntity as any)?.latestFullTableProfile?.[0];
    const latestPartitionProfile = (statsEntity as any)?.latestPartitionProfile?.[0];
    const latestProfile = latestFullTableProfile || latestPartitionProfile;
    const queryCountLast30Days = (statsEntity as any)?.statsSummary?.queryCountLast30Days;

    const hasUsageStats = usageStatsData?.dataset?.usageStats !== undefined;
    const usageStats = (hasUsageStats && (usageStatsData?.dataset?.usageStats as UsageQueryResult)) || undefined;
    const users = usageStats?.aggregations?.users;
    const totalSqlQueries = usageStats?.aggregations?.totalSqlQueries;

    const columnStats = (latestProfile && latestProfile.fieldProfiles) || [];
    const rowCount = latestProfile?.rowCount ?? undefined;
    const columnCount = latestProfile?.columnCount ?? entityWithSchema?.schemaMetadata?.fields?.length ?? undefined;
    const queryCount = queryCountLast30Days ?? totalSqlQueries ?? undefined;
    const totalOperations = operationsStats?.dataset?.operationsStats?.aggregations?.totalOperations ?? undefined;

    return {
        usageStats,
        columnStats,
        rowCount,
        columnCount,
        queryCount,
        totalOperations,
        users,
        isSiblingsMode,
    };
};
