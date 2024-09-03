import React from 'react';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { DatasetStatsSummary as DatasetStatsSummaryObj } from '../../../../../../types.generated';
import { getDatasetLastUpdatedMs } from '../../../../../entityV2/shared/utils';
import { useBaseEntity } from '../../../../shared/EntityContext';
import { DatasetStatsSummary } from '../../../shared/DatasetStatsSummary';

export const DatasetStatsSummarySubHeader = ({ properties }: { properties?: any }) => {
    const result = useBaseEntity<GetDatasetQuery>();
    const dataset = result?.dataset;

    const maybeStatsSummary = dataset?.statsSummary as DatasetStatsSummaryObj;

    const latestFullTableProfile = dataset?.latestFullTableProfile?.[0];
    const latestPartitionProfile = dataset?.latestPartitionProfile?.[0];

    const maybeLastProfile = latestFullTableProfile || latestPartitionProfile || undefined;

    const rowCount = maybeLastProfile?.rowCount;
    const columnCount = maybeLastProfile?.columnCount;
    const sizeInBytes = maybeLastProfile?.sizeInBytes;
    const totalSqlQueries = dataset?.usageStats?.aggregations?.totalSqlQueries;
    const queryCountLast30Days = maybeStatsSummary?.queryCountLast30Days;
    const queryCountPercentileLast30Days = maybeStatsSummary?.queryCountPercentileLast30Days;
    const uniqueUserCountLast30Days = maybeStatsSummary?.uniqueUserCountLast30Days;
    const uniqueUserPercentileLast30Days = maybeStatsSummary?.uniqueUserPercentileLast30Days;
    const lastUpdatedMs = getDatasetLastUpdatedMs(dataset?.properties, dataset?.operations)?.lastUpdatedMs;

    return (
        <DatasetStatsSummary
            rowCount={rowCount}
            columnCount={columnCount}
            sizeInBytes={sizeInBytes}
            totalSqlQueries={totalSqlQueries}
            queryCountLast30Days={queryCountLast30Days}
            queryCountPercentileLast30Days={queryCountPercentileLast30Days}
            uniqueUserCountLast30Days={uniqueUserCountLast30Days}
            uniqueUserPercentileLast30Days={uniqueUserPercentileLast30Days}
            lastUpdatedMs={lastUpdatedMs}
            shouldWrap={properties?.shouldWrap}
        />
    );
};
