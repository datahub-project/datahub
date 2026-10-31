import React from 'react';

import { useBaseEntity } from '@app/entityV2/shared/EntityContext';
import { DatasetStatsSummary } from '@app/sharedV2/datasetStats/DatasetStatsSummary';

import { GetDatasetQuery } from '@graphql/dataset.generated';
import { DatasetProperties, DatasetStatsSummary as DatasetStatsSummaryObj, Operation } from '@types';

function getLastUpdatedMs(
    properties: Pick<DatasetProperties, 'lastModified'> | null | undefined,
    operations: Pick<Operation, 'lastUpdatedTimestamp'>[] | null | undefined,
): number | undefined {
    return (
        Math.max(
            properties?.lastModified?.time || 0,
            (operations?.length && operations[0].lastUpdatedTimestamp) || 0,
        ) || undefined
    );
}

export default function DatasetStats() {
    const result = useBaseEntity<GetDatasetQuery>();
    const dataset = result?.dataset;

    const maybeStatsSummary = dataset?.statsSummary as DatasetStatsSummaryObj;
    const maybeLastProfile = dataset?.latestFullTableProfile?.[0] || dataset?.latestPartitionProfile?.[0] || undefined;

    return (
        <DatasetStatsSummary
            rowCount={maybeLastProfile?.rowCount}
            columnCount={maybeLastProfile?.columnCount}
            sizeInBytes={maybeLastProfile?.sizeInBytes}
            totalSqlQueries={dataset?.usageStats?.aggregations?.totalSqlQueries}
            queryCountLast30Days={maybeStatsSummary?.queryCountLast30Days}
            uniqueUserCountLast30Days={maybeStatsSummary?.uniqueUserCountLast30Days}
            lastUpdatedMs={getLastUpdatedMs(dataset?.properties, dataset?.operations)}
            shouldWrap
        />
    );
}
